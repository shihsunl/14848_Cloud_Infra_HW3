from flask import Flask, json, session, request, redirect, url_for, send_from_directory
from datetime import datetime
import time
import random
import csv
import os
import magic # python-magic
from werkzeug.utils import secure_filename # Werkzeug

from util.s3_util import S3Utility
from util.dynamodb_util import DynamoDBUtility
from util.aws_util import AWSUtility
from util.config_util import ConfigUtility

AWS_SERVER_ACCESS_KEY = ''
AWS_SERVER_SECRET_KEY = ''
BUCKET_NAME = ''
DYNAMO_TABLE = ''
REGION = ''
LOCAL_FILE_FOLDER = './upload'
REMOTE_FILE_FOLDER = 'files'
LOCAL_DOWNLOAD_FOLDER = './download'

ALLOWED_EXTENSIONS = {'csv'}
ALLOWED_MIME_TYPES = {'text/csv', 'text/plain'}

folder_list = ['download', 'upload', 'config']
for folder_name in folder_list:
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

def load_config():
    global AWS_SERVER_ACCESS_KEY
    global AWS_SERVER_SECRET_KEY
    global BUCKET_NAME
    global DYNAMO_TABLE
    global REGION

    config_path = './config/config.ini'
    config_obj = ConfigUtility(config_path)
    config = config_obj.get_config()
    AWS_SERVER_ACCESS_KEY = config['AWS']['AWS_SERVER_ACCESS_KEY']
    AWS_SERVER_SECRET_KEY = config['AWS']['AWS_SERVER_SECRET_KEY']
    BUCKET_NAME = config['AWS']['BUCKET_NAME']
    DYNAMO_TABLE = config['AWS']['DYNAMO_TABLE']
    REGION = config['AWS']['REGION']

def current_milli_time():
    return round(time.time() * 1000)

api = Flask(__name__)
api.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

def is_allowed_file(file):
    if '.' in file.filename:
        ext = file.filename.rsplit('.', 1)[1].lower()
    else:
        return False

    mime_type = magic.from_buffer(file.stream.read(), mime=True)
    if (mime_type in ALLOWED_MIME_TYPES and ext in ALLOWED_EXTENSIONS):
        # move the cursor to the beginning
        file.stream.seek(0,0)
        return True

    return False

@api.route('/', methods=['GET'])
def index():
    return (
        '<!doctype html>'
        '<title>Upload File to S3</title>'
        '<h1>Upload File to S3</h1>'
        '<form method="post" enctype="multipart/form-data" action="/s3">'
        '<input type="file" name="file">'
        '<input type="submit" value="Upload">'
        '</form>'
    )

@api.route('/success', methods=['GET'])
def success():
    return (
        '<!doctype html>'
        '<title>Success</title>'
        '<h1>Success</h1>'
    )

@api.route('/table', methods=['GET'])
def get_table_content():
    # Return the row data from the table

    # Parameters:
    #    key_name (str): the key schema name for searching the data in table
    #    key_val (str): the key value for searching the data in table

    # Curl example: curl "http://127.0.0.1:5000/table?keyname=PartitionKey&keyval=1"

    key_name = request.args.get('keyname')
    key_val = request.args.get('keyval')
    dynamodb_obj = DynamoDBUtility(DYNAMO_TABLE, REGION, AWS_SERVER_ACCESS_KEY, AWS_SERVER_SECRET_KEY)
    table_info = dynamodb_obj.get_table_info_by_key(key_name, key_val)
    return json.dumps(table_info)

@api.route('/tablecount', methods=['GET'])
def get_table_count():
    # Return the number of data in the table

    try:
        dynamodb_obj = DynamoDBUtility(DYNAMO_TABLE, REGION, AWS_SERVER_ACCESS_KEY, AWS_SERVER_SECRET_KEY)
        table_info = dynamodb_obj.get_table_count()
        return json.dumps(table_info)
    except Exception as err:
        print(err)
        return ''

@api.route('/table', methods=['POST'])
def post_table_content():
    # Update DynamoDB table and return detail
    # The uploaded csv file must contains Id and URL
    # Curl example: curl -X POST  -F 'file=@path/to/experiments.csv' "http://127.0.0.1:5000/table"
    # Schema:
    # | Id | Temp | Conductivity | Concentration |    URL   |
    # | 1  |  -1  |      52      |      3.4      | exp1.csv |

    try:
        output_arr = []
        file = request.files['file']
        local_file = ''
        # save uploaded data to local
        if file and is_allowed_file(file):
            print("post_s3_content ", file)
            filename = secure_filename(file.filename)
            local_file = os.path.join(LOCAL_FILE_FOLDER, filename)
            file.save(local_file)
        else:
            return {'result': 'Fail to upload file'}

        with open(local_file, mode='r', encoding='utf-8-sig') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')

            # store the headers in a separate variable,
            # move the reader object to point on the next row
            header = next(csvreader)
            if 'Id' in header and 'URL' in header:

                # get the index of Id and URL
                id_index = header.index('Id')
                url_index = header.index('URL')

                # fetch data
                for row in csvreader:
                    remote_file_path = '{}/{}'.format(REMOTE_FILE_FOLDER, row[url_index])
                    now_time = datetime.now()
                    dt_string = now_time.strftime("%d/%m/%Y %H:%M:%S")
                    meta_data = {
                        'PartitionKey': row[id_index], # ID
                        'description': 'test_data',
                        'date': dt_string,
                        'url': remote_file_path
                    }

                    dynamodb_obj = DynamoDBUtility(DYNAMO_TABLE, REGION, AWS_SERVER_ACCESS_KEY, AWS_SERVER_SECRET_KEY)
                    table_info = dynamodb_obj.put_item(meta_data)
                    output_arr.append(table_info)
            else:
                return {'result': 'Can not find URL or Id'}
        return json.dumps(output_arr)

    except Exception as err:
        print(err)
        return ''

@api.route('/s3', methods=['GET'])
def get_s3_content():
    # Return the s3 file structure

    try:
        s3_obj = S3Utility(BUCKET_NAME, AWS_SERVER_ACCESS_KEY, AWS_SERVER_SECRET_KEY)
        file_list = s3_obj.get_file_list()
        return json.dumps(file_list)
    except Exception as err:
        print(err)
        return ''

@api.route('/downloads3/<path:remotefile>', methods=['GET'])
def download_s3_file(remotefile):
    # Download the s3 file binary

    try:
        random_hash = random.getrandbits(128)
        tmp_file = "{}_{}".format(current_milli_time(), random_hash)
        download_tmp_file = "{}/{}".format(LOCAL_DOWNLOAD_FOLDER, tmp_file)
        s3_obj = S3Utility(BUCKET_NAME, AWS_SERVER_ACCESS_KEY, AWS_SERVER_SECRET_KEY)
        success = s3_obj.download_file(remotefile, download_tmp_file)
        if success == True:
            data = send_from_directory(directory=LOCAL_DOWNLOAD_FOLDER, path=tmp_file, filename=tmp_file)
            os.unlink(download_tmp_file)
            return data
        else:
            return '{"result": Fail}'
    except Exception as err:
        print(err)
        return '{"result": Fail}'

@api.route('/s3', methods=['POST'])
def post_s3_content():
    # Upload file to s3 and return the remote file path
    # Curl example: curl -X POST  -F 'file=@/path/to/file.csv' "http://127.0.0.1:5000/s3"

    try:
        file = request.files['file']
        # save uploaded data to local
        if file and is_allowed_file(file):
            print("post_s3_content ", file)
            filename = secure_filename(file.filename)
            file.save(os.path.join(LOCAL_FILE_FOLDER, filename))

            # push data to s3
            local_file_path = '{}/{}'.format(LOCAL_FILE_FOLDER, filename)
            remote_file_path = '{}/{}'.format(REMOTE_FILE_FOLDER, filename)
            s3_obj = S3Utility(BUCKET_NAME, AWS_SERVER_ACCESS_KEY, AWS_SERVER_SECRET_KEY)
            success = s3_obj.upload_file(local_file_path, remote_file_path)
            if success == True:
                return json.dumps({'s3_file_name': remote_file_path})
            else:
                return ''
    except Exception as err:
        print(err)
        return ''

if __name__ == '__main__':
    load_config()
    api.run(host='0.0.0.0') 