import boto3
import os
from util.aws_util import AWSUtility
from util.config_util import ConfigUtility
from util.s3_util import S3Utility

AWS_SERVER_ACCESS_KEY = ''
AWS_SERVER_SECRET_KEY = ''
BUCKET_NAME = ''
DYNAMO_TABLE = ''
BUCKET_NAME = ''
REGION = ''
LOCAL_FILE_FOLDER = '.'
REMOTE_FILE_FOLDER = 'files'

def load_config():
    global AWS_SERVER_ACCESS_KEY
    global AWS_SERVER_SECRET_KEY
    global BUCKET_NAME
    global DYNAMO_TABLE
    global BUCKET_NAME
    global REGION
    config_path           = './config/config.ini'
    config_obj            = ConfigUtility(config_path)
    config                = config_obj.get_config()
    AWS_SERVER_ACCESS_KEY = config['AWS']['AWS_SERVER_ACCESS_KEY']
    AWS_SERVER_SECRET_KEY = config['AWS']['AWS_SERVER_SECRET_KEY']
    BUCKET_NAME           = config['AWS']['BUCKET_NAME']
    DYNAMO_TABLE          = config['AWS']['DYNAMO_TABLE']
    REGION                = config['AWS']['REGION']


if __name__ == '__main__':
    load_config()
    s3_obj = S3Utility(BUCKET_NAME, AWS_SERVER_ACCESS_KEY, AWS_SERVER_SECRET_KEY)
    new_bucket_name = "s3-14848-shihsunl"
    s3_obj.create_bucket(new_bucket_name)
    s3_obj.create_folder('files')
    s3_obj.create_folder('log')
    s3_obj.create_folder('test')

