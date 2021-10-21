import boto3
import os
from util.aws_util import AWSUtility
from util.config_util import ConfigUtility
from util.dynamodb_util import DynamoDBUtility

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
    dynamodb_obj = DynamoDBUtility(DYNAMO_TABLE, REGION, AWS_SERVER_ACCESS_KEY, AWS_SERVER_SECRET_KEY)
    table_info = dynamodb_obj.get_table_count()
    print(table_info)

    key_schema=[
        {
            'AttributeName': 'PartitionKey',
            'KeyType': 'HASH'
        }
    ]
    attribute_definitions=[
        {
            'AttributeName': 'PartitionKey',
            'AttributeType': 'S'
        }
    ]
    provisioned_throughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
    new_table_name = "DataTable"
    dynamodb_obj.create_table(new_table_name, key_schema, attribute_definitions, provisioned_throughput)


