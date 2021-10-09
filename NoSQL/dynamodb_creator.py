import boto3
import os
from util.aws_util import AWSUtility
from util.config_util import ConfigUtility

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

load_config()

session = boto3.Session(
    aws_access_key_id=AWS_SERVER_ACCESS_KEY,
    aws_secret_access_key=AWS_SERVER_SECRET_KEY,
)

dynamodb = session.resource('dynamodb', region_name='us-west-1')
table = dynamodb.create_table(
    TableName='DataTable',
    KeySchema=[
        {
            'AttributeName': 'PartitionKey',
            'KeyType': 'HASH'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'PartitionKey',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Wait until the table exists.
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

# Print out some data about the table.
print(table.item_count)

