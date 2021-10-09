from util.aws_util import AWSUtility

class DynamoDBUtility():
    def __init__(self, dynamodb_table, region, aws_access_key, aws_secret_key):
        self.aws_access_key  = aws_access_key
        self.aws_secret_key  = aws_secret_key
        self.dynamodb_table  = dynamodb_table
        self.region          = region
        self.aws_util_obj    = AWSUtility('', self.dynamodb_table, self.region, self.aws_access_key, self.aws_secret_key)
        self.session         = self.aws_util_obj.init_aws_session()
        self.dynamodb, self.dytable = self.aws_util_obj.init_aws_dynamodb_resource(self.session)

    def error_msg_handler(self, err, output):
        print(err)
        return output

    def get_table_count(self):
        try:
            # Wait until the table exists.
            self.dytable.meta.client.get_waiter('table_exists').wait(TableName=self.dynamodb_table)
            # Print out some data about the table.
            return self.dytable.item_count
        except Exception as err:
            return self.error_msg_handler(err, False)

    def get_table_info_by_key(self, key_name, key_val):
        try:
            response = self.dytable.get_item(Key={key_name: key_val})
            return response['Item']
        except Exception as err:
            return self.error_msg_handler(err, False)

    def put_item(self, meta_data):
        try:
            response = self.dytable.put_item(Item=meta_data)
            return response
        except Exception as err:
            return self.error_msg_handler(err, False)

