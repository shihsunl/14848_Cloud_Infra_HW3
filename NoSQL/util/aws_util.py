import boto3

class AWSUtility():
    def __init__(self, bucket_name, dynamodb_table, region, aws_access_key, aws_secret_key):
        self.bucket_name = bucket_name
        self.dynamodb_table = dynamodb_table
        self.region = region
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key

    def init_aws_session(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key ,
        )
        return session

    def init_aws_s3_resource(self, session):
        s3 = session.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        return (s3, bucket)

    def init_aws_dynamodb_resource(self, session):
        dynamodb = session.resource('dynamodb', region_name=self.region)
        table = dynamodb.Table(self.dynamodb_table)
        return (dynamodb, table)

