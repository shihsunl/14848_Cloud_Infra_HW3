from util.aws_util import AWSUtility

class S3Utility():
    def __init__(self, bucket_name, aws_access_key, aws_secret_key):
        self.aws_access_key  = aws_access_key
        self.aws_secret_key  = aws_secret_key
        self.bucket_name     = bucket_name
        self.aws_util_obj    = AWSUtility(self.bucket_name, '', '', self.aws_access_key, self.aws_secret_key)
        self.session         = self.aws_util_obj.init_aws_session()
        self.s3, self.bucket = self.aws_util_obj.init_aws_s3_resource(self.session)

    def create_bucket(self, bucket_name):
        try:
            bucket = self.s3.create_bucket(Bucket=bucket_name, ACL='public-read', CreateBucketConfiguration={'LocationConstraint': 'us-west-1'})
            self.bucket_name     = bucket_name
            self.aws_util_obj    = AWSUtility(self.bucket_name, '', '', self.aws_access_key, self.aws_secret_key)
            self.session         = self.aws_util_obj.init_aws_session()
            self.s3, self.bucket = self.aws_util_obj.init_aws_s3_resource(self.session)
            print(bucket)
            return True
        except Exception as err:
            return self.error_msg_handler(err, False)

    def create_folder(self, folder_name):
        try:
            res = self.bucket.put_object(Bucket=self.bucket_name, Key=(folder_name+'/'), ACL='public-read')
            print(res)
            return True
        except Exception as err:
            return self.error_msg_handler(err, False)

    def change_bucket(self, bucket_name):
        try:
            self.bucket_name     = bucket_name
            self.aws_util_obj    = AWSUtility(self.bucket_name, '', '', self.aws_access_key, self.aws_secret_key)
            self.session         = self.aws_util_obj.init_aws_session()
            self.s3, self.bucket = self.aws_util_obj.init_aws_s3_resource(self.session)
            return True
        except Exception as err:
            return self.error_msg_handler(err, False)

    def error_msg_handler(self, err, output):
        print(err)
        return output

    def get_file_list(self):
        try:
            output_arr = []
            for file in self.bucket.objects.all():
                output_arr.append(file.key)
            return output_arr
        except Exception as err:
            return self.error_msg_handler(err, [])

    def upload_file(self, local_file_path, remote_file_path):
        try:
            res = self.s3.meta.client.upload_file(local_file_path, self.bucket_name, remote_file_path)
            return True
        except Exception as err:
            return self.error_msg_handler(err, False)

    def check_file_exist(self, filepath):
        try:
            res = self.s3.Object(self.bucket_name, filepath).load()
            return True
        except Exception as err:
            # file not found
            return self.error_msg_handler(err, False)

    def download_file(self, filepath, output_filename):
        try:
            self.bucket.download_file(filepath, output_filename)
            return True
        except Exception as err:
            # fail to download file
            return self.error_msg_handler(err, False)
