import boto3


class AWSStorage:

    def __init__(self, access_key, secret):
        self.access_key = access_key
        self.secret = secret
        self.s3_client = boto3.client(
            service_name='s3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret
        )

    def read_data(self, bucket, file_path):
        return self.s3_client.get_object(Bucket=bucket, Key=file_path)

    def write_data(self, bucket, file_path, body):
        return self.s3_client.put_object(Bucket=bucket,  Key=file_path, Body=body)
