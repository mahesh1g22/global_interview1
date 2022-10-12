import os
from aws_storage import AWSStorage
import pipeline

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET = os.environ['AWS_SECRET']
bucket = 'rd-interview-sample-data'

storage_client = AWSStorage(AWS_ACCESS_KEY, AWS_SECRET)
data = storage_client.read_data(bucket=bucket, file_path="2021/01/30/2021-01-30.csv")

status = data.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    result = pipeline.transform(data.get("Body"))
    print(result)
    # storage_client.write_data(bucket,'output', result.getvalue())
else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")
