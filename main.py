import os
import sys
from io import StringIO
from datetime import datetime
from aws_storage import AWSStorage
import pipeline

def printusageandquit(message: str):
    print(message)
    quit()
def validateinput():
    if len(sys.argv) < 2:
        printusageandquit('Please pass the day as argument in YYYY-MM-DD format to process the data')

validateinput()
bucket='rd-interview-sample-data'
try:
    AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET = os.environ['AWS_SECRET']
except:
    printusageandquit('Please set the envirtonment variables AWS_ACCESS_KEY and AWS_SECRET values')

try:
    inputdate = sys.argv[1]
    date = datetime.strptime(inputdate,'%Y-%m-%d')
except:
    printusageandquit(f'Invalid date format {inputdate}. Expected format is YYYY-MM-DD')


storage_client = AWSStorage(AWS_ACCESS_KEY, AWS_SECRET)
formateddate = date.strftime('%Y/%m/%d')
data = storage_client.read_data(bucket=bucket, file_path=f"{formateddate}/{inputdate}.csv")

status = data.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status == 200:
    result = pipeline.transform(data.get("Body"))
    csv_buffer = StringIO()
    result.to_csv(csv_buffer, index=False)
    storage_client.write_data(bucket=bucket,file_path=f"results/daily_agg_{date.strftime('%Y%m%d')}_Ven.csv", body=csv_buffer.getvalue())
else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")
