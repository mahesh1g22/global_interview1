# Assignment
As part of a technology transformation project the data engineering team at Global has been
asked to build a data pipeline to process raw log files received from our ad serving platform.
The data engineering team will be responsible for cleansing and transforming the data prior
to it being surfaced to the commercial team in the application front end.
The data is received in CSV format into an s3 bucket continuously throughout the day and
arrives in date partitions with the following format: YYYY/MM/DD. Currently data is
processed in daily batches and output to another s3 location to be picked up by a downstream
application.
Your task is to write a pipeline in python which can process a specified day&#39;s data and meets
the following business and technical requirements:
- Remove duplicates within the same day partition based on the key columns
IMPRESSION_ID, IMPRESSION_DATETIME
- Aggregate the de-duplicated data across all files within a specific date partition by
CAMPAIGN_ID, and the hour the impression occurred.
- Write the aggregated data for a single day in CSV format to paths mirroring the
source bucket e.g. s3://rd-interview-sample-data/results/YYYY/MM/DD. Please
include your initials in the file name. e.g. daily_agg_20210130_{Initials}.csv
- The process should be idempotent.
- Code reusability is important when writing production pipelines, consider how you
would make your code modular and reusable.
- The script should be parameterised and executable from the command line with the
user being able to specify which day they wish to process.

# How to Run the application
This assignment uses two dependecies i.e., `Pandas` and `boto3`. The dependency versions are mentioned in `requirements.txt`. Please use below command to install the dependencies.
```shell
pip install -r requirements.txt 
```
After the successfully installed the dependecies set the below environment variables

```shell
export AWS_ACCESS_KEY=<AWS access key>
export AWS_SECRET = <AWS secret key>
```

Use below command to run the project
```shell
python main.py <date>
```
where `date` is the input file date in the format of `YYYY-MM-DD`