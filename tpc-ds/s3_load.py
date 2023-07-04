import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## -----------------------------

df = (spark.readStream
      .format("kinesis")
      .option("streamName", 'dbc-handson')
      .option('endpointUrl', 'https://kinesis.ap-northeast-2.amazonaws.com')
      .option("region", 'ap-northeast-2')
      .option("awsSTSRoleARN", "arn:aws:iam::{id}:role/dbc-handson-kinesis-crossaccount")
      .option("awsSTSSessionName", "handson")
      .option("startingPosition", "TRIM_HORIZON")
      .load())
      
def foreachBatchFunction(microBatchOutputDF, batchId):
  microBatchOutputDF.show()
  if not microBatchOutputDF.rdd.isEmpty():
      microBatchOutputDF.createOrReplaceTempView("updates")
      microBatchOutputDF._jdf.sparkSession().sql("""
        insert into jenny.lake
        select data, streamName, partitionKey, sequenceNumber, approximateArrivalTimestamp from updates
      """)
## lake table의 s3경로가 지정되어있으므로 거기에 저장이 됨.

(df.writeStream
              .format("parquet")
              .option("checkpointLocation", "{checkpoint-s3uri}")
              .foreachBatch(foreachBatchFunction)
              .trigger(processingTime='5 second')
              .outputMode("update")
              .start().awaitTermination())
job.commit()

##전부 다 load한 이후 category 별로 나눠서 저장하는 script 필요
