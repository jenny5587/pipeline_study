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

#뭉텅이로 스트림을 읽기
df = (spark.readStream
      .format("kinesis")
      .option("streamName", 'dbc-handson')
      .option('endpointUrl', 'https://kinesis.ap-northeast-2.amazonaws.com')
      .option("region", 'ap-northeast-2')
      .option("awsSTSRoleARN", "arn:aws:iam::{id}:role/dbc-handson-kinesis-crossaccount")
      .option("awsSTSSessionName", "handson")
      .option("startingPosition", "TRIM_HORIZON")
      .load())
      
#갈래쳐서 s3에 각각 저장하기
sr_df = df.filter(F.col('partitionkey') == '매장환불기록')
ss_df = df.filter(F.col('partitionkey') == '매장판매기록')
cs_df = df.filter(F.col('partitionkey') == '카탈로그판매기록')
cr_df = df.filter(F.col('partitionkey') == '카탈로그환불기록')


#foreachBatchFunction 으로 각각 s3에 업데이트하기
def foreachBatchFunction(microBatchOutputDF, batchId):
    microBatchOutputDF.show()
    if not microBatchOutputDF.rdd.isEmpty():
        microBatchOutputDF.createOrReplaceTempView("updates")
        microBatchOutputDF._jdf.sparkSession().sql("""
        insert into split_etl.sr_df
        select data, streamName, partitionKey, sequenceNumber, approximateArrivalTimestamp from updates
        """)
      
def foreachBatchFunction2(microBatchOutputDF, batchId):
    microBatchOutputDF.show()
    if not microBatchOutputDF.rdd.isEmpty():
        microBatchOutputDF.createOrReplaceTempView("updates")
        microBatchOutputDF._jdf.sparkSession().sql("""
        insert into split_etl.ss_df
        select data, streamName, partitionKey, sequenceNumber, approximateArrivalTimestamp from updates
        """)
      
def foreachBatchFunction3(microBatchOutputDF, batchId):
    microBatchOutputDF.show()
    if not microBatchOutputDF.rdd.isEmpty():
        microBatchOutputDF.createOrReplaceTempView("updates")
        microBatchOutputDF._jdf.sparkSession().sql("""
        insert into split_etl.cr_df
        select data, streamName, partitionKey, sequenceNumber, approximateArrivalTimestamp from updates
        """)
      
def foreachBatchFunction4(microBatchOutputDF, batchId):
    microBatchOutputDF.show()
    if not microBatchOutputDF.rdd.isEmpty():
        microBatchOutputDF.createOrReplaceTempView("updates")
        microBatchOutputDF._jdf.sparkSession().sql("""
        insert into split_etl.cs_df
        select data, streamName, partitionKey, sequenceNumber, approximateArrivalTimestamp from updates
        """)

sr_df0 = (sr_df.writeStream
              .format("csv")
              .option("checkpointLocation", "s3://{/checkpoint/sr_df}")
              .foreachBatch(foreachBatchFunction)
              .trigger(processingTime='5 second')
              .outputMode("update")
              .start())
              
ss_df0 = (ss_df.writeStream
              .format("csv")
              .option("checkpointLocation", "s3://{/checkpoint/sc_df}")
              .foreachBatch(foreachBatchFunction2)
              .trigger(processingTime='5 second')
              .outputMode("update") 
              .start())

cr_df0 = (cr_df.writeStream
              .format("csv")
              .option("checkpointLocation", "s3://{/checkpoint/cr_df}")
              .foreachBatch(foreachBatchFunction3)
              .trigger(processingTime='5 second')
              .outputMode("update")
              .start())
              
cs_df0 = (cs_df.writeStream
              .format("csv")
              .option("checkpointLocation", "s3://{/checkpoint/cs_df}")
              .foreachBatch(foreachBatchFunction4)
              .trigger(processingTime='5 second')
              .outputMode("update")
              .start())
              
ss_df0.awaitTermination()
sr_df0.awaitTermination()
cr_df0.awaitTermination()
cs_df0.awaitTermination()

#deta로 하니까 안들어오던게, parquet으로 하니까 데이터가 잘 들어오는것 확인 ...

job.commit()
