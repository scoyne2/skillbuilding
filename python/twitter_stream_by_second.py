import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']


schema = StructType([
    StructField("quote_count", StringType()),
    StructField("contributors", StringType()),
    StructField("truncated", StringType()),
    StructField("contributors", StringType()),
    StructField("text", StringType()),
    StructField("is_quote_status", StringType()),
    StructField("in_reply_to_status_id", StringType()),
    StructField("reply_count", StringType()),
    StructField("id", StringType()),

])

# spark-submit --jars jars/spark-sql-kinesis_2.11-2.4.0.jar python/twitter_stream_by_second.py 

if __name__ == "__main__":

  spark = SparkSession \
          .builder \
          .appName("twitter-streaming") \
          .getOrCreate()

  # schema
  # [streamName, approximateArrivalTimestamp, sequenceNumber, partitionKey, data]
  kinesis = spark.readStream.format("kinesis") \
                 .option("streamName", "twitter-stream") \
                 .option("endpointUrl", "https://kinesis.us-west-2.amazonaws.com") \
                 .option("startingposition", "TRIM_HORIZON") \
                 .option("awsAccessKeyId", AWS_ACCESS_KEY_ID) \
                 .option("awsSecretKey", AWS_SECRET_ACCESS_KEY) \
                 .load()


  data = kinesis.selectExpr("cast (data as STRING) jsonData")

  parsed = data.select(from_json("jsonData", schema).alias("message"))

  parsed.writeStream.format("console").option("truncate", "false").start().awaitTermination()

