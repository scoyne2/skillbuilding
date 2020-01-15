import os
import schemas
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# spark-submit --jars jars/spark-sql-kinesis_2.11-2.4.0.jar python/twitter_stream_by_second.py 

if __name__ == "__main__":

  spark = SparkSession \
          .builder \
          .appName("twitter-streaming") \
          .getOrCreate()

  # schema
  # [streamName, approximateArrivalTimestamp, sequenceNumber, partitionKey, data]
  stream = spark.readStream.format("kinesis") \
                .option("streamName", "twitter-stream") \
                .option("endpointUrl", "https://kinesis.us-west-2.amazonaws.com") \
                .option("startingposition", "TRIM_HORIZON") \
                .option("awsAccessKeyId", AWS_ACCESS_KEY_ID) \
                .option("awsSecretKey", AWS_SECRET_ACCESS_KEY) \
                .load()


  stream.withColumn('json', from_json(stream.data.cast("string"), schemas.kinesis_schema) ) \
        .withColumn('data', stream.data.cast("string"))\
        .select("json.quote_count", 
                "json.entities",
                "json.entities.hashtags",
                "json.entities.urls",
                "json.entities.user_mentions",
                "json.entities.media",
                "json.entities.symbols",
                "json.entities.polls" ) \
        .writeStream.format("console") \
        .option("truncate", "false") \
        .start() \
        .awaitTermination()