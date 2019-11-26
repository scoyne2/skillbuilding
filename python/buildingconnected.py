from pyspark.sql import SparkSession, SQLContext, HiveContext
import logging
import psycopg2
import boto3
import os

#run like
#spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 --conf "spark.sql.warehouse.dir=/spark-warehouse/" building_connected_etl.py 

#Redshift connection
DB_NAME = 'dev'
HOST = 'redshift-cluster-1.cvxvz5dxbpdn.us-west-1.redshift.amazonaws.com'
PORT = '5439'
USER = 'awsuser'
PASSWORD = 'TIger42!!'
REDSHIFTURL = "jdbc:redshift://"+HOST+":5439/"+DB_NAME+"?user="+USER+"&password="+PASSWORD

#S3 connection
FOLDER_PATH = 's3a://interviewtestbld/scoyne/data/'
TEMPS3DIR = "s3a://interviewtestbld/temp"
RAW_DATA_PATH =  FOLDER_PATH + "rawdata/"
ARCHIVE_PATH =  FOLDER_PATH + "archive/"
DATE_DIM_PATH =  FOLDER_PATH + "dimdata/dimdates/"

#Spark warehouse connection
WAREHOUSE_LOCATION = "/usr/local/airflow/spark-warehouse/"

#Tables
DATABASE = "buildingconnected"
RAW_TABLE = DATABASE + "." +"events_raw"
DIRTY_TABLE = DATABASE + "." +"events_dirty"
CLEAN_TABLE = DATABASE + "." + "events_clean"
PROJECTS_EVENTS_TABLE = DATABASE + "." + "events_projects"
BID_PACKAGES_EVENTS_TABLE = DATABASE + "." + "events_bid_packages"
BIDDER_GROUPS_EVENTS_TABLE = DATABASE + "." + "events_bidder_groups"
PROJECT_SNAPSHOT_TABLE = DATABASE + "." + "snapshot_projects"
BID_PACKAGES_SNAPSHOT_TABLE = DATABASE + "." + "snapshot_bid_packages"
BIDDER_GROUPS_SNAPSHOT_TABLE = DATABASE + "." + "snapshot_bidder_groups"
DIM_DATES_TABLE = DATABASE + "." + "dim_date"


def execute_redshift_query(sql_query):
    #create raw table

    con = psycopg2.connect(dbname=DB_NAME,host=HOST,port=PORT,user=USER,password=PASSWORD) 
    cur = con.cursor()     
    cur.execute(sql_query)    
    con.commit()
    cur.close()
    con.close()


def init_spark(appName):
    """Return SparkSession, SparkContext and HiveContext

    Initialize spark session for data processing 
    """
    logging.warn('********************************* starting spark session *********************************')
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    return spark, sc, sqlContext


def archive_files(source, destination):
    """Return None

    After raw files have been processed, move to archive folder.
    """
    s3 = boto3.resource('s3')

    for key in s3.list_objects(Bucket=source)['Contents']:
        files = key['Key']
        copy_source = {'Bucket': "bucket_to_copy",'Key': files}
        s3.meta.client.copy(copy_source, destination, files)
        print(files)


def write_table( df, table, coalesce = 20, mode = "append"):
    """Return None

    Take a data frame and save it as a table. Use parquet and snappy compression for read optimization 
    """
    logging.warn('********************************* writing table *********************************')
    df.coalesce(coalesce).write.format("parquet").option("compression", "snappy").mode(mode).insertInto(table)
    df.show()

def write_redshift_table(df):
    df.write.format("com.databricks.spark.redshift") \
            .option("url", REDSHIFTURL) \
            .option("dbtable", "dev.buildingconnected_local.snapshot_bidder_groups") \
            .option("forward_spark_s3_credentials", true) \
            .option("tempdir", TEMPS3DIR) \
            .mode("error") \
            .save() \
