from pyspark.sql import SparkSession, SQLContext, HiveContext
import logging
import psycopg2
import boto3
import os

#Credentials
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
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
RAW_TABLE = "events_raw"
DIRTY_TABLE = "events_dirty"
CLEAN_TABLE = "events_clean"
PROJECTS_EVENTS_TABLE = "events_projects"
BID_PACKAGES_EVENTS_TABLE = "events_bid_packages"
BIDDER_GROUPS_EVENTS_TABLE = "events_bidder_groups"
PROJECT_SNAPSHOT_TABLE = "snapshot_projects"
BID_PACKAGES_SNAPSHOT_TABLE = "snapshot_bid_packages"
BIDDER_GROUPS_SNAPSHOT_TABLE = "snapshot_bidder_groups"
DIM_DATES_TABLE = "dim_dates"
#Options
PARTITION_COLUMN = "ingest_date"


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


def write_table(df, table, coalesce = 20, mode = "append"):
    """Return None

    Take a data frame and save it as a table. Use parquet and snappy compression for read optimization 
    """
    logging.warn('********************************* writing table *********************************')
    #df.coalesce(coalesce).write.format("csv").option("header","false").mode(mode).insertInto(DATABASE + "." + table)
    df.coalesce(coalesce).write \
        .format("parquet") \
        .option("header","false") \
        .mode("Overwrite") \
        .partitionBy(PARTITION_COLUMN) \
        .save("{0}{1}.{2}".format(FOLDER_PATH, DATABASE, table))
    df.show()

    
def read_from_s3(spark, table, partition = "*", folderPath = FOLDER_PATH, database=DATABASE, partitionColumn=PARTITION_COLUMN):
    """Return DataFRame

    Return a dataframe from files in S3
    """
    logging.warn('********************************* reading from s3 *********************************')
    path = "{0}{1}.{2}/{3}={4}".format(folderPath, database, table, partitionColumn, partition)
    return spark.read.load(path)


def write_redshift_table(df, tableName):
   #write temp table as csv split into 20 parts but no compression and no partition
    df.coalesce(1) \
        .write.format("CSV") \
        .option("header","false") \
        .mode("Overwrite") \
        .save(TEMPS3DIR+"/"+tableName)

    sql_query = """delete dev.buildingconnected.{0}""".format(tableName)
    execute_redshift_query(sql_query)

    #copy from temp table to redshift table of same name 
    sql_query = """copy dev.buildingconnected.{0}  
                   from 's3://interviewtestbld/temp/{0}'
                   iam_role 'arn:aws:iam::852056369035:role/RedshiftCopyUnload'
                   format as CSV
                """.format(tableName)
    execute_redshift_query(sql_query)


def execute_redshift_query(sql_query):
    """Return None

    Take a sql query and execute it on the redshift cluster.
    """
    con = psycopg2.connect(dbname=DB_NAME,host=HOST,port=PORT,user=USER,password=PASSWORD)
    con.set_session(autocommit=True)
    cur = con.cursor()
    cur.execute(sql_query)
    con.commit()
    cur.close()
    con.close()