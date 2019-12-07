from pyspark.sql import SparkSession, SQLContext, HiveContext
import logging
import psycopg2
import boto3
import config as cfg

def log_progress(func):
    """Return none
    Apply  logging at start, exception and end
    """
    def wrapper_logger(*args, **kwargs):
        funcName = func.__name__
        logging.warning('********************************* starting {0} *********************************'.format(funcName))
        try:
            return func(*args, **kwargs)
        except:
            logging.warning('********************************* exception occured while running {0} *********************************'.format(funcName))
        finally:
            logging.warning('********************************* end {0} *********************************'.format(funcName))
    return wrapper_logger


@log_progress
def init_spark(appName):
    """Return SparkSession, SparkContext and HiveContext
    Initialize spark session for data processing 
    """
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    return spark, sc, sqlContext


@log_progress
def write_table(df, table, coalesce = 20, mode = "append"):
    """Return None
    Take a data frame and save it as a table. Use parquet and snappy compression for read optimization 
    """
    df.coalesce(coalesce).write \
        .format("parquet") \
        .option("header","false") \
        .mode("Overwrite") \
        .partitionBy(cfg.PARTITION_COLUMN) \
        .save("{0}{1}.{2}".format(cfg.FOLDER_PATH, cfg.DATABASE, table))
    df.show()

    
@log_progress
def read_from_s3(spark, table, partition = "*", folderPath = cfg.FOLDER_PATH, database=cfg.DATABASE, partitionColumn=cfg.PARTITION_COLUMN):
    """Return DataFRame
    Return a dataframe from files in S3
    """
    path = "{0}{1}.{2}/{3}={4}".format(folderPath, database, table, partitionColumn, partition)
    return spark.read.load(path)


@log_progress
def write_redshift_table(df, tableName):
    """Return None
    Write a dataframe to the redshift cluster by storing in a temp S3 bucket and copying
    """    
    df.coalesce(1) \
        .write.format("CSV") \
        .option("header","false") \
        .mode("Overwrite") \
        .save(cfgTEMPS3DIR+"/"+tableName)

    sql_query = """delete dev.buildingconnected.{0}""".format(tableName)
    execute_redshift_query(sql_query)

    #copy from temp table to redshift table of same name 
    sql_query = """copy dev.buildingconnected.{0}  
                   from 's3://interviewtestbld/temp/{0}'
                   iam_role 'arn:aws:iam::852056369035:role/RedshiftCopyUnload'
                   format as CSV
                """.format(tableName)
    execute_redshift_query(sql_query)


@log_progress
def execute_redshift_query(sql_query):
    """Return None
    Take a sql query and execute it on the redshift cluster.
    """
    con = psycopg2.connect(dbname=cfg.DB_NAME,host=cfg.HOST,port=cfg.PORT,user=cfg.USER,password=cfg.PASSWORD)
    con.set_session(autocommit=True)
    cur = con.cursor()
    cur.execute(sql_query)
    con.commit()
    cur.close()
    con.close()