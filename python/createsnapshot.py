from pyspark.sql.functions import col, lit, max as sparkMax, dense_rank, desc, current_date
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Window 
from datetime import datetime
import logging
import argparse
import buildingconnected as bc

RAW_DATA_PATH =  bc.RAW_DATA_PATH
RAW_TABLE = bc.RAW_TABLE
CLEAN_TABLE = bc.CLEAN_TABLE
DATABASE = bc.DATABASE


TODAYS_PARTITION = datetime.today().strftime('%Y-%m-%d')

def main(inputTable, outputTable, partitionColumns):
    """Return None

    Takes an input table and a snapshot column and appends data to a snapshot table based on recent changes.
    """
    logging.basicConfig(filename='BuildingConnectedCreateSnapshot.log', level=logging.INFO)
    logging.warn('********************************* start snapshot creation *********************************')  

    spark, sc, sqlContext = bc.init_spark("creatsnapshot"+ DATABASE + "." + inputTable)
    inputDF = bc.read_from_s3(spark, inputTable, TODAYS_PARTITION).drop(col("state")).withColumn("ingest_date", current_date())
    
    try:
        #get the last snapshot from the outputTable
        outputDf = bc.read_from_s3(spark, outputTable, TODAYS_PARTITION)        
        #combine the existing snapshot and the new data, rank by the unique identifier (partition columns) and order by timestamp, in this manner the most recent version of a item will be rank 1
        unionDf = inputDF.union(outputDf).withColumn("rank", dense_rank().over(Window.partitionBy(*partitionColumns).orderBy(desc("event_timestamp"))))
        #keep only rank 1 records
        finalDf = unionDf.where(col("rank")==lit("1")).drop(col("rank"))        
    except:#TODO better error handling, this catches ALL errors, not just the one we expect.
        print("It is possible this is the first run and there is no data in the outputtable")
        tempDf = inputDF.withColumn("rank", dense_rank().over(Window.partitionBy(*partitionColumns).orderBy(desc("event_timestamp"))))
        finalDf = tempDf.where(col("rank")==lit("1")).drop(col("rank"))

    bc.write_table(finalDf, outputTable, 10, "overwrite")    
    bc.write_redshift_table(finalDf, outputTable)
    
    logging.warn('********************************* done snapshot creation  *********************************') 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Declare the collection to create a snapshot for')
    parser.add_argument('-c', '--collection', required=True, choices=['project','bid-packages','bidder-groups'], help='the name of the collection')
    args = parser.parse_args()

    if args.collection == "project":
        arg_inputTable = bc.PROJECTS_EVENTS_TABLE
        arg_outputTable = bc.PROJECT_SNAPSHOT_TABLE
        arg_partition_columns = ["projectId"]

    elif args.collection == "bid-packages":
        arg_inputTable = bc.BID_PACKAGES_EVENTS_TABLE
        arg_outputTable = bc.BID_PACKAGES_SNAPSHOT_TABLE
        arg_partition_columns = ["bidPackageId"]
    
    elif args.collection == "bidder-groups":
        arg_inputTable = bc.BIDDER_GROUPS_EVENTS_TABLE
        arg_outputTable = bc.BIDDER_GROUPS_SNAPSHOT_TABLE
        arg_partition_columns = ["biddergroupid"]

    main(arg_inputTable, arg_outputTable, arg_partition_columns)