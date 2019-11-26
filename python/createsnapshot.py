from pyspark.sql.functions import col, lit, max as sparkMax, dense_rank, desc
from pyspark.sql.utils import AnalysisException
import logging
import argparse
import buildingconnected

RAW_DATA_PATH =  buildingconnected.RAW_DATA_PATH
RAW_TABLE = buildingconnected.RAW_TABLE
CLEAN_TABLE = buildingconnected.CLEAN_TABLE

def main(inputTable, outputTable, partitionColumns):
   """Return None

    Takes an input table and a snapshot column and appends data to a snapshot table based on recent changes.
    """
    logging.basicConfig(filename='BuildingConnectedCreateSnapshot.log', level=logging.INFO)
    logging.warn('********************************* start snapshot creation *********************************')  

    spark, sc, sqlContext = buildingconnected.init_spark("creatsnapshot"+inputTable)
    maxInputDate = spark.table(inputTable).agg(sparkMax(col("ingest_date"))).collect()[0][0]
    inputDF = spark.table(inputTable).where(col("ingest_date") == lit(maxInputDate)).drop(col("state"))
    #get the last snapshot from the outputTable
    maxSnapshotDate = spark.table(outputTable).agg(sparkMax(col("ingest_date"))).collect()[0][0]
    outputDf = spark.table(outputTable).where(col("ingest_date") == lit(maxSnapshotDate))
    #combine the existing snapshot and the new data, rank by the unique identifier (partition columns) and order by timestamp, in this manner the most recent version of a item will be rank 1
    unionDf = inputDF.union(outputDf).withColumn("rank", dense_rank().over(Window.partitionBy(*partitionColumns).orderBy(desc("event_timestamp"))))
    #keep only rank 1 records
    finalDf = unionDf.where(col("rank")==lit("1")).drop(col("rank"))
    buildingconnected.write_table(finalDf, outputTable, 10, "overwrite")
    
    logging.warn('********************************* done snapshot creation  *********************************') 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Declare the collection to create a snapshot for')
    parser.add_argument('-c', '--collection', required=True, choices=['project','bid-packages','bidder-groups'], help='the name of the collection')
    args = parser.parse_args()

    if args.c == "project":
        arg_inputTable = buildingconnected.PROJECTS_EVENTS_TABLE
        arg_outputTable = buildingconnected.PROJECT_SNAPSHOT_TABLE
        arg_partition_columns = ["projectId"]
    elif args.c == "bid-packages":
        arg_inputTable = buildingconnected.BID_PACKAGES_EVENTS_TABLE
        arg_outputTable = buildingconnected.BID_PACKAGES_SNAPSHOT_TABLE
        arg_partition_columns = ["bidPackageId"]
    elif args.c == "bidder-groups":
        arg_inputTable = buildingconnected.BIDDER_GROUPS_EVENTS_TABLE
        arg_outputTable = buildingconnected.BIDDER_GROUPS_SNAPSHOT_TABLE
        arg_partition_columns = ["biddergroupid"]

    main(arg_inputTable, arg_outputTable, arg_partition_columns)