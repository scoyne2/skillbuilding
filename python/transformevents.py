from pyspark.sql.functions import col, lit
import logging
import argparse
import buildingconnected as bc

#def transform_events_tables(inputTable, outputTable, columns, collection):
def main(outputTable, columns, collection):
    """Return None

    Take an input table and a list of columns and output a table with only the required columns. 
    """  
    logging.basicConfig(filename='BuildingConnectedTransformEvents.log', level=logging.INFO)
    logging.warn('********************************* start transform events *********************************')  

    spark, sc, sqlContext = buildingconnected.init_spark("transformevents" + bc.CLEAN_TABLE)

    finalDf = spark.table(bc.CLEAN_TABLE).select(*columns).where(col("collection") == lit(collection)).distinct()    
    buildingconnected.write_table(finalDf, outputTable)
    
    logging.warn('********************************* done transform events *********************************') 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Declare the collection to transform')
    parser.add_argument('-c', '--collection', required=True, choices=['project','bid-packages','bidder-groups'], help='the name of the collection')
    args = parser.parse_args()

    if args.c == "project":
        arg_outputTable = buildingconnected.PROJECTS_EVENTS_TABLE
        arg_columns = ["event_timestamp", "event_type", "state", "projectId", "public", "officeID", "ndaRequired", "dateStart", "dateRFIsDue", "dateEnd", "dateCreated", "dateBidsDue", "creatorId", "companyId", "bidsSealed",  "ingest_uuid", "ingest_timestamp", "ingest_date"]
        arg_collection = "Projects"
    elif args.c == "bid-packages":
        arg_outputTable = buildingconnected.BID_PACKAGES_EVENTS_TABLE
        arg_columns = ["event_timestamp","event_type","state","keywords","dateStart","datePublished","dateEnd","dateCreated","dateBidsDue","creatorId","bidPackageId",  "ingest_uuid", "ingest_timestamp", "ingest_date"]
        arg_collection = "BidPackages"
    elif args.c == "bidder-groups":
        arg_outputTable = buildingconnected.BIDDER_GROUPS_EVENTS_TABLE
        arg_columns = ["event_timestamp","event_type","state","officeID","ndaRequired","dateFirstViewed","dateFirstInvited","dateCreated","companyId","biddergroupid","bidPackageId",  "ingest_uuid",  "ingest_timestamp","ingest_date"]
        arg_collection = "BidderGroups"

    main(arg_outputTable, arg_columns, arg_collection)