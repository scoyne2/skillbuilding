from pyspark.sql.functions import current_date
from pyspark.sql.utils import AnalysisException
import logging
import buildingconnected as bc

def main():
    """Return None

    Take path to json files and extract them to a hive table. Rename the @ columns for easier readability. 
    """
    logging.basicConfig(filename='BuildingConnectedExtractRaw.log', level=logging.INFO)
    logging.warn('********************************* start extracting raw *********************************')  

    spark, sc, sqlContext = bc.init_spark("extractraw")
    #Read the json to a dataframe and add a new column for use as the partition, populated by today's date.
    #catch exceptions caused by the source folder being empty (for the case where we schedule this on the hour but maybe json files have not yet arrived)
    
    print("******* trying to show tables")
    sqlContext.sql("show tables in buildingconnected").show()

    try:
        rawDF = sqlContext.read.format("json").load(bc.RAW_DATA_PATH).withColumn("ingest_date", current_date())
        #Rename and reorder columns
        df = rawDF.withColumnRenamed("@timestamp", "ingest_timestamp").withColumnRenamed("@uuid", "ingest_uuid").withColumnRenamed("timestamp", "event_timestamp")
        #Append new data, since we are using append and not overwrite backfills/re-run will be tough. We also need to delete the source json after it is loaded
        #Destination table is partitioned by ingest_date, coalesce will need to be adjusted once production data size is known
        #in order to prevent spark small files issues, coalese to 1 file per partition for now
        bc.write_table(df, bc.RAW_TABLE)
    except AnalysisException:
        logging.warn('AnalysisException: Could not determine JSON schema, it is likely no new JSON files have arrived thus the source folder is empty')
    
    logging.warn('********************************* done extracting raw *********************************') 

if __name__ == "__main__":
    main()