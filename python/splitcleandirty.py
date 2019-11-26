from pyspark.sql.functions import  col, split, regexp_replace, current_date
import logging
import buildingconnected as bc

VALID_COLLECTIONS = ["Projects", "BidPackages", "BidderGroups"]

def main():
    """Return None

    Take an input table and split it to two tables (clean and dirty) based on the isClean criteria.. 
    """
    
    logging.basicConfig(filename='BuildingConnectedSplitCleanDirty.log', level=logging.INFO)
    logging.warn('********************************* start splitting clean and dirty *********************************')


    spark, sc, sqlContext = bc.init_spark("splitcleandirty") 

    inputDF=sqlContext.table("buildingconnected.events_raw")
    #inputDF = spark.read.format("parquet").load("s3a://interviewtestbld/scoyne/data/events_raw/")

    flatDf = inputDF.drop("ingest_date").select("*", "message.*").drop("message").withColumn("ingest_date", current_date())

    is_clean = (col("state").isNotNull() & col("collection").isin(VALID_COLLECTIONS))

    #dirty records are where state is null, or collection is invalid type By reviewing the data these cases appear
    #to be data issues where the record is mainly null, or all data is shifted over a column
    logging.warn('********************************* processing dirty table *********************************')
    dfDirty = flatDf.where(is_clean == False)
    bc.write_table(dfDirty, bc.DIRTY_TABLE)
    
    #clean records have a populated state column and a valid collection.
    #transform the keywords column from a string to an array of strings
    logging.warn('********************************* processing clean table *********************************')
    dfClean = flatDf.where(is_clean == True).withColumn("keywords", split(regexp_replace(col("keywords"), "\[|\]", ""), ","))
    bc.write_table(dfClean, bc.CLEAN_TABLE)

    logging.warn('********************************* done splitting clean and dirty *********************************')


if __name__ == "__main__":
    main()