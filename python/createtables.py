import buildingconnected as bc
import logging


# CREATE EXTERNAL SCHEMA IF NOT EXISTS buildingconnected_external
# FROM HIVE METASTORE 
# DATABASE 'dev'
# URI 'ec2-52-53-151-146.us-west-1.compute.amazonaws.com'
# IAM_ROLE 'arn:aws:iam::852056369035:role/bigdatacertadmin'


# CREATE EXTERNAL TABLE dev.buildingconnected.dim_dates (DateNum int, Date date, YearMonthNum integer, Calendar_Quarter string, MonthNum integer, MonthName string, MonthShortName string, WeekNum integer, DayNumOfYear integer, DayNumOfMonth integer, DayNumOfWeek integer, DayName string, DayShortName string, Quarter integer, YearQuarterNum integer, DayNumOfQuarter integer) 
# stored as textfile 
# location 's3://interviewtestbld/scoyne/data/dim_data/dim_dates';

#create schema if not exists buildingconnected_local;






def main():
    sql_query = "create schema if not exists buildingconnected_local"    
    bc.execute_redshift_query(sql_query)

    sql_query = "CREATE TABLE dev.buildingconnected_local.snapshot_bidder_groups (event_timestamp VARCHAR, event_type VARCHAR, officeID VARCHAR, ndaRequired VARCHAR, dateFirstViewed date, dateFirstInvited date, dateCreated date, companyId VARCHAR, biddergroupid VARCHAR, bidPackageId VARCHAR, ingest_uuid VARCHAR, ingest_timestamp VARCHAR)"   
    #bc.execute_redshift_query(sql_query)

    main2()


def main2():
    """Return None

    Creates the necessary database and tables for the ETL pipeline
    """
    logging.basicConfig(filename='BuildingConnectedCreateTables.log', level=logging.INFO)
    logging.warn('********************************* start creating database and tables  *********************************')
    logging.warn('********************************* start spark  *********************************')

    spark, sc, sqlContext = bc.init_spark("createtables")
    logging.warn('********************************* create db  *********************************')

    #create DATABASE        
    sqlContext.sql(" CREATE DATABASE IF NOT EXISTS {0} ".format(bc.DATABASE))
    
    logging.warn('********************************* create raw table  *********************************')

    #create raw table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (ingest_timestamp string, ingest_uuid string, collection string, message STRUCT <bidPackageId: string, biddergroupid: string, bidsSealed: string, \
        companyId: string, creatorId: string, dateBidsDue: date, dateCreated: date, dateEnd: date, dateFirstInvited: date, dateFirstViewed: date, datePublished: date, dateRFIsDue: date, dateStart: date, \
        keywords: string, ndaRequired: string, officeId: string, projectId: string, public: string, state: string>, event_timestamp string, event_type string) \
        PARTITIONED BY (ingest_date string) \
        LOCATION '{0}{1}'".format(bc.FOLDER_PATH, bc.RAW_TABLE))
   
    #create dirty table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (ingest_timestamp string, ingest_uuid string, collection string, event_timestamp string, event_type string, bidPackageId string, biddergroupid string,\
        bidsSealed string, companyId string, creatorId string, dateBidsDue date, dateCreated date, dateEnd date, dateFirstInvited date, dateFirstViewed date, datePublished date, dateRFIsDue date, dateStart date, \
        keywords string, ndaRequired string, officeId string, projectId string, public string, state string) \
        PARTITIONED BY (ingest_date string) \
        LOCATION '{0}{1}'".format(bc.FOLDER_PATH, bc.DIRTY_TABLE))
    
    #create clean table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (ingest_timestamp string, ingest_uuid string, collection string, event_timestamp string, event_type string, bidPackageId string, biddergroupid string, \
        bidsSealed string, companyId string, creatorId string, dateBidsDue date, dateCreated date, dateEnd date, dateFirstInvited date, dateFirstViewed date, datePublished date, dateRFIsDue date, dateStart date, \
        keywords array<string>, ndaRequired string, officeId string, projectId string, public string, state string) \
        PARTITIONED BY (ingest_date string) \
        LOCATION '{0}{1}'".format(bc.FOLDER_PATH, bc.CLEAN_TABLE))
    
    #create projects transactions table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (event_timestamp string, event_type string, state string, projectId string, public string, officeID string, ndaRequired string, dateStart date, dateRFIsDue date,\
        dateEnd date, dateCreated date, dateBidsDue date, creatorId string, companyId string, bidsSealed string, ingest_uuid string, ingest_timestamp string) \
        PARTITIONED BY (ingest_date string) \
        LOCATION '{0}{1}'".format(bc.FOLDER_PATH, bc.PROJECTS_EVENTS_TABLE))

    #create bid packages transactions table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (event_timestamp string, event_type string, state string, keywords array<string>, dateStart date, datePublished date, dateEnd date, dateCreated date, dateBidsDue date, \
        creatorId string, bidPackageId string, ingest_uuid string, ingest_timestamp string) \
        PARTITIONED BY (ingest_date string) \
        LOCATION '{0}{1}'".format(bc.FOLDER_PATH, bc.BID_PACKAGES_EVENTS_TABLE))

    #create bidder groups transactions table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (event_timestamp string, event_type string, state string, officeID string, ndaRequired string, dateFirstViewed date, dateFirstInvited date, \
        dateCreated date,  companyId string, biddergroupid string, bidPackageId string, ingest_uuid string, ingest_timestamp string) \
        PARTITIONED BY (ingest_date string) \
        LOCATION '{0}{1}'".format(bc.FOLDER_PATH, bc.BIDDER_GROUPS_EVENTS_TABLE))

    #create projects snapshot table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (event_timestamp string, event_type string, projectId string, public string, officeID string, ndaRequired string, dateStart date, dateRFIsDue date,\
        dateEnd date, dateCreated date, dateBidsDue date, creatorId string, companyId string, bidsSealed string, ingest_uuid string, ingest_timestamp string) \
        PARTITIONED BY (ingest_date string) \
        LOCATION '{0}{1}'".format(bc.FOLDER_PATH, bc.PROJECT_SNAPSHOT_TABLE))

    #create bid packages snapshot table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (event_timestamp string, event_type string, keywords array<string>, dateStart date, datePublished date, dateEnd date, dateCreated date, dateBidsDue date, \
        creatorId string, bidPackageId string, ingest_uuid string, ingest_timestamp string) \
        PARTITIONED BY (ingest_date string) \
        LOCATION '{0}{1}'".format(bc.FOLDER_PATH, bc.BID_PACKAGES_SNAPSHOT_TABLE))

    #create bidder groups snapshot table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (event_timestamp string, event_type string, officeID string, ndaRequired string, dateFirstViewed date, dateFirstInvited date, \
        dateCreated date,  companyId string, biddergroupid string, bidPackageId string, ingest_uuid string, ingest_timestamp string) \
        PARTITIONED BY (ingest_date string) \
        LOCATION '{0}{1}'".format(bc.FOLDER_PATH, bc.BIDDER_GROUPS_SNAPSHOT_TABLE))

    #create date dimension table
    sqlContext.sql(" CREATE EXTERNAL TABLE IF NOT EXISTS {1} (DateNum integer, Date date, YearMonthNum integer, Calendar_Quarter string, MonthNum integer, MonthName string, MonthShortName string, WeekNum integer, \
        DayNumOfYear integer, DayNumOfMonth integer, DayNumOfWeek integer, DayName string, DayShortName string, Quarter integer, YearQuarterNum integer, DayNumOfQuarter integer) \
        LOCATION '{0}dim_data/dim_dates/'".format(bc.FOLDER_PATH, bc.DIM_DATES_TABLE))

    print("******* trying to show tables")
    sqlContext.sql("show tables in buildingconnected").show()

    logging.warn('********************************* end creating database and tables  *********************************')

if __name__ == "__main__":
    main()