import buildingconnected as bc
import logging
from datetime import datetime
import config as cfg


TODAY = datetime.today().strftime('%Y-%m-%d')


def main():
    """Return None
    Creates the necessary database and tables for the ETL pipeline
    """

    logging.basicConfig(filename='BuildingConnectedCreateTables.log', level=logging.INFO)
    logging.warn('********************************* start creating database and tables  *********************************')
    
    #create local and external schemas
    sql_query = "create schema if not exists buildingconnected"    
    bc.execute_redshift_query(sql_query)
    sql_query =  """create external schema if not exists buildingconnected_external from data catalog 
                    database 'buildingconnected_db' 
                    iam_role 'arn:aws:iam::852056369035:role/RedshiftCopyUnload'
                    create external database if not exists;"""
    bc.execute_redshift_query(sql_query)


    #create events_raw external table for read only query on redshift
    #external table is needed to support struct
    #add today's partition
    sql_query =  """drop table if exists buildingconnected_external.{0}""".format(cfg.RAW_TABLE)
    bc.execute_redshift_query(sql_query)
    sql_query = """CREATE EXTERNAL TABLE dev.buildingconnected_external.{0}
                   ( ingest_timestamp VARCHAR, 
                     ingest_uuid VARCHAR, 
                     collection VARCHAR, 
                     message struct < bidPackageId: VARCHAR, biddergroupid: VARCHAR, bidsSealed: VARCHAR,
                                      companyId: VARCHAR, creatorId: VARCHAR, dateBidsDue: date, 
                                      dateCreated: date, dateEnd: date, dateFirstInvited: date,
                                      dateFirstViewed: date, datePublished: date, dateRFIsDue: date, 
                                      dateStart: date, keywords: VARCHAR, ndaRequired: VARCHAR, 
                                      officeId: VARCHAR, projectId: VARCHAR, public: VARCHAR, state: VARCHAR >,
                     event_timestamp VARCHAR, 
                     event_type VARCHAR 
                    )
                partitioned by ({1} date)
                STORED AS PARQUET
                LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{0}/'
                table properties ('compression'='snappy');""".format(cfg.RAW_TABLE, cfg.PARTITION_COLUMN)
    bc.execute_redshift_query(sql_query)
    sql_query = """ALTER TABLE dev.buildingconnected_external.{2} ADD if not exists PARTITION ({1}='{0}') 
                    LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{2}/{1}={0}';""".format(TODAY, cfg.PARTITION_COLUMN, cfg.RAW_TABLE)
    bc.execute_redshift_query(sql_query)


    #create events_clean external table for read only query on redshift
    #add today's partition
    sql_query =  """drop table if exists buildingconnected_external.{0}""".format(cfg.CLEAN_TABLE)
    bc.execute_redshift_query(sql_query)
    sql_query = """CREATE EXTERNAL TABLE dev.buildingconnected_external.{0}
                   ( ingest_timestamp VARCHAR, ingest_uuid VARCHAR, collection VARCHAR, event_timestamp VARCHAR, 
                     event_type VARCHAR, bidPackageId VARCHAR, biddergroupid VARCHAR, bidsSealed VARCHAR, companyId VARCHAR,
                     creatorId VARCHAR, dateBidsDue date, dateCreated date, dateEnd date, dateFirstInvited date,
                     dateFirstViewed date, datePublished date, dateRFIsDue date, dateStart date, keywords VARCHAR, 
                     ndaRequired VARCHAR, officeId VARCHAR, projectId VARCHAR, public VARCHAR, state VARCHAR
                    )
                partitioned by ({1} date)
                STORED AS PARQUET
                LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{0}/'
                table properties ('compression'='snappy');""" .format(cfg.CLEAN_TABLE, cfg.PARTITION_COLUMN)

    bc.execute_redshift_query(sql_query)
    sql_query = """ALTER TABLE dev.buildingconnected_external.{2} ADD if not exists PARTITION ({1}='{0}') 
                    LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{2}/{1}={0}';""".format(TODAY, cfg.PARTITION_COLUMN, cfg.CLEAN_TABLE)
    bc.execute_redshift_query(sql_query)


    #create events_dirty external table for read only query on redshift
    #add today's partition
    sql_query =  """drop table if exists buildingconnected_external.{0}""".format(cfg.DIRTY_TABLE)
    bc.execute_redshift_query(sql_query)
    sql_query = """CREATE EXTERNAL TABLE dev.buildingconnected_external.{0}
                   ( ingest_timestamp VARCHAR, ingest_uuid VARCHAR, collection VARCHAR, event_timestamp VARCHAR, 
                     event_type VARCHAR, bidPackageId VARCHAR, biddergroupid VARCHAR, bidsSealed VARCHAR, companyId VARCHAR,
                     creatorId VARCHAR, dateBidsDue date, dateCreated date, dateEnd date, dateFirstInvited date,
                     dateFirstViewed date, datePublished date, dateRFIsDue date, dateStart date, keywords VARCHAR, 
                     ndaRequired VARCHAR, officeId VARCHAR, projectId VARCHAR, public VARCHAR, state VARCHAR
                    )
                partitioned by ({1} date)
                STORED AS PARQUET
                LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{0}/'
                table properties ('compression'='snappy');""" .format(cfg.DIRTY_TABLE, cfg.PARTITION_COLUMN)
    bc.execute_redshift_query(sql_query)
    sql_query = """ALTER TABLE dev.buildingconnected_external.{2} ADD if not exists PARTITION ({1}='{0}') 
                    LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{2}/{1}={0}';""".format(TODAY, cfg.PARTITION_COLUMN, cfg.DIRTY_TABLE)
    bc.execute_redshift_query(sql_query)   


    #create project events external table for read only query on redshift
    #add today's partition
    sql_query =  """drop table if exists buildingconnected_external.{0}""".format(cfg.PROJECTS_EVENTS_TABLE)
    bc.execute_redshift_query(sql_query)
    sql_query = """CREATE EXTERNAL TABLE dev.buildingconnected_external.{0}
                   (
                    event_timestamp VARCHAR, event_type VARCHAR, state VARCHAR, projectId VARCHAR, public VARCHAR, officeID VARCHAR, 
                    ndaRequired VARCHAR, dateStart date, dateRFIsDue date, dateEnd date, dateCreated date, dateBidsDue date,
                    creatorId VARCHAR, companyId VARCHAR, bidsSealed VARCHAR, ingest_uuid VARCHAR, ingest_timestamp VARCHAR
                    ) 
                partitioned by ({1} date)
                STORED AS PARQUET
                LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{0}/'
                table properties ('compression'='snappy');""" .format(cfg.PROJECTS_EVENTS_TABLE, cfg.PARTITION_COLUMN)
    bc.execute_redshift_query(sql_query)
    sql_query = """ALTER TABLE dev.buildingconnected_external.{2} ADD if not exists PARTITION ({1}='{0}') 
                    LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{2}/{1}={0}';""".format(TODAY, cfg.PARTITION_COLUMN, cfg.PROJECTS_EVENTS_TABLE)
    bc.execute_redshift_query(sql_query)   


    #create bid packages events external table for read only query on redshift
    #add today's partition
    sql_query =  """drop table if exists buildingconnected_external.{0}""".format(cfg.BID_PACKAGES_EVENTS_TABLE)
    bc.execute_redshift_query(sql_query)
    sql_query = """CREATE EXTERNAL TABLE dev.buildingconnected_external.{0}
                   (event_timestamp VARCHAR, event_type VARCHAR, state VARCHAR, keywords VARCHAR, dateStart date, datePublished date, 
                    dateEnd date, dateCreated date, dateBidsDue date, creatorId VARCHAR, bidPackageId VARCHAR, ingest_uuid VARCHAR, 
                    ingest_timestamp VARCHAR)
                partitioned by ({1} date)
                STORED AS PARQUET
                LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{0}/'
                table properties ('compression'='snappy');""".format(cfg.BID_PACKAGES_EVENTS_TABLE, cfg.PARTITION_COLUMN)
    bc.execute_redshift_query(sql_query)
    sql_query = """ALTER TABLE dev.buildingconnected_external.{2} ADD if not exists PARTITION ({1}='{0}') 
                    LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{2}/{1}={0}';""".format(TODAY, cfg.PARTITION_COLUMN, cfg.BID_PACKAGES_EVENTS_TABLE)
    bc.execute_redshift_query(sql_query)   


    #create bidder groups events external table for read only query on redshift
    #add today's partition
    sql_query =  """drop table if exists buildingconnected_external.{0}""".format(cfg.BIDDER_GROUPS_EVENTS_TABLE)
    bc.execute_redshift_query(sql_query)
    sql_query = """CREATE EXTERNAL TABLE dev.buildingconnected_external.{0}
                   (event_timestamp VARCHAR, event_type VARCHAR, state VARCHAR, officeID VARCHAR, ndaRequired VARCHAR, dateFirstViewed date,
                    dateFirstInvited date, dateCreated date,  companyId VARCHAR, biddergroupid VARCHAR, bidPackageId VARCHAR, ingest_uuid VARCHAR, ingest_timestamp VARCHAR)
                partitioned by ({1} date)
                STORED AS PARQUET
                LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{0}/'
                table properties ('compression'='snappy');""" .format(cfg.BIDDER_GROUPS_EVENTS_TABLE, cfg.PARTITION_COLUMN)
    bc.execute_redshift_query(sql_query)
    sql_query = """ALTER TABLE dev.buildingconnected_external.{2} ADD if not exists PARTITION ({1}='{0}') 
                    LOCATION 's3://interviewtestbld/scoyne/data/buildingconnected.{2}/{1}={0}';""".format(TODAY, cfg.PARTITION_COLUMN, cfg.BIDDER_GROUPS_EVENTS_TABLE)
    bc.execute_redshift_query(sql_query)   


    #create snapshot tables local on redshift.
    sql_query = """CREATE TABLE IF NOT EXISTS dev.buildingconnected.snapshot_bidder_groups 
                   ( event_timestamp VARCHAR, event_type VARCHAR, officeID VARCHAR, ndaRequired VARCHAR, 
                     dateFirstViewed VARCHAR, dateFirstInvited VARCHAR, dateCreated VARCHAR, companyId VARCHAR, 
                     biddergroupid VARCHAR, bidPackageId VARCHAR, ingest_uuid VARCHAR, ingest_timestamp VARCHAR, ingest_date DATE) """ 

    bc.execute_redshift_query(sql_query)

    sql_query = """CREATE TABLE IF NOT EXISTS dev.buildingconnected.snapshot_projects 
                   (event_timestamp VARCHAR, event_type VARCHAR, projectId VARCHAR, public VARCHAR, 
                   officeID VARCHAR, ndaRequired VARCHAR, dateStart VARCHAR, dateRFIsDue VARCHAR,
                   dateEnd VARCHAR, dateCreated VARCHAR, dateBidsDue VARCHAR, creatorId VARCHAR, 
                   companyId VARCHAR, bidsSealed VARCHAR, ingest_uuid VARCHAR, ingest_timestamp VARCHAR, ingest_date DATE) """ 
    bc.execute_redshift_query(sql_query)

    sql_query = """CREATE TABLE IF NOT EXISTS dev.buildingconnected.snapshot_bid_packages 
                   (event_timestamp VARCHAR, event_type VARCHAR, keywords VARCHAR, dateStart VARCHAR, datePublished VARCHAR, 
                    dateEnd VARCHAR, dateCreated VARCHAR, dateBidsDue VARCHAR, creatorId VARCHAR, bidPackageId VARCHAR, ingest_uuid VARCHAR, 
                    ingest_timestamp VARCHAR, ingest_date VARCHAR) """ 
    bc.execute_redshift_query(sql_query)


    #create date dimension external table for read only query on redshift
    sql_query =  """drop table if exists buildingconnected_external.{0}""".format(cfg.DIM_DATES_TABLE)
    bc.execute_redshift_query(sql_query)
    sql_query = """CREATE EXTERNAL TABLE dev.buildingconnected_external.{0}
                   ( DateNum integer, Date date, YearMonthNum integer, Calendar_Quarter VARCHAR, MonthNum integer, MonthName VARCHAR, MonthShortName VARCHAR, WeekNum integer,
                     DayNumOfYear integer, DayNumOfMonth integer, DayNumOfWeek integer, DayName VARCHAR, DayShortName VARCHAR, Quarter integer, YearQuarterNum integer, DayNumOfQuarter integer
                    )             
                row format delimited
                fields terminated by ','  
                STORED AS TEXTFILE
                LOCATION 's3://interviewtestbld/scoyne/data/dim_data/{0}/'  ;""" .format(cfg.DIM_DATES_TABLE)
    bc.execute_redshift_query(sql_query)
   
    logging.warn('********************************* end creating database and tables  *********************************')


if __name__ == "__main__":
    main()