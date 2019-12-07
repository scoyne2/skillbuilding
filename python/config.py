import os

#AWS Credentials
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
