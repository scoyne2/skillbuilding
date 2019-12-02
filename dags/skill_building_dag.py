from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'root',
    'run_as': 'root'}

dag = DAG(
    "skill_building",
    default_args=default_args,
    description="skill building DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False)


#--deploy-mode cluster --master yarn   need to set YARN_CONF_DIR in dockerfile
#TODO find a better way to build the bash command
create_tables = BashOperator(
    task_id="create_tables",
    bash_command=" spark-submit  --packages org.apache.hadoop:hadoop-aws:2.7.0 --conf 'spark.sql.warehouse.dir=/usr/local/airflow/spark-warehouse/' --conf 'spark.driver.extraJavaOptions=-Dderby.system.home=/usr/local/airflow/derby' /usr/local/airflow/python/createtables.py",
    retries=3,
    dag=dag,
)

extract_raw = BashOperator(
    task_id="extract_raw",
    bash_command=" spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 --conf 'spark.sql.warehouse.dir=/usr/local/airflow/spark-warehouse/' --conf 'spark.driver.extraJavaOptions=-Dderby.system.home=/usr/local/airflow/derby' /usr/local/airflow/python/extractraw.py",
    retries=3,
    dag=dag,
)

split_clean_dirty = BashOperator(
    task_id="split_clean_dirty",
    bash_command=" spark-submit  --packages org.apache.hadoop:hadoop-aws:2.7.0  --conf 'spark.sql.warehouse.dir=/usr/local/airflow/spark-warehouse/' --conf 'spark.driver.extraJavaOptions=-Dderby.system.home=/usr/local/airflow/derby' /usr/local/airflow/python/splitcleandirty.py",
    retries=3,
    dag=dag,
)

transform_projects = BashOperator(
    task_id="transform_projects",
    bash_command=" spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0  --conf 'spark.sql.warehouse.dir=/usr/local/airflow/spark-warehouse/' --conf 'spark.driver.extraJavaOptions=-Dderby.system.home=/usr/local/airflow/derby' /usr/local/airflow/python/transformevents.py -c project",
    retries=3,
    dag=dag,
)

transform_bid_packages = BashOperator(
    task_id="transform_bid_packages",
    bash_command=" spark-submit  --packages org.apache.hadoop:hadoop-aws:2.7.0  --conf 'spark.sql.warehouse.dir=/usr/local/airflow/spark-warehouse/' --conf 'spark.driver.extraJavaOptions=-Dderby.system.home=/usr/local/airflow/derby' /usr/local/airflow/python/transformevents.py -c bid-packages",
    retries=3,
    dag=dag,
)

transform_bidder_groups = BashOperator(
    task_id="transform_bidder_groups",
    bash_command=" spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0  --conf 'spark.sql.warehouse.dir=/usr/local/airflow/spark-warehouse/' --conf 'spark.driver.extraJavaOptions=-Dderby.system.home=/usr/local/airflow/derby' /usr/local/airflow/python/transformevents.py -c bidder-groups",
    retries=3,
    dag=dag,
)

snapshot_projects = BashOperator(
    task_id="snapshot_projects",
    bash_command=" spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 --conf 'spark.sql.warehouse.dir=/usr/local/airflow/spark-warehouse/' --conf 'spark.driver.extraJavaOptions=-Dderby.system.home=/usr/local/airflow/derby' /usr/local/airflow/python/createsnapshot.py -c project",
    retries=3,
    dag=dag,
)

snapshot_bid_packages = BashOperator(
    task_id="snapshot_bid_packages",
    bash_command=" spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0  --conf 'spark.sql.warehouse.dir=/usr/local/airflow/spark-warehouse/' --conf 'spark.driver.extraJavaOptions=-Dderby.system.home=/usr/local/airflow/derby' /usr/local/airflow/python/createsnapshot.py -c bid-packages",
    retries=3,
    dag=dag,
)

snapshot_bidder_groups = BashOperator(
    task_id="snapshot_bidder_groups",
    bash_command=" spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.0 --jars https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar --conf 'spark.sql.warehouse.dir=/usr/local/airflow/spark-warehouse/' --conf 'spark.driver.extraJavaOptions=-Dderby.system.home=/usr/local/airflow/derby' /usr/local/airflow/python/createsnapshot.py -c bidder-groups",
    retries=3,
    dag=dag,
)

create_tables >> extract_raw >> split_clean_dirty 
split_clean_dirty >> transform_projects >> snapshot_projects
split_clean_dirty >> transform_bid_packages >> snapshot_bid_packages
split_clean_dirty >> transform_bidder_groups >> snapshot_bidder_groups
