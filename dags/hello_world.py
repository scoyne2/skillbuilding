from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def print_hello():
    return "Hello world!"


dag = DAG(
    "hello_world",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)


dummy_operator = DummyOperator(task_id="dummy_task", retries=3, dag=dag)
dummy_operator2 = DummyOperator(task_id="dummy_task2", retries=3, dag=dag)


hello_operator = PythonOperator(
    task_id="hello_task", python_callable=print_hello, dag=dag
)

spark_task = BashOperator(
    task_id="spark_test",
    bash_command="spark-submit --master yarn /usr/local/airflow/python/hello_spark.py",
    dag=dag,
)
spark_task >> hello_operator
dummy_operator >> hello_operator
dummy_operator2 >> hello_operator
