from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator


default_args: dict = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# spark-submit --master spark://spark:7077 \
# --conf spark.cores.max=4 \
# --conf spark.executor.memory=4g \
# --conf spark.executor.driver=4g \
# --conf spark.executor.memoryOverhead=2g \
# --jars /usr/local/spark/assembly/target/scala-2.13/jars/postgresql-42.5.4.jar \
# --name read_parquet \
# --queue root.default \
# /usr/local/spark/app/etl_process.py

spark_config: dict = {
    "spark.cores.max": "3",
    "spark.executor.memory": "8g",
    # "spark.executor.driver": "8g",
    "spark.executor.memoryOverhead": "400m",
    "spark.driver.memory":"8g",
    "num_executors":'3',
    "executor_cores":'3'
}

with DAG(

    'etl_dag',
    default_args=default_args,
    description='A simple ETL DAG to load data from parquet to PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2023, 10, 30),
    catchup=False,
    tags=["etl", "parquet", "postgresql"]
) as dag:
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    test_postgres_connection = PostgresOperator(
        task_id='test_postgres_connection',
        postgres_conn_id='postgres_default',
        sql='SELECT 1',
        dag=dag
    )

    read_parquet = SparkSubmitOperator(
        task_id='etl_task',
        conf=spark_config,
        name="read_parquet",
        conn_id="spark_default",
        jars="/usr/local/spark/assembly/target/scala-2.13/jars/postgresql-42.5.4.jar",
        application="/usr/local/spark/app/etl_process.py",
    )

    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # check_docker_compose >> test_postgres_connection >>etl_task
    start >> test_postgres_connection >> read_parquet >> end
