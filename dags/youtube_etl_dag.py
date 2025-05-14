from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='youtube_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_spark_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command="""
        cd /mnt/sda4/Project/youtube-analytics && \
        source venv/bin/activate && \
        python run_etl.py
        """
    )
