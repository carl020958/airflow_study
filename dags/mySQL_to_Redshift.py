from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.models import Variable
from datetime import datetime, timedelta


select_sql = "SELECT * FROM {}.{} WHERE DATE(created_at) = DATE('{}')"\
        .format("{{params.schema}}", "{{params.table}}", "{{execution_date}}")


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    "mySQL_to_Redshift",
    start_date=datetime(2022, 5, 7),
    schedule_interval="0 9 * * *", 
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
    params={
        "schema": Variable.get("schema"),
        "table": Variable.get("table"),
        "s3_bucket": Variable.get("s3_bucket"),
        "s3_key": f'{Variable.get("schema")}-{Variable.get("table")}'
    }

    ) as dag:

    s3_folder_cleanup = S3DeleteObjectsOperator(
            task_id="s3_folder_cleanup",
            bucket="{{params.s3_bucket}}",
            keys="{{params.s3_key}}",
            aws_conn_id="aws_conn_id",
            dag=dag
            )

    mysql_to_s3_nps = MySQLToS3Operator(
            task_id="mysql_to_s3_nps",
            query=select_sql,
            s3_bucket="{{params.s3_bucket}}",
            s3_key="{{params.s3_key}}",
            mysql_conn_id="mysql_conn_id",
            aws_conn_id="aws_conn_id",
            verify=False,
            dag=dag
            )

    s3_to_redshift_nps = S3ToRedshiftOperator(
            task_id="s3_to_redshift_nps",
            s3_bucket="{{params.s3_bucket}}",
            s3_key="{{params.s3_key}}",
            schema="{{params.schema}}",
            table="{{params.table}}",
            copy_options=["csv"],
            redshift_conn_id="redshift_conn_id",
            dag=dag
            )

s3_folder_cleanup >> mysql_to_s3_nps >> s3_to_redshift_nps

"""
-- mariadb
CREATE DATABASE prod;

CREATE TABLE prod.nps (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    created_at timestamp DEFAULT now(),
    score smallint     
);
"""

"""
--REDSHIFT
CREATE SCHEMA prod

CREATE TABLE prod.nps (
    id INT,
    created_at timestamp,
    score smallint     
);
"""