from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import logging


def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id = "redshift_conn_id")
    return hook.get_conn().cursor()


def exec_sql(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    select_sql = context["params"]["sql"]

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_connection()
    sql = f"""
        DROP TABLE IF EXISTS {schema}.temp_{table};
        CREATE TABLE {schema}.temp_{table} AS 
        """
    sql += select_sql
    
    cur.execute(sql)
    cur.execute(f"SELECT COUNT(1) FROM {schema}.temp_{table}")
    count = cur.fetchone()[0]

    if not count:
        raise ValueError(f"{schema}.{table} doesn't have any record")

    try:
        sql = f"""
            DROP TABLE IF EXISTS {schema}.{table};
            ALTER TABLE {schema}.temp_{table} RENAME to {table};
        """
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error("Failed to execute SQL Statement. Completed ROLLBACK")
        raise AirflowException("")


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


with DAG(
    "build_summary",
    start_date = datetime(2022, 5, 7),
    schedule_interval = "@once", 
    catchup = False,
    default_args = default_args
    ) as dag:

    exec_sql = PythonOperator(
        task_id="exec_sql",
        python_callable=exec_sql,
        params={
            "schema": "prod",
            "table": "nps_summary",
            "sql": """
                    SELECT 
                        DISTINCT(id)
                        ,FIRST_VALUE(score) OVER(
                            PARTITION BY id 
                            ORDER BY created_at 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                        ,LAST_VALUE(score) OVER(
                            PARTITION BY id 
                            ORDER BY created_at 
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    FROM prod.nps;
                    """},
        provide_context=True,
        dag=dag)