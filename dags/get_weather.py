from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import logging
import psycopg2

def extract(**context):
    # 기준: 서울 시청 
    seoul_lat = 37.541
    seoul_lon = 126.986
    api_key = context["params"]["api_key"]
    logging.info(api_key)

    api_url = f"https://api.openweathermap.org/data/2.5/onecall?lat={seoul_lat}&lon={seoul_lon}&appid={api_key}"

    # task_instance = context["task_instance"]
    execution_date = context["execution_date"]

    logging.info(execution_date)
    res = requests.get(api_url)

    return res.json()


def transform(**context):

    res = context["task_instance"].xcom_pull(task_ids="extract")

    daily = res["daily"]
    
    data = []
    for i in range(1, 8):
        dt = datetime.fromtimestamp(daily[i]["dt"]).strftime("%Y-%m-%d")
        temp = daily[i]["temp"]["day"] # 낮 온도
        min_temp = daily[i]["temp"]["min"] # 최저 온도
        max_temp = daily[i]["temp"]["max"] # 최고 온도
        
        data.append([dt, temp, min_temp, max_temp])

    return data

def get_connection():
    # default autocommit: False
    hook = PostgresHook(postgres_conn_id = "postgres2_weather")
    return hook.get_conn().cursor()


def load(**context):

    data = context["task_instance"].xcom_pull(task_ids="transform")
    schema = context["params"]["schema"]
    table = context["params"]["table"]
   
    logging.info(data)
    logging.info(schema)
    logging.info(table)
    cur = get_connection()
    
    try:
        # cur.execute("BEGIN;")
        # cur.execute(f"DELETE FROM {schema}.{table};")
        for d in data:
            logging.info(d[0], d[1], d[2], d[3])
            cur.execute(f"INSERT INTO {schema}.{table} VALUES ('{d[0]}', {d[1]}, {d[2]}, {d[3]});")
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
        cur.execute("ROLLBACK;")
        raise 


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    "get_weather",
    start_date=datetime(2022, 5, 7),
    schedule_interval="0 * * * *", 
    max_active_runs=3,
    catchup=False,
    default_args=default_args
    ) as dag:
    
    extract = PythonOperator(
            task_id="extract",
            python_callable=extract,
            params={
                "api_key" : Variable.get("open_weather_api_key")
            },
            provide_context=True,
            dag=dag
            )

    transform = PythonOperator(
            task_id="transform",
            python_callable=transform,
            params={},
            provide_context=True,
            dag=dag
            )

    load = PythonOperator(
            task_id="load",
            python_callable=load,
            params={
                "schema": Variable.get("weather_schema"),
                "table": Variable.get("weather_table")
                },
            provide_context=True,
            dag=dag
            )

extract >> transform >> load

"""
CREATE TABLE weather.weather_forecast (
    date date,
    temp float,
    min_temp float,
    max_temp float,
    created_date timestamp DEFAULT now()
);
"""