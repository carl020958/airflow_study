from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import logging
import psycopg2

def get_connection():
    # default autocommit: False
    hook = PostgresHook(postgres_conn_id = "postgres2_weather")
    return hook.get_conn().cursor()

def etl(**context):
    # [extract]
    seoul_lat = context["params"]["seoul_lat"]
    seoul_lon = context["params"]["seoul_lon"]
    api_key = context["params"]["api_key"]
    
    url = f"https://api.openweathermap.org/data/2.5/onecall?lat={seoul_lat}&lon={seoul_lon}&appid={api_key}"
    data = requests.get(url).json()

    # [transform]
    records = []
    for d in data["daily"]:
        dt = datetime.fromtimestamp(d["dt"]).strftime("%Y-%m-%d")
        records.append("('{}', {}, {}, {})".format(dt, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))

    # [load]
    """
    Primary key를 보장해주시기 위해
    1) weather_forecast로부터 데이터를 가져온 뒤 temp_weather_forecast라는 임시 테이블에 저장
    2) 새로운 데이터를 temp_weather_forecast에 추가
    3) 가장 최신의 데이터를 기준으로 temp_weather_forecast로부터 가지고와 weather_forecast에 저장
    """

    schema = context["params"]["schema"]
    table = context["params"]["table"]
    cur = get_connection()

    # 1) load data from weather_forecast table into temp_weather_forecast
    # CTAS를 사용할 경우 attribute 중 default value와 관련해 제대로 복사가 안될 수도 있어 아래의 쿼리 사용
    create_sql = f"""
    DROP TABLE IF EXISTS {schema}.temp_{table};
    CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);
    INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table};
    """
    logging.info(create_sql)
    try:
        cur.execute(create_sql)
        cur.execute("COMMIT;")
    except:
        cur.execute("ROLLBACK;")
        raise

    # 2) load data(new) into temp_weather_forecast
    insert_sql = f"INSERT INTO {schema}.temp_{table} VALUES " + ",".join(records)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except:
        cur.execute("ROLLBACK;")
        raise

    # 3) load data(clean) into weather_forecast
    alter_sql = f"""
    DELETE FROM {schema}.{table};
    INSERT INTO {schema}.{table}
    SELECT tt.date, tt.temp, tt.min_temp, tt.max_temp 
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY date ORDER BY created_date DESC) seq
        FROM {schema}.temp_{table}
    ) tt
    WHERE tt.seq = 1;
    """
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except:
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
    "get_weather_v2",
    start_date = datetime(2022, 5, 7),
    schedule_interval = "0 * * * *", 
    max_active_runs = 3,
    catchup = False,
    default_args = default_args

    ) as dag:

    etl = PythonOperator(
            task_id = "etl",
            python_callable = etl,
            params = {
                "seoul_lat": 37.541,
                "seoul_lon": 126.986,
                "api_key" : Variable.get("open_weather_api_key"),
                "schema" : Variable.get("weather_schema"),
                "table" : Variable.get("weather_table")
                },
            provide_context = True,
            dag = dag
            )

etl


"""
CREATE TABLE weather.weather_forecast (
 date date,
 temp float,
 min_temp float,
 max_temp float,
 created_date timestamp DEFAULT now()
);
"""