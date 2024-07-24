import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging
import os
import requests
import json
from kafka import KafkaProducer
from airflow.models.connection import Connection
from airflow.decorators import task
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow import settings


POSTGRES_HOST=os.environ.get("POSTGRES_HOST")
POSTGRES_USER=os.environ.get("POSTGRES_USER")
POSTGRES_DB=os.environ.get("POSTGRES_DB")
POSTGRES_PASSWORD=os.environ.get("POSTGRES_PASSWORD")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)
logger = logging.getLogger("crypto_data_stream")

def get_data():
    try:
        api_key = os.environ.get(
            "COINMARKETCAP_API_KEY", "d3789fb8-057e-4101-b654-ca35ab75ffb7"
        )
    except:
        logger.error(f"API key couldn't be obtained from the os.")
    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
    headers = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": api_key}
    parameters = {"symbol": "BTC", "convert": "USD"}
    res = requests.get(url, headers=headers, params=parameters)
    data = json.loads(res.text)
    return data

def extract_data(data):
    price_data = data["data"]["BTC"][0]["quote"]["USD"]
    extracted_data = {
        "timestamp": data["status"]["timestamp"],
        "name": data["data"]["BTC"][0]["name"],
        "price": price_data["price"],
        "volume_24h": price_data["volume_24h"],
        "percentage_change_24h": price_data["volume_change_24h"],
    }
    print(extracted_data)
    return extracted_data



def streaming_data():
    data=get_data()
    extracted_data=extract_data(data)
    producer=KafkaProducer(bootstrap_servers=["kafka:9092"])
    producer.send("Topic_bitcoin_price",json.dumps(extracted_data).encode("utf-8"))

with DAG(
    dag_id="kafka_streaming_data_from_API",
    start_date=datetime.datetime(2024,7,23),
    schedule_interval='@daily',
    catchup=False,
) as dags:
    
    conn = Connection(
        conn_id='bitcoin_new',
        conn_type='postgres',
        host=POSTGRES_HOST,
        login=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=5432,   
    )
    
    
    @task
    def create_connection():
        try:
            BaseHook.get_connection(conn.conn_id)
        except AirflowNotFoundException:
            session=settings.Session()
            session.add(conn)
            session.commit()
            
    
    streaming_task = PythonOperator(
        task_id="streaming_task", 
        python_callable=streaming_data
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table", 
        sql="create_table.sql", 
        database=POSTGRES_DB,
        conn_id=conn.conn_id, 
    )
    
    create_connection() >> streaming_task >> create_table
    



