from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from KafkaClient import KafkaClient
from AWSClient import AWSClient
from logger_creator import CreateLogger

from datetime import datetime, timedelta
from random import randint
from json import loads
import sys

# Configuration Variables
bucket_name = 'unprocessed-stt'
kafka_servers = [
    'localhost:9092',
    'localhost:9093',
    'localhost:9094'
]
# Creating Logger
logger = CreateLogger('Airflow-Audio-Input-storer', handlers=1)
logger = logger.get_default_logger()

# Instantating a KafkaClient Object
kf_client = KafkaClient(
    'audio-data-description-storer-DAG',
    kafka_servers
)

# Creating a Produce using for the KafkaClient
kf_client.create_producer(value_serializer=kf_client.get_json_serializer())

# Creating a AWSClient for uploading(storing) the Data
aws_client = AWSClient()

# DECLARING Airflow DAG CONFIGURATION
DAG_CONFIG = {
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['milkybekele@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '0 0/1 0 ? * * *',
}

# Random int generator with equal probabilities for all numbers


def generate(end: int):
    end = int(end/2)
    x = randint(0, end)
    y = randint(0, end)

    return 2*x - (y & 1) + 1

# Declaring DAG used functions
# AWS Data Reader and validator


def _read_aws_data(ti):
    file_names = aws_client.get_file_names(bucket_name)
    file_names = [
        file_name for file_name in file_names if file_name.endswith('.json')]
    if(len(file_names) > 0):
        ti.xcom_push(key='file_names', value=file_names)
        return True

    print('No Valid Data Available, Exiting')
    return False

# Random data selector and reader


def _get_and_send_corpus_data(ti):
    file_names = ti.xcom_pull(key='file_names', task_ids=[
                              'reading_aws_data'])[0]

    rand_file_index = generate(len(file_names))

    data = aws_client.load_file_bytes(bucket_name, file_names[rand_file_index])
    data = loads(data)

    send_data = []
    for id, text in data.items():
        send_data.append({'id': id, 'text': text})

    kf_client.send_data(topic_name='Text-Corpus', data_list=send_data)


# DAG
with DAG('text_corpus_producer', catchup=False, default_args=DAG_CONFIG) as dag:
    # read data from aws bucket and validate if data exists
    reading_aws_data = ShortCircuitOperator(
        task_id='reading_aws_data',
        python_callable=_read_aws_data,
    )

    # randomly select data file, read the json value and send to kafka servers
    loading_and_sending_corpus_data = PythonOperator(
        task_id='loading_and_sending_corpus_data',
        python_callable=_get_and_send_corpus_data,
        do_xcom_push=False
    )

    reading_aws_data >> loading_and_sending_corpus_data
