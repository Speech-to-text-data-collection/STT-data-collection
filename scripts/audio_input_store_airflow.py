from airflow import DAG
from airflow.operators.python import PythonOperator

from Kafkaclient import KafkaClient
from AWSClient import AWSClient
from logger_creator import CreateLogger

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import sys

# Configuration Variables
csv_file_name = 'audio_descriptions.csv'
bucket_name = 'unprocessed-stt-audio'

# Creating Logger
logger = CreateLogger('Airflow-Audio-Input-storer', handlers=1)
logger = logger.get_default_logger()

# Instantating a KafkaClient Object
kf_client = KafkaClient(
    'audio-data-description-storer-DAG',
    [
        'localhost:9092',
        'localhost:9093',
        'localhost:9094'
    ]
)

# Creating a Consumer using for the KafkaClient
kf_client.create_consumer(
    topics='Text-Audio-input',
    offset='earliest',
    auto_commit=True,
    group_id='abebe',
    value_deserializer=kf_client.get_json_deserializer(),
    timeout=1000
)

# Creating a AWSClient for uploading(storing) the Data
aws_client = AWSClient()

# DECLARING Airflow DAG CONFIGURATION
DAG_CONFIG = {
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': ['milkybekele@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '0 0 0/6 ? * * *',
}

# Declaring DAG used functions
# Kafka Data Reader


def _consume_kafka_data():
    try:
        data = kf_client.get_data()

        logger.info(
            f'SUCCESSFULLY LOADED {len(data)} DATA VALUES FROM KAFKA\'s "Text-Audio-input" Topic')

        return data

    except Exception as e:
        logger.exception('FAILED TO READ DATA FROM KAFKA SERVERS')
        sys.exit(1)

# Dataframe constructor


def _create_csv_file(ti):
    try:
        fetched_data = ti.xcom_pull(task_ids=['reading_kafka_data'])
        df = pd.DataFrame(fetched_data)

        logger.info('SUCCESSFULLY CREATED CSV FILE FROM LOADED DATA VALUES')

        return df

    except Exception as e:
        logger.exception('FAILED TO CREATE CSV FILE FROM LOADED DATA VALUES')
        sys.exit(1)

# Dataframe S3 uploader


def _save_csv_file(ti):
    try:
        new_df = ti.xcom_pull(task_ids=['creating_datframe_from_data'])
        # check if a previous csv file exists
        if(csv_file_name in aws_client.get_file_names(bucket_name)):
            # load the previous csv file
            csv_data = aws_client.load_file_bytes(bucket_name, csv_file_name)
            csv_string = csv_data.decode('utf-8')

            df = pd.read_csv(StringIO(csv_string))

            # merge the previous and new csv files
            new_df = pd.concat([df, new_df])
            new_df = new_df.reset_index()

        # upload the csv file to the s3 bucket
        csv_buffer = StringIO()
        new_df.to_csv(csv_buffer)
        aws_client.put_file(bucket_name, csv_buffer.getvalue(), csv_file_name)

        logger.info('SUCCESSFULLY SAVED NEW DATAFRAME TO S3 BUCKET')

    except Exception as e:
        logger.exception('FAILED TO SAVE NEW DATAFRAME TO S3 BUCKET')
        sys.exit(1)


with DAG('audio-data-description-storer', catchup=False, default_args=DAG_CONFIG) as dag:
    # read data from kafka text-audio-input topic
    reading_kafka_data = [
        PythonOperator(
            task_id='reading_kafka_data',
            python_callable=_consume_kafka_data
        )
    ]

    # create csv file from the data
    creating_datframe_from_data = [
        PythonOperator(
            task_id='creating_datframe_from_data',
            python_callable=_create_csv_file
        )
    ]

    # save csv file
    saving_csv_file = [
        PythonOperator(
            task_id='saving_csv_file',
            python_callable=_save_csv_file
        )
    ]

    reading_kafka_data >> creating_datframe_from_data >> saving_csv_file
