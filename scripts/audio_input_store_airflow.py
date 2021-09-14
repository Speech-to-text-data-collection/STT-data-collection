from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from KafkaClient import KafkaClient
from AWSClient import AWSClient
from logger_creator import CreateLogger

from datetime import datetime
import pandas as pd
from io import StringIO

# Configuration Variables
csv_file_name = 'audio_descriptions.csv'
bucket_name = 'unprocessed-stt-audio'
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

# Creating a Consumer using for the KafkaClient
kf_client.create_consumer(
    topics='Text-Audio-input',
    offset='earliest',
    auto_commit=True,
    group_id='airflow-text-audio-input-reader',
    value_deserializer=kf_client.get_json_deserializer(),
    timeout=1000
)

# Creating a AWSClient for uploading(storing) the Data
aws_client = AWSClient()

# DECLARING Airflow DAG CONFIGURATION
DAG_CONFIG = {
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['milkybekele@gmail.com'],
    'email_on_failure': True,
    'schedule_interval': '0 0 0/1 ? * * *',
}

# Declaring DAG used functions
# Kafka Data Reader


def _consume_kafka_data(ti):
    try:
        data = kf_client.get_data()

        if(len(data) > 0):
            ti.xcom_push(key='kafka_data', value=data)

            logger.info(
                f'SUCCESSFULLY LOADED {len(data)} DATA VALUES FROM KAFKA\'s "Text-Audio-input" Topic')

            return True

        return False

    except Exception as e:
        logger.exception('FAILED TO READ DATA FROM KAFKA SERVERS')
        return False

# Dataframe constructor


def _create_and_save_csv_file(ti):
    try:
        fetched_data = ti.xcom_pull(
            key='kafka_data', task_ids=['reading_kafka_data'])[0]

        df = pd.DataFrame(fetched_data)

        logger.info('SUCCESSFULLY CREATED CSV FILE FROM LOADED DATA VALUES')

        # upload the csv file to the s3 bucket
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            aws_client.put_file(
                bucket_name, csv_buffer.getvalue(), csv_file_name)

            logger.info('SUCCESSFULLY SAVED NEW DATAFRAME TO S3 BUCKET')

        except Exception as e:
            logger.exception('FAILED TO SAVE NEW DATAFRAME TO S3 BUCKET')

    except Exception as e:
        logger.exception('FAILED TO CREATE CSV FILE FROM LOADED DATA VALUES')

# Dataframe S3 uploader


# def _save_csv_file(ti):
#     try:
#         new_df = ti.xcom_pull(key='dataframe', task_ids=[
#                               'creating_datframe_from_data'])[0]
#         logger.info(f'type:{type(new_df)}, len:{len(new_df)}')
#         # check if a previous csv file exists
#         # if(csv_file_name in aws_client.get_file_names(bucket_name)):
#         #     # load the previous csv file
#         #     csv_data = aws_client.load_file_bytes(bucket_name, csv_file_name)
#         #     csv_string = csv_data.decode('utf-8')

#         #     df = pd.read_csv(StringIO(csv_string))

#         #     # merge the previous and new csv files
#         #     new_df = pd.concat([df, new_df])
#         #     new_df = new_df.reset_index()

#         # upload the csv file to the s3 bucket
#         csv_buffer = StringIO()
#         new_df.to_csv(csv_buffer)
#         aws_client.put_file(bucket_name, csv_buffer.getvalue(), csv_file_name)

#         logger.info('SUCCESSFULLY SAVED NEW DATAFRAME TO S3 BUCKET')

#     except Exception as e:
#         logger.exception('FAILED TO SAVE NEW DATAFRAME TO S3 BUCKET')


# DAG
with DAG('audio-data-description-storer', catchup=False, default_args=DAG_CONFIG) as dag:
    # read data from kafka text-audio-input topic
    reading_kafka_data = ShortCircuitOperator(
        task_id='reading_kafka_data',
        python_callable=_consume_kafka_data
    )

    # create csv file from the data
    creating_and_saving_datframe_from_data = PythonOperator(
        task_id='creating_datframe_from_data',
        python_callable=_create_and_save_csv_file
    )

    # # save csv file
    # saving_csv_file = PythonOperator(
    #     task_id='saving_csv_file',
    #     python_callable=_save_csv_file
    # )

    reading_kafka_data >> creating_and_saving_datframe_from_data
