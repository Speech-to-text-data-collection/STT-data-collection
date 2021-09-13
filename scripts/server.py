from fastapi import FastAPI, File, UploadFile
from KafkaClient import KafkaClient
from AWSClient import AWSClient
from datetime import datetime, timezone

# Create an AWSClient to upload files to S3 Bucket
server_aws_client = AWSClient()

# Create Consumer to read Text Corpus Values from the Kafka Cluster
# Instantiating a KafkaClient Object
server_kafka_consumer = KafkaClient(
    'python-fast-api-server',
    [
        'localhost:9092',
        'localhost:9093',
        'localhost:9094'
    ]
)
# Creating a consumer using the kafkaclient with a json deserializer
server_kafka_consumer.create_consumer(
    topics='Text-Corpus',
    offset='earliest',
    auto_commit=True,
    group_id='abebe',
    value_deserializer=server_kafka_consumer.get_json_deserializer(),
    timeout=1000
)

# Function which reads using the created consumer


async def call_consumer_get_data():
    return server_kafka_consumer.get_data()

# Creating a producer using the kafkaclient with a json serializer
server_kafka_consumer.create_producer(
    value_serializer=server_kafka_consumer.get_json_serializer())

# Function called when file is recieved by server


async def validate_file(file: UploadFile) -> bool:
    try:
        # Check file is an audio type
        assert file.content_type.startswith('audio')

        return True

    except AssertionError:
        return False

    except Exception as e:
        print('FAILED TO VALIDATE FILE')
        print(e)
        # return False


async def upload_file(file: UploadFile) -> bool:
    try:
        server_aws_client.upload_file_object(
            file.file, 'unprocessed-stt-audio', file.filename)

        return True

    except Exception as e:
        print(e)
        return False


def generate_file_data(file: UploadFile):
    file_link = server_aws_client.get_file_link(
        'unprocessed-stt-audio', file.filename)

    text_id, audio_id = file.filename.split('_')

    upload_time = datetime.now(timezone.utc)
    date = upload_time.strftime("%m/%d/%y")
    time = upload_time.strftime("%H:%M:%S")

    data = {'file_name': file.filename, 'content_type': file.content_type, 'text_id': text_id, 'audio_id': audio_id,
            'link': file_link, 'upload_date (UTC)': date, 'upload_time (UTC)': time}

    return data

# Using  Upload audio file to S3 Bucket and pass the link to kafka servers


async def send_detail_to_kafka(file: UploadFile):
    try:
        file_data = generate_file_data(file)

        server_kafka_consumer.send_data('Text-Audio-input', [file_data])

    except Exception as e:
        print(e)


# List for holding fetched text values
fetched_data = []


app = FastAPI()


@app.get('/fetch-text')
async def fetch_text():
    # Check if we have previously fetched data with more than 20 data objects
    if(len(fetched_data) <= 50):
        print('Items lower than 50, fetching data')
        # Fetch Data
        data = await call_consumer_get_data()
        fetched_data.extend(data)

    # Pop a Data Value from fetched_data list
    return_data = fetched_data.pop(0)
    print("\t-> Returning:", return_data)

    # Return Data Value (id and text)
    return return_data


@app.post('/upload-audio')
async def handle_upload_audio(file: UploadFile = File(...)):
    try:
        # Validate if file is audio
        if(await validate_file(file)):
            # Upload file to S3 Bucket
            assert await upload_file(file)
            # Send Data to Kafka the text id and reference link from S3 using the producer
            await send_detail_to_kafka(file)

        # return Success or Failure
        return {'filename': file.filename, 'content_type': file.content_type, 'status': 'success', 'detail': 'File Upload Successful'}

    except AssertionError:
        return {'filename': file.filename, 'content_type': file.content_type, 'status': 'failure', 'detail': 'File Upload Failed'}

    except Exception as e:
        print(e)
        return {'status': 'failed', 'error-message': str(e)}
