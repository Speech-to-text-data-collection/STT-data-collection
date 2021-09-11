from fastapi import FastAPI, File, UploadFile
from KafkaClient import KafkaClient

# Create Consumer to read Text Corpus Values from the Kafka Cluster
## Instantiating a KafkaClient Object
server_kafka_consumer = KafkaClient(
    'python-fast-api-server',
    [
        'localhost:9092',
        'localhost:9093',
        'localhost:9094'
    ]
    )
## Creating a consumer using the kafkaclient
server_kafka_consumer.create_consumer(
    topics='Text-Corpus',
    offset='earliest',
    auto_commit=True,
    group_id='abebe',
    value_deserializer=server_kafka_consumer.get_json_deserializer(),
    timeout=1000
    )

## Function which reads using the created consumer
async def call_consumer_get_data():
    return server_kafka_consumer.get_data()


# Create Producer to Upload audio file to S3 Bucket and pass the link to kafka servers





# suppose_values = [
#     {'id': 1, 'text': 'I am working on the server'},
#     {'id': 2, 'text': 'I am flabergased'},
#     {'id': 3, 'text': 'I am milky'},
#     {'id': 4, 'text': 'I am laughing'},
#     {'id': 5, 'text': 'I am joking'},
#     {'id': 6, 'text': 'I am playing'},
#     {'id': 7, 'text': 'I am shoveling'}
# ]


# async def getvalues():
#     return suppose_values

# List for holding fetched text values
fetched_data = []


app = FastAPI()


@app.get('/fetch-text')
async def fetch_text():
    # Check if we have previously fetched data with more than 20 data objects
    if(len(fetched_data) <= 50):
        print('Items lower than 5, fetching data')
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
        # filename give you myimage.jpg
        # content_type gives image/jpeg
        # file gives us a spooledtemporaryfile(file like object)

        # Upload file to S3 Bucket

        # Send Data to Kafka the text id and reference link from S3 using the producer

        # return Success or Failure
        # return {'filename': file.filename, 'content_type': file.content_type}
        return {'filename': file.filename, 'content_type': file.content_type, 'status': 'success'}

    except Exception as e:
        print(e)
        return {'status': 'failed', 'error-message': str(e)}
