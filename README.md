# STT-data-collection
The purpose of this challenge is to build a data engineering pipeline that allows recording millions of Amharic speakers reading digital texts in-app and web platforms.

# Table of content
- [STT-data-collection](#stt-data-collection)
- [Table of content](#table-of-content)
  - [Introduction](#introduction)
  - [Pipeline](#pipeline)
    - [Pipeline Description](#pipeline-description)
  - [Installation](#installation)
  - [Folders](#folders)
  - [Technolologies](#technolologies)
  - [Contributers](#contributers)
  - [License](#license)


## Introduction
There are many text corpuses for Amharic and Swahili. Our client 10 academy wants to gather vast amount of quality audio data from diffrent applications by displaying text corpus and record users reading the displayed text. And build robust, large scale, fault tolerant, highly available Kafka cluster that can be used to post a sentence and receive an audio file.

## Pipeline
> The diagram below depicts the proposed pipeline with all the tools that will be used for implementation

![alt text](https://github.com/Speech-to-text-data-collection/STT-data-collection/blob/main/data/Flowchart_Diagram.jpg "Data Handling and Processing Pipeline")
### Pipeline Description
>   The pipeline consists of five major parts. Starting from the application part, it will be in charge of handling the interaction with the user. The user can request texts to dictate (audio recording of the text) and send their dictation using the application. These actions create a JavaScript-based request and send it to a python server. This server will handle the text request by acting as a consumer for the 'Text-Corpus' topic in the Kafka Cluster. It will load the entire data when requesting for the first time and provides single data entities to the application until it finishes the fetched data. It handles the dictated audio input first by uploading the audio data to an S3 bucket and then passes references of the audio data to the Kafka Cluster, acting as a producer for the 'Text-Audio-Input' topic.
>
>   The Kafka Cluster will be responsible for handling the messaging system. It will receive and transmit data from the application side to the data processing and storage side. The data received at the 'Text-Audio-Input' topic will be read continuously by a consumer scheduled every two minutes by the Airflow component and stored in the 'Unprocessed S3 Bucket'. The Airflow will also orchestrate two other tasks. A task that runs every six hours reading the audio files and descriptive data stored in the 'Unprocessed S3 Bucket' and then passes it to the spark component. And another task which runs every five minutes fetching unprocessed text-corpus from the 'Unprocessed S3 Bucket' and sends it to Kafka's 'Text-Corpus' topic as a producer.
>
>   The spark component will be responsible for retrieving the audio files and applying audio cleaning transformations like trimming, normalization, and more. It then validates the audio with its predefined text using a model, and if it passes the validation similarity metric, the audio gets saved on the 'Processed S3 Bucket'. If it fails, the audio gets omitted. This process finalizes how the entire pipeline works.


## Installation
- kafka installation 
- airflow installation
- spark installation

## Folders
- **data** : 
- **notebooks** : 
- **scripts** : 
- **tests** : 

## Technolologies
- [Apache Kafka](https://kafka.apache.org/) : 
- [Apache Airflow](https://airflow.apache.org/) : 
- [Apache Spark](https://spark.apache.org/) : 

## Contributers
- [Milky Bekele](https://github.com/DePacifier)
- [Bethelhem Sisay]()
- [Natnael Sisay](https://github.com/NatnaelSisay)
- [Chimdessa_Tesfaye]()
- [Harriet_Sibitenda]()
- [Luel]()
- [Michael Tekle]()
- [Mizan_Abaynew]()

## License
[MIT](https://github.com/DePacifier/3DEP-Farm/blob/main/LICENSE)