from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
from kafka import KafkaConsumer


class KafkaClient():
    def __init__(self, id: str, servers: list) -> None:
        try:
            self.kafka_servers = servers
            self.id = id
            self.consumer, self.producer = None, None
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=servers,
                client_id=self.id
            )

        except Exception as e:
            print(e)
            print('failed')

    def get_topics(self) -> list:
        try:
            return self.admin_client.list_topics()
        except Exception as e:
            print(e)

    def create_topic_type(self, name: str, partitions: int = 1, replication_factor: int = 1) -> NewTopic:
        return NewTopic(name=name, num_partitions=partitions, replication_factor=replication_factor)

    def create_topics(self, topics: list) -> None:
        try:
            result = self.admin_client.create_topics(new_topics=topics)
            print(result)
        except Exception as e:
            print(e)

    def delete_topics(self, topic_names: list) -> None:
        try:
            self.admin_client.delete_topics(topics=topic_names)
        except Exception as e:
            print(e)

    def create_producer(self, key_serializer=None, value_serializer=None, acks: int = 1, retries: int = 3, max_in_flight_requests_per_connection: int = 5):
        try:
            self.producer = KafkaProducer(
                client_id=self.id,
                bootstrap_servers=self.kafka_servers,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
                acks=acks,
                retries=retries)
        except Exception as e:
            print(e)

    def send_data(self, topic_name: str, data_list: list, func=lambda x: x):
        if(self.producer != None):
            try:
                for data in data_list:
                    pass_data = func(data)
                    self.producer.send(topic_name, value=pass_data)

            except Exception as e:
                print(e)
        else:
            print('Define a Producer First Using Create Consumer Method')

    def create_consumer(self, name: str, group_id: str, auto_commit: bool = True, offset: str = 'earliest', key_deserializer=None, value_deserializer=None):
        try:
            self.consumer = KafkaConsumer(
                name=name,
                client_id=self.id,
                bootstrap_servers=self.kafka_servers,
                auto_offset_reset=offset,
                enable_auto_commit=auto_commit,
                group_id=group_id,
                key_deserializer=key_deserializer,
                value_deserializer=value_deserializer
            )

        except Exception as e:
            print(e)

    def get_data(self, amount: int = 100):
        if(self.consumer != None):
            return_data = []
            try:
                for index, message in enumerate(self.consumer):
                    while index <= amount:
                        message = message.value
                        return_data.append(f'{index}-{message}')

                return return_data

            except Exception as e:
                print(e)
        else:
            print('Define a Consumer First Using Create Consumer Method')


if __name__ == "__main__":
    from json import dumps, loads

    kf_client = KafkaClient(
        'milkyb',
        [
            'b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092',
            'b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092'
        ]
    )

    print(kf_client.get_topics())

    kf_client.create_producer(
        value_serializer=lambda x: dumps(x).encode('utf-8'))

    kf_client.create_consumer(
        name='Reiten-Text-Corpus',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='text-corpus-reader',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    kf_client.send_data(topic_name='Reiten-Text-Corpus',
                        data_list=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])

    print(kf_client.get_data(10))
