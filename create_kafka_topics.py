from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaClient
import os

KAFKA_BROKER = 'IP:9092'
KAFKA_USERNAME = 'user'
KAFKA_PASSWORD = 'pass'
TOPICS_FILE = './list_topic.txt'
PARTITIONS = 3
REPLICATION_FACTOR = 3
RETENTION_MS = 60 * 60 * 1000  
MAX_SEGMENT_SIZE = 1073741824  

def create_topics(admin_client, topics):
    new_topics = []
    for topic in topics:
        new_topic = NewTopic(
            name=topic,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
            topic_configs={
                'retention.ms': str(RETENTION_MS),
                'segment.bytes': str(MAX_SEGMENT_SIZE)
            }
        )
        new_topics.append(new_topic)
    admin_client.create_topics(new_topics=new_topics, validate_only=False)

def main():
    
    if not os.path.exists(TOPICS_FILE):
        print(f"File {TOPICS_FILE} not exist.")
        return

    with open(TOPICS_FILE, 'r') as file:
        topics = [line.strip() for line in file if line.strip()]

    if not topics:
        print("File empty.")
        return

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id='ja',
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
    )

    try:
        create_topics(admin_client, topics)
        print("Tópicos creados con éxito.")
    except Exception as e:
        print(f"Error al crear los tópicos: {e}")
    finally:
        admin_client.close()

if __name__ == '__main__':
    main()
