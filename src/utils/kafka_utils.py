from kafka import KafkaConsumer, KafkaProducer
from bson.json_util import loads, dumps
import json

# Load configurations
with open('project_config.json', 'r') as f:
    config = json.load(f)
#Build str for server connection
server = config['KAFKA']['HOST'] + ':' + str(config['KAFKA']['PORT'])

def connectConsumer(topic_name: str)-> KafkaConsumer:
    """Connect kafka consumer
    Args:
        topic_name (str): name of kafka topic to fetch messages
    Returns:
        KafkaConsumer: Kafka consumer object as stream of news items
    """
    return KafkaConsumer(topic_name, auto_offset_reset='earliest', bootstrap_servers=[server], api_version=(0, 10), consumer_timeout_ms=1000)

def connectProducer()->KafkaProducer:
    """Connect kafka producer
    Args:
        -
    Returns:
        KafkaProducer: KafkaProducer object to publish messages
    """
    return KafkaProducer(bootstrap_servers=[server], api_version=(0, 10))

def publish_message(producer: KafkaProducer, topic_name: str, key: bytes, value: bytes):
    """Publish meassage to specific kafka topic
    Args:
        producer (KafkaProducer): Kafka producer to publish message
        topic_name (str): Name of kafka topic to which the message should be published
        key (bytes): message key (used for partitioning in kafka)
        value(bytes): byte representation of the actual message or object to be published
    Returns:
        -
    """
    try:
        key_bytes = key.encode("utf-8")
        value_bytes = dumps(value).encode("utf-8")
        producer.send(topic_name, key=key_bytes, value=value_bytes)
        producer.flush()
        print('Message published successfully: ' + key)
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))