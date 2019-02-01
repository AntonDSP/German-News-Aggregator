import json
import os
import time

from src.methods import ner_extraction, preprocessing, aclust, text_representation
from src.utils import data_connector
import kafka
from bson.json_util import loads
from time import sleep

ROOT_PATH=os.path.abspath('./')
CONFIG_PATH = os.path.join(ROOT_PATH, 'news_aggregator_config.json')
with open(CONFIG_PATH, 'r') as f:
    default_config = json.load(f)



if __name__ == '__main__':
    kafka_consumer=kafka.KafkaConsumer('publications', auto_offset_reset='earliest', client_id='german_news_aggregator',
                  bootstrap_servers=['localhost:9092'], api_version=(0, 10), \
                  consumer_timeout_ms=1000, group_id='my-group-test3', enable_auto_commit=True)
    while (True):
        for msg in kafka_consumer:
            msg=loads(msg.value)
            print(msg['published'])
        sleep(10)
        print('looping')
    print("Finished")