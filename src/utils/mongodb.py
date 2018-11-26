import json
from pymongo import MongoClient
from src.utils import kafka_utils

# Import configurations
with open('../project_config.json', 'r') as f:
    config = json.load(f)

connection=MongoClient(config['MONGO']['HOST'], config['MONGO']['PORT'])
db=connection.get_database(name=config['MONGO']['DATABASE'])

def mongo_collection(collection_name):
    collection = db.get_collection(name=collection_name)
    pipe = [{'$sort': {'published': -1}}]
    return collection.aggregate(pipe, allowDiskUse=True)

def publish_to_kafka(collection, kafka_topic_name):
    producer= kafka_utils.connectProducer()
    for news_item in collection:
        kafka_utils.publish_message(producer, kafka_topic_name, news_item['url'], news_item)

if __name__ == '__main__':
    print('Running scraped news publisher..')
    news_collection=mongo_collection(collection_name='german_news')
    publish_to_kafka(news_collection,kafka_topic_name='scraped_news')
    print('Publishing finished')