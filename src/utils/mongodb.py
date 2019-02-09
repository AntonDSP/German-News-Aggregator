import json
from pymongo import MongoClient
from src.utils import kafka_utils
import os

# Import configurations
root_path=os.getcwd()
path_project_config=os.path.join(root_path, 'project_config.json')
with open(path_project_config, 'r') as f:
    config = json.load(f)

def connect2db(db):
    uri='mongodb://'+config['MONGO']['USER']+':'+config['MONGO']['PASSWORD']+'@' \
        +config['MONGO']['HOST']+':'+str(config['MONGO']['PORT'])+'/'+config['MONGO']['DATABASE']
    client=MongoClient(uri)
    return client.get_database(name=db)

def mongo_collection(collection_name, db):
    collection = db.get_collection(name=collection_name)
    return collection.find({}, no_cursor_timeout=True)

def publish_to_kafka(collection, kafka_topic_name):
    producer= kafka_utils.connectProducer()
    for news_item in collection:
        kafka_utils.publish_message(producer, kafka_topic_name, news_item['url'], news_item)

def write_news_item(news_item, db, collection_name):
    if '_id' in news_item:
        del news_item['_id']
    db[collection_name].insert_one(dict(news_item))
    print('Message published successfully in mongo: ' + str(news_item['publication_id']))

def write_cluster(news_cluster, db, collection_name):
    news_ids=[n.content['publication_id'] for n in news_cluster]
    news_of_cluster={'news_ids':news_ids}
    db[collection_name].insert_one(dict(news_of_cluster))
    print('Cluster published successfully: ')



if __name__ == '__main__':
    print('Running scraped news publisher..')
    mongo_db=connect2db('german_news')
    news_collection=mongo_collection(collection_name='publications', db=mongo_db)
    publish_to_kafka(news_collection,kafka_topic_name='crawled_publications')
    print('Publishing finished')