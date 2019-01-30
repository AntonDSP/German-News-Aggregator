from bson.json_util import loads
from src.news import NewsItem
from src.utils import mongodb
from src.utils import kafka_utils


class NewsReader:
    def __init__(self, app, db, collection):
        self.collection=collection
        self.app=app
        self.db=db

    def read_news(self):
        if self.app == 'mongo':
           mongo_client=mongodb.connect2db(db=self.db )
           mongodb_news_stream = mongodb.mongo_collection(collection_name=self.collection, db=mongo_client)
           for item in mongodb_news_stream:
               yield NewsItem(item)
        elif self.app== 'kafka':
            kafka_news_stream = kafka_utils.connectConsumer(self.collection)
            for msg in kafka_news_stream:
                item=loads(msg.value)
                yield NewsItem(item)

class NewsWriter:
    def __init__(self, app, db, collection, cluster_collection):
        self.app=app
        self.db=db
        self.collection=collection
        self.cluster_collection=cluster_collection
        if self.app== 'mongo':
            self.writer=mongodb.connect2db(self.db)
        elif self.app== 'kafka':
            self.writer = kafka_utils.connectProducer()

    def write_news(self, news_item):
        if self.app== 'mongo':
            mongodb.write_news_item(news_item.content, self.writer, self.collection)
        elif self.app== 'kafka':
            kafka_utils.publish_message(self.writer, self.collection, news_item.content['url'], news_item.content)

    def write_cluster(self, news_cluster):
        if self.app== 'mongo':
            mongodb.write_cluster(news_cluster,self.writer, self.cluster_collection)
        elif self.app== 'kafka':
            writer = kafka_utils.connectProducer()
            kafka_utils.publish_cluster(writer, self.collection, news_cluster.id, news_cluster)