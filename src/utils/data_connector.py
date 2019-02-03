from bson.json_util import loads
from src.news import NewsItem
from src.utils import mongodb
from src.utils import kafka_utils


class NewsReader:
    def __init__(self, app, db, collection):
        self.collection=collection
        self.app=app
        self.db=db
        if self.app == 'mongo':
           self.reader=mongodb.connect2db(db=self.db )
        elif self.app== 'kafka':
            self.reader= kafka_utils.connectConsumer(self.collection)


    def read_news(self):
        if self.app == 'mongo':
            mongo_collection=self.collection
            mongodb_news_stream = self.reader.german_news.find()
            for item in mongodb_news_stream:
               yield NewsItem(item)
        elif self.app== 'kafka':
            for msg in self.reader:
                item=loads(msg.value)
                yield NewsItem(item)

class NewsWriter:
    def __init__(self, app, db, collection):
        self.app=app
        self.db=db
        self.collection=collection
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