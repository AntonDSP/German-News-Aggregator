from bson.json_util import loads
from src.news import NewsItem
from src.utils import mongodb
from src.utils import kafka_utils


class NewsReader:
    def __init__(self, config: dict):
        self.collection=config['COLLECTION']
        self.app=config['APP']
        self.db=config['DB']

    def read_news(self):
        if self.app == 'mongo':
           mongodb_news_stream = mongodb.mongo_collection(self.collection)
           for item in mongodb_news_stream:
               yield NewsItem(item)
        elif self.app== 'kafka':
            kafka_news_stream = kafka_utils.connectConsumer(self.collection)
            for msg in kafka_news_stream:
                item=loads(msg.value)
                yield NewsItem(item)

class NewsWriter:
    def __init__(self, config):
        self.app=config['APP']
        self.db=config['DB']
        self.collection=config['COLLECTION']
        if self.app== 'mongo':
            todo=1
        elif self.app== 'kafka':
            self.writer = kafka_utils.connectProducer()

    def write_news(self, news_item):
        if self.app== 'mongo':
            todo=1
        elif self.app== 'kafka':
            writer = kafka_utils.connectProducer()
            kafka_utils.publish_message(writer, self.collection, news_item.content['url'], news_item.content)


