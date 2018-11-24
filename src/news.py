# Model of news item. News item represents news published in web
import json

from gensim.matutils import cossim
from models.src.topic_detection import detect_topic
from models.src.utils import data_connector
from preprocessing import preprocess_text

from src.ner_extraction import named_entity_extraction

with open('aggregator_config.json', 'r') as f:
    default_config = json.load(f)

class NewsItem:
    features={}
    def __init__(self, content):
        self.content=content

    def distance(self, other):
        return 1-cossim(self.features['topics'], other.features['topics'])

    def is_correlated(self, other):
        return cossim(self.features['topics'], other.features['topics'])>0.5

    def concatenate_content(self, content_parts):
        joint_content=""
        for part in content_parts:
            joint_content=joint_content+self.content[part]
        return joint_content

# News items object factory function
def create_news_item(content: dict, config: dict)->NewsItem:
    config=default_config
    # Initialization
    news_item=NewsItem(content)
    # Preprocessing
    input=config['PREPROCESSING']['INPUT']
    for news_part in input:
        news_part_as_tokens=news_part+'_as_tokens'
        news_item.content[news_part_as_tokens]=preprocess_text(news_item.content[news_part], config=config['PREPROCESSING'])
    # NER
    named_entities={}
    input=config['NER']['INPUT']
    for input_item in input:
        ne=named_entity_extraction(news_item.content[input_item], config['NER'])
        if ne is not None:
            named_entities.update(ne)
    news_item.content['ner']=named_entities
    # Topic detection
    news_item.features['topics']=detect_topic(news_item.content['text_as_tokens'], config['TDT'])
    # Feature generation which can be used for clustering

    return news_item


if __name__ == '__main__':
    print("Run...")
    config=default_config
    #create_doc2bow_lda_model(config)
    news_stream = data_connector.NewsReader(config['SOURCE']).read_news()
    for news_item in news_stream:
        news_item=create_news_item(news_item.content, config)
        print(news_item.content['url'])
        print(news_item.features['topics'])

    print("Finished")