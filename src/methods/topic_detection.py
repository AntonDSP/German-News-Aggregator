# Provide methods for topic detection
# Doesn't use the configuration file
import json
import os
from gensim import models, corpora
from src.utils import data_connector
from src.methods import preprocessing


def create_model(model_param: dict, corpus):
    if model_param['MODEL']=='lda':
        return models.LdaModel(corpus, num_topics=model_param['NUM_OF_TOPICS'])
    else:
        print("No model creation logic found")
        return None


if __name__ == '__main__':

    with open('aggregator_config.json', 'r') as f:
        default_config = json.load(f)

    print("Run...")
    config=default_config

    model_path = os.path.join('models', config['TDT']['MODEL_NAME'])
    if os.path.isfile(model_path):
        topic_detection_model = models.LdaModel.load(model_path, mmap='r')
    else:
        news = data_connector.NewsReader(config['CORPORA']).read_news()
        news_cleaned = (preprocessing.clean_and_tokenize(news_item.content['text'],
                                                         remove_stopwords_model=config['PRERPOCESSING']['REMOVE_STOPDWORS'],
                                                         stemming_model=config['PREPROCESSING']['STEMMING']) for news_item in news)

        dictionary_name = config['CORPORA']['COLLECTION']+'.dict'
        dictionary_path = os.path.join('models',dictionary_name)
        dictionary=corpora.Dictionary.load(dictionary_path)
        news_vector_rep = (dictionary.doc2bow(item) for item in news_cleaned)
        topic_detection_model = create_model(model_param=config['TDT']['MODEL_PARAM'], corpus=news_vector_rep)

    print(topic_detection_model.show_topics(num_topics=10))

    print("Finished")