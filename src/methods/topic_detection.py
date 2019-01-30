# Provide methods for topic detection
# Doesn't use the configuration file
import json
import os
from gensim import models, corpora
from src.utils import data_connector
from src.methods import preprocessing

class TopicDetector:
    def __init__(self, model_name, corpus_vector_form, model_path, num_topics):
        self.model_name=model_name
        self.corpus_vector_form=corpus_vector_form
        if self.model_name=='lda':
            if os.path.isfile(model_path):
                self.model = models.LdaModel.load(model_path, mmap='r')
            else:
                self.model=models.LdaModel(self.corpus_vector_form, num_topics=num_topics)
        else:
            print('This model is not implemented')

    def get_topics(self, text_as_vector, max_topics=10):
        topic_dist_vector=self.model[text_as_vector]
        most_relevant_topic_id=max(topic_dist_vector, key=lambda item: item[1])[0]
        return self.model.get_topic_terms(topicid=most_relevant_topic_id, topn=max_topics)


if __name__ == '__main__':

    print("Run...")

    flow_config_path = 'flow_config.json'
    with open(flow_config_path, 'r') as f:
        flow_config = json.load(f)

    news_aggregator_file_name=flow_config['NEWS_AGGREGATOR_CONFIG']+".json"
    news_aggregator_config_path= news_aggregator_file_name
    with open(news_aggregator_config_path, 'r') as f:
        news_aggregator_config = json.load(f)

    model_path = os.path.join('models', news_aggregator_config['TDT']['MODEL_NAME'])
    if os.path.isfile(model_path):
        topic_detection_model = models.LdaModel.load(model_path, mmap='r')
    else:
        news = data_connector.NewsReader(news_aggregator_config['CORPORA']).read_news()
        news_cleaned = (preprocessing.clean_and_tokenize(news_item.content['text'],
                                                         remove_stopwords_model=news_aggregator_config['PREPROCESSING']['REMOVE_STOPWORDS'],
                                                         stemming_model=news_aggregator_config['PREPROCESSING']['STEMMING']) for news_item in news)

        dictionary_name = news_aggregator_config['CORPORA']['DB']+'.dict'
        dictionary_path = os.path.join('models',dictionary_name)
        dictionary=corpora.Dictionary.load(dictionary_path)
        news_vector_rep = (dictionary.doc2bow(item) for item in news_cleaned)
        topic_detection_model = create_model(model_param=news_aggregator_config['TDT']['MODEL_PARAM'], corpus=news_vector_rep)

    print(topic_detection_model.show_topics(num_topics=10))

    print("Finished")