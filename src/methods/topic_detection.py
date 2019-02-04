# Provide methods for topic detection
# Doesn't use the configuration file
import json
import os
from gensim import models, corpora
from src.utils import data_connector
from src.methods import preprocessing, text_representation

class TopicDetector:
    def __init__(self, model_name, models_path, method, train_corpus, num_topics):
        self.model_name=model_name
        self.max_num_topics=num_topics
        self.models_path=models_path
        self.train_corpus=train_corpus
        self.method=method
        file_path=os.path.join(self.models_path, self.model_name)
        if self.method=='lda':
            if os.path.isfile(file_path):
                self.topic_detection_model = models.LdaModel.load(file_path, mmap='r')
            else:
                self.topic_detection_model=models.LdaModel(self.train_corpus, num_topics=self.num_topics)
                self.topic_detection_model.save(file_path)
        else:
            print('This model is not implemented')

    def get_topics(self, text_as_vector, show_top_n_topics):
        self.topic_detection_model.update(text_as_vector)
        topic_dist_vector=self.topic_detection_model[text_as_vector]
        most_relevant_topic_id=max(topic_dist_vector, key=lambda item: item[1])[0]
        return self.topic_detection_model.get_topic_terms(topicid=most_relevant_topic_id, topn=show_top_n_topics)


if __name__ == '__main__':

    print("Run...")

    flow_config_path = 'flow_config.json'
    with open(flow_config_path, 'r') as f:
        flow_config = json.load(f)

    news_aggregator_file_name=flow_config['NEWS_AGGREGATOR_CONFIG']+".json"
    news_aggregator_config_path= news_aggregator_file_name
    with open(news_aggregator_config_path, 'r') as f:
        na_config = json.load(f)

    publications=data_connector.NewsReader(app='mongo', db='german_news', collection='publications').read_news()

    preprocessor=preprocessing.Preprocessor(remove_stopwords_model='nltk', stemming_model=None, lemmatization_model=None)
    publications_tokenized = [preprocessor.clean_and_tokenize(publication.content['text']) for publication in publications]

    text2vector_model=text_representation.TextToVector(word_representation='word2vec', models_path='models', model_name='cc.de.300.vec')

    corpus_as_vector=[text2vector_model.transform(publication_tokenized) for publication_tokenized in publications_tokenized]

    topic_detection_model=TopicDetector(model_name='lda_word2vec', models_path='models', method='lda', train_corpus=corpus_as_vector, num_topics=100)

    for publication_vec in corpus_as_vector:
        topics=topic_detection_model.get_topics(publication_vec, show_top_n_topics=3)
        print(topics)

    print("Finished")