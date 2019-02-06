# Provide methods for topic detection
# Doesn't use the configuration file
import json
import os
from gensim import models, corpora
from src.utils import data_connector
from src.methods import preprocessing

class TopicDetector:
    def __init__(self, model_name, models_path, method, representation, num_topics, train_corpus, collection_name):
        self.model_name=model_name
        self.num_topics=num_topics
        self.models_path=models_path
        self.method=method
        self.representation=representation
        self.collection_name=collection_name

        # Load or create dictionary
        self.file_dictionary_path = os.path.join(self.models_path, self.collection_name)
        if os.path.isfile(self.file_dictionary_path):
            self.dictionary=corpora.Dictionary.load(self.file_dictionary_path,mmap='r')
        else:
            try:
                self.dictionary=corpora.Dictionary(train_corpus)
            except:
                print("No dictionary available. Please provide a train corpus")
            self.dictionary.filter_extremes(no_above=0.1)
            self.dictionary.save(self.file_dictionary_path)

        # Load or transform corpus to vectors
        self.file_corpus_path = os.path.join(self.models_path, (self.collection_name+' '+self.representation))
        if os.path.isfile(self.file_corpus_path):
            self.vec_doc2bow_corpus=corpora.MmCorpus(self.file_corpus_path)
        else:
            self.vec_doc2bow_corpus = [self.dictionary.doc2bow(doc) for doc in train_corpus]
            corpora.MmCorpus.serialize(self.file_corpus_path, self.vec_doc2bow_corpus) # Save always in doc2bow format
        if self.representation=='tfidf':
            tfidf_model=models.TfidfModel(self.vec_doc2bow_corpus)
            vec_tfidf_corpus=tfidf_model[self.vec_doc2bow_corpus]

        #Create or load model
        self.topic_model_path=os.path.join(self.models_path,self.model_name)
        if self.method=='lda':
            if os.path.isfile(self.topic_model_path):
                self.topic_detection_model = models.LdaModel.load(self.topic_model_path)
            else:
                if self.representation=='doc2bow':
                    self.topic_detection_model=models.LdaModel(corpus=self.vec_doc2bow_corpus, num_topics=self.num_topics, id2word=self.dictionary)
                elif self.representation=='tfidf':
                    self.topic_detection_model=models.LdaModel(corpus=vec_tfidf_corpus, num_topics=self.num_topics, id2word=self.dictionary)
        else:
            self.topic_detection_model=None
            print('This model is not implemented')
        self.topic_detection_model.save(self.topic_model_path)


    def get_topics(self, tokenized_text, show_top_n_topics):
        # Update dictionary
        documents=[].append(tokenized_text)
        self.dictionary.add_documents(documents)
        self.dictionary.save(self.file_dictionary_path)
        # Transform to vector
        vector=self.dictionary.doc2bow[tokenized_text]
        self.vec_doc2bow_corpus.append(vector)
        corpora.MmCorpus.serialize(self.file_corpus_path,self.vec_doc2bow_corpus)
        if self.representation=='tfidf':
            tfidf_model=models.TfidfModel(self.vec_doc2bow_corpus)
            vector=self.tfidf_model[vector]
        #Upate model
        self.topic_detection_model.update(vector)
        self.topic_detection_model.save(self.topic_model_path)

        # Get most relevant topics
        topic_dist_vector=self.topic_detection_model[vector]
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

    publications=data_connector.NewsReader(app='mongo', db='german_news', collection='publications_crawled').read_news()


    preprocessor=preprocessing.Preprocessor(remove_stopwords_model='nltk', stemming_model=None, lemmatization_model=None)

    publications_tokenized=[]
    for publication in publications:
        tokenized_publication=preprocessor.clean_and_tokenize(publication.content['title'])
        print(tokenized_publication)
        publications_tokenized.append(tokenized_publication)

    topic_detection_model=TopicDetector(model_name='lda500_publications', models_path='models', method='lda', train_corpus=publications_tokenized, representation='tfidf', num_topics=500, collection_name='publications')

    for publication in publications_tokenized:
        topics=topic_detection_model.get_topics(publication, show_top_n_topics=3)
        print(topics)

    print("Finished")