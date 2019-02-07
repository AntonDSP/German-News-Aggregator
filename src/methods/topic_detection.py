# Scripts for topics detection
import json
from gensim import models, corpora
from src.utils import data_connector
from src.methods import preprocessing
from src.utils import mongodb
import dateutil
import datetime

class TopicDetector:
    def __init__(self, model_name, representation, num_topics, time_range):
        self.model_name=model_name
        self.num_topics=num_topics
        self.representation=representation
        self.time_range=time_range


    def assign_topics(self, publications):
        mongo_coll = mongodb.connect2db(db='german_news').get_collection(name='publications')
        preprocessor = preprocessing.Preprocessor(remove_stopwords_model='nltk', stemming_model=None, lemmatization_model=None)
        new_l_publications = list()
        for publication in publications:
            # Get news last 3 days back
            try:
                start=dateutil.parser.parse(publication.content['published'])-datetime.timedelta(days=self.time_range)
                end = dateutil.parser.parse(publication.content['published'])
                query={"published":{"$lte":end.isoformat(),"$gte":start.isoformat()}}
                publications_within_time_range = mongo_coll.find(query)
                # Create dictionary
                tokenized_corpus=[preprocessor.clean_and_tokenize(p['text']) for p in publications_within_time_range]
                tokenized_text_publication=preprocessor.clean_and_tokenize(publication.content['text'])
                dictionary=corpora.Dictionary(tokenized_corpus)
                dictionary.filter_extremes(no_above=0.1) # Filter out words with low frequency
                # Vectorization
                vec_corpus = [dictionary.doc2bow(doc) for doc in tokenized_corpus]
                vec_publication=dictionary.doc2bow(tokenized_text_publication)
                if self.representation=='tfidf':
                    tfidf_model=models.TfidfModel(vec_corpus)
                    vec_corpus=tfidf_model[vec_corpus]
                    vec_publication=tfidf_model[vec_publication]
                # Model creation
                if self.model_name=='lda':
                    topic_detection_model = models.LdaModel(corpus=vec_corpus, num_topics=self.num_topics, id2word=dictionary)
                elif self.model_name=='lsi':
                    topic_detection_model=models.LsiModel(corpus=vec_corpus, num_topics=self.num_topics, id2word=dictionary)
                # Get most relevant topics
                topic_dist_vector=topic_detection_model[vec_publication]
                most_relevant_topic_id=max(topic_dist_vector, key=lambda item: item[1])[0]
                top_words=topic_detection_model.get_topic_terms(topicid=most_relevant_topic_id)
                publication.content['topics']=[dictionary.id2token[t[0]] for t in top_words]
            except:
                publication.content['topics']=''
            new_l_publications.append(publication)

        return new_l_publications


if __name__ == '__main__':

    print("Run...")

    publications=data_connector.NewsReader(app='mongo', db='german_news', collection='publications').read_news()

    topic_detection_model=TopicDetector(model_name='lda', representation='tfidf', num_topics=10, time_range=3)

    publications_with_topics=topic_detection_model.assign_topics(publications=publications)

    for publication in publications_with_topics:
        print(publication.content['title']+':            '+publication.content['topics'])

    print("Finished")