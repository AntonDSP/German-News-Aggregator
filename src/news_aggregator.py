import json
import os
from gensim import corpora, models
from src.methods import ner_extraction, topic_detection, preprocessing
from src.utils import data_connector

with open('aggregator_config.json', 'r') as f:
    default_config = json.load(f)

class NewsAggregator:
    def __init__(self, config=default_config):
        # Load if exists or create dictionary
        self.config=config
        dictionary_name = config['CORPORA']['COLLECTION']+'.dict'
        dictionary_path = os.path.join('models',dictionary_name)
        news=None
        if os.path.isfile(dictionary_path):
            self.dictionary=corpora.Dictionary.load(dictionary_path)
        else:
            news= data_connector.NewsReader(config['CORPORA']).read_news()
            texts=(publication.concatenate_content(config['CORPORA']['USE']) for publication in news)
            cleaned_content=(
            preprocessing.clean_and_tokenize(texts, remove_stopwords_model=config['PREPROCESSING']['REMOVE_STOPWORDS'],
                                             stemming_model=config['PREPORCESSING']['STEMMING']))
            self.dictionary=corpora.Dictionary(cleaned_content)
            self.dictionary.save(dictionary_path)

        #Load models
        #NER extraction model
        self.named_entities_recognizer= ner_extraction.load_ner_recognizer(model=config['NER']['MODEL'])

        #Topic detection model
        model_path = os.path.join('models', config['TDT']['MODEL_NAME'])
        if os.path.isfile(model_path):
            topic_detection_model = models.LdaModel.load(model_path, mmap='r')
        else:
            if news is None:
                news = data_connector.NewsReader(config['CORPORA']).read_news()
                news_cleaned = (preprocessing.clean_and_tokenize(news_item.content['text'], remove_stopwords_model=config['PRERPOCESSING']['REMOVE_STOPDWORS'],
                                                                 stemming_model=config['PREPROCESSING']['STEMMING']) for news_item in news)
                news_vector_rep=self.doc2vec(tokens_stream=news_cleaned, representation=config['TDT']['REPRESENTATION'])
                self.topic_detection_model= topic_detection.create_model(model_param=config['TDT']['MODEL_PARAM'], corpus=news_vector_rep)


    def extract_named_entities(self, publication):
        return ner_extraction.named_entity_extraction(text=publication.concatenate_content(self.config['NER']['USE'])
                                                        , recognizer=self.named_entities_recognizer)


    def extract_topics(self, publication):
        publication_cleaned = preprocessing.clean_and_tokenize(text=publication.content['text'], remove_stopwords_model=config['PRERPOCESSING']['REMOVE_STOPDWORS'],
                                                                   stemming_model=config['PREPROCESSING']['STEMMING'])
        publication_vec_repres = self.doc2vec(tokens_stream=publication_cleaned, representation=config['TDT']['REPRESENTATION'])
        return self.topic_detection_model[publication_vec_repres]


    def doc2vec(self, representation, text_as_tokens):
        if representation=='doc2bow':
            return self.dictionary.doc2bow(text_as_tokens)
        else:
            return None


if __name__ == '__main__':
    print("Run...")
    config=default_config
    #Initilize news aggregator
    aggregator=NewsAggregator(config)
    #Read input stream
    news_stream = data_connector.NewsReader(config['SOURCE']).read_news()

    # Create features
    for publication in news_stream:
        publication.features['ners']=aggregator.extract_named_entities(publication)
        publication.features['topics']=aggregator.extract_topics(publication)

    # Cluster
    km = MiniBatchKMeans(n_clusters=true_k, init='k-means++', n_init=1,
                         init_size=1000, batch_size=1000, verbose=opts.verbose)
    km.fit(publication.features['topics'])
    # Write back results to mongo


    for news_item in news_stream:
        print(news_item.content['url'])

    print("Finished")