import json
import os

import models.src.topic_detection
import preprocessing
from gensim import corpora, models
from models.src import topic_detection
from models.src.utils import data_connector

from src import ner_extraction

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


    def recognize_named_entities(self, publications):
        for publication in publications:
            ner= ner_extraction.named_entity_extraction(text=publication.concatenate_content(self.config['NER']['USE'])
                                                        , recognizer=self.named_entities_recognizer)
            yield ner



    def detect_topics(self, publications):
        topics={}
        return topics

    def doc2vec(self, representation, tokens_stream):
        if representation=='doc2bow':
            return self.doc2bow(tokens_stream)
        else:
            return None


    def doc2bow(self, tokens_stream):
        for tokens in tokens_stream:
            yield self.dictionary.doc2bow(tokens)
