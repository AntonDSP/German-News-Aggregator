import json
import os
from gensim import corpora, models
from src.methods import ner_extraction, topic_detection, preprocessing, aclust
from src.utils import data_connector

ROOT_PATH=os.path.abspath('../')
CONFIG_PATH = os.path.join(ROOT_PATH, 'news_aggregator_config.json')
with open(CONFIG_PATH, 'r') as f:
    default_config = json.load(f)

class NewsAggregator:
    def __init__(self, config=default_config):
        # Load if exists or create dictionary
        print('Start news aggregator configuration')
        self.config=config
        dictionary_name = config['CORPORA']['COLLECTION']+'.dict'
        dictionary_path = os.path.join(os.path.join(ROOT_PATH, 'models'), dictionary_name)
        print(dictionary_path)
        news=None
        if os.path.isfile(dictionary_path):
            self.dictionary=corpora.Dictionary.load(dictionary_path)
        else:
            news= data_connector.NewsReader(config['CORPORA']).read_news()
            texts=(publication.concatenate_content(config['CORPORA']['USE']) for publication in news)
            cleaned_content=(
            preprocessing.clean_and_tokenize(texts, remove_stopwords_model=config['PREPROCESSING']['REMOVE_STOPWORDS'],
                                             stemming_model=config['PREPROCESSING']['STEMMING']))
            self.dictionary=corpora.Dictionary(cleaned_content)
            self.dictionary.save(dictionary_path)

        print("Dictionary added: "+dictionary_name)

        #Load models
        #NER extraction model
        self.named_entities_recognizer= ner_extraction.load_ner_recognizer(model=config['NER']['MODEL_NAME'])

        print("Named entities recognizer added: "+self.named_entities_recognizer.name)

        #Topic detection model
        model_path = os.path.join(ROOT_PATH, os.path.join('models', config['TDT']['MODEL_NAME']))
        print(model_path)
        if os.path.isfile(model_path):
            self.topic_detection_model = models.LdaModel.load(model_path, mmap='r')
        else:
            if news is None:
                news = data_connector.NewsReader(config['CORPORA']).read_news()
                news_cleaned = (preprocessing.clean_and_tokenize(news_item.content['text'], remove_stopwords_model=config['PREPROCESSING']['REMOVE_STOPWRDS'],
                                                                 stemming_model=config['PREPROCESSING']['STEMMING']) for news_item in news)
                news_vector_rep=(self.doc2vec(text_as_tokens=tokens, representation=config['TDT']['REPRESENTATION']) for tokens in news_cleaned)
                self.topic_detection_model= topic_detection.create_model(model_param=config['TDT']['MODEL_PARAM'], corpus=news_vector_rep)
                self.topic_detection_model.save(model_path)
        print('Topic detection model added: '+model_path)


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
    flow_config_path = os.path.join(ROOT_PATH, 'flow_config.json')
    with open(flow_config_path, 'r') as f:
        flow_config = json.load(f)

    news_aggregator_file_name=flow_config['NEWS_AGGREGATOR_CONFIG']+".json"
    news_aggregator_config_path=os.path.join(ROOT_PATH, news_aggregator_file_name)
    with open(news_aggregator_config_path, 'r') as f:
        news_aggregator_config = json.load(f)

    print('#Initilize news aggregator')
    aggregator=NewsAggregator(news_aggregator_config)
    print('#Read input stream')
    news_stream = data_connector.NewsReader(flow_config['SOURCE']).read_news()

    print('# Create features')
    for publication in news_stream:
        publication.features['ners']=aggregator.extract_named_entities(publication)
        publication.features['topics']=aggregator.extract_topics(publication)

    print('# Cluster')
    clusters=aclust.mclust(news_stream)

    print(clusters)

    # Write back results to mongo

    print("Finished")