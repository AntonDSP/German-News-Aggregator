import json
import os
from src.methods import ner_extraction, preprocessing, aclust, text_representation, clustering
from src.utils import data_connector

ROOT_PATH=os.path.abspath('./')
CONFIG_PATH = os.path.join(ROOT_PATH, 'news_aggregator_config.json')
with open(CONFIG_PATH, 'r') as f:
    default_config = json.load(f)

class NewsAggregator:
    def __init__(self, config=default_config):
        self.config=default_config
        #Initialize preprocessor
        self.preprocessor=preprocessing.Preprocessor(remove_stopwords_model=config['PREPROCESSING']['REMOVE_STOPWORDS'], \
                          stemming_model=config['PREPROCESSING']['STEMMING'], lemmatization_model=config['PREPROCESSING']['STEMMING'])
        print("Processing component is initialized successfuly")

        # Initialize text to vector model
        self.text2vector_model=text_representation.TextToVector(word_representation=config['TEXT_REPRESENTATION']['REPRESENTATION'], \
                          models_path=config['TEXT_REPRESENTATION']['MODELS_PATH'], model_name=config['TEXT_REPRESENTATION']['MODEL'])
        print("Text2vector component is added: " + config['TEXT_REPRESENTATION']['MODEL'])

        #Initialize NER
        self.ner_model=ner_extraction.NERExtractor(ner_model=config['NER']['MODEL_NAME'])
        print("Named entities recognizer is added: "+config['NER']['MODEL_NAME'])

        #Initlize cluster model
        self.cluster_model=clustering.CluterModel(model_name=config['CLUSTERING']['MODEL_NAME'], threshold=config['CLUSTERING']['THRESHOLD'] , \
                                similarity_measure=config['CLUSTERING']['SIMILARITY_MEASURE'], time_range=config['CLUSTERING']['TIME_RANGE'], model_params=config['CLUSTERING']['MODEL_PARAM'])
        print("Clustering model is added: " + config['CLUSTERING']['MODEL_NAME'])


    def generate_event_decriptors(self, publications):
        for publication in publications:
            # Named entities recognition
            named_entities=self.ner_model.extract(publication.concatenate_content(self.config['NER']['USE']))
            publication.content['ne_locations']=named_entities['locations']
            publication.content['ne_persons']=named_entities['persons']
            publication.content['ne_orgs']=named_entities['orgs']
            publication.content['ne_misc']=named_entities['misc']
            print("ner finished")
            # Top 3 discussed topics
            #Sentiment score
            #Keywords
            yield publication

    def cluster(self, publications):
        for publication in publications:
            tokenized_text = self.preprocessor.clean_and_tokenize(publication.concatenate_content(self.config['CLUSTERING']['USE']))
            publication.clust_features = self.text2vector_model.transform(tokenized_text)
            print("tokenized")
        publications_with_cluster_assignments=self.cluster_model.assign_clusters(publications)
        return publications_with_cluster_assignments



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

    print('#Create reader and writer for input stream')


    # Write back results to mongo
    #writer=data_connector.NewsWriter(app=flow_config['TARGET']['APP'], db=flow_config['TARGET']['DB'], collection=flow_config['TARGET']['COLLECTION'] \
    #                                 ,cluster_collection=flow_config['TARGET']['CLUSTER_COLLECTION'])

    news_reader=data_connector.NewsReader(app=flow_config['SOURCE']['APP'], db=flow_config['SOURCE']['DB'],collection=flow_config['SOURCE']['COLLECTION'])

    news_stream = news_reader.read_news()
   # print('# Extract event descriptions')
   # publications_with_event_descriptions=list(aggregator.generate_event_decriptors(publications=news_stream))
    #publications_with_event_descriptions=list(news_stream)
    #for p in publications_with_event_descriptions:
     #   print(p)

    print('# Clustering')
    l=list(news_stream)
    publications_with_cluster_assignments=aggregator.cluster(publications=l)

    for p in publications_with_cluster_assignments:
        print(p.content['cluster'])

    #pubs=aggregator.cluster_model.assign_existing_clusters(news_stream)
    #clusters=aggregator.cluster_model.create_news_clusters(pubs)

    #for cluster in clusters:
    #    for p in cluster:
    #        print("------------------------")
    #        print(p[0].content['cluster'])
    #for publ in publications_with_cluster_assignments:
     #
    #    print(publ)


    # for cluster in clusters:
    #     writer.write_cluster(cluster)
    #     for publication in cluster:
    #         writer.write_news(publication)
    # print("Cluster generated and published")


    print("Finished")