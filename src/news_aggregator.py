import json
import os
from src.methods import ner_extraction, preprocessing, text_representation, clustering, keywords_extraction, sentiment_analysis, topic_detection, text_summarization
from src.utils import data_connector
import time



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

        #Initialize key phrases model
        self.keyphrase_model=keywords_extraction.KeyPhraseExtractor(model_name=config['KEYPHRASES']['MODEL_NAME'], num_of_phrases=config['KEYPHRASES']['NUM_OF_PHRASES'])
        print("Keyphrases model is added: "+config['KEYPHRASES']['MODEL_NAME'])

        #Intialize topic detection model
        self.topic_detection_model=topic_detection.TopicDetector(model_name=config['TDT']['MODEL_NAME'], representation=config['TDT']['REPRESENTATION'], \
                                                                 num_topics=config['TDT']['NUM_OF_TOPICS'], time_range=config['TDT']['TIME_RANGE'])
        print("Topic detection model is added: "+ config['TDT']['MODEL_NAME'])

        #Initialize sentiment analysis model
        self.sentiment_analysis_model=sentiment_analysis.SentimentAnalyzer(model_name=config['SENTIMENT_ANALYSIS']['MODEL_NAME'])
        print("Sentiment analysis model is added: "+ config['SENTIMENT_ANALYSIS']['MODEL_NAME'])

        #Initialize cluster model
        self.cluster_model=clustering.CluterModel(model_name=config['CLUSTERING']['MODEL_NAME'], threshold=config['CLUSTERING']['THRESHOLD'] , \
                                similarity_measure=config['CLUSTERING']['SIMILARITY_MEASURE'], time_range=config['CLUSTERING']['TIME_RANGE'], model_params=config['CLUSTERING']['MODEL_PARAM'])
        print("Clustering model is added: " + config['CLUSTERING']['MODEL_NAME'])

        #Initialize text summarization model
        self.text_summarization_model=text_summarization.TextSummarizer(model_name=config['TEXT_SUMMARIZATION']['MODEL_NAME'], num_of_sentences=config['TEXT_SUMMARIZATION']['NUM_OF_SENTENCES'])
        print("Text summarization model is added: " + config['TEXT_SUMMARIZATION']['MODEL_NAME'])


    def run_pipeline(self, publications):
        print("Event description extraction and feature generation")
        for publication in publications:
            # Named entities recognition
            named_entities=self.ner_model.extract(publication.concatenate_content(self.config['NER']['USE']))
            publication.content['ne_locations']=named_entities['locations']
            publication.content['ne_persons']=named_entities['persons']
            publication.content['ne_orgs']=named_entities['orgs']
            publication.content['ne_misc']=named_entities['misc']
            #Sentiment score
            sentiment_scores=self.sentiment_analysis_model.score(publication.concatenate_content(self.config['SENTIMENT_ANALYSIS']['USE']))
            publication.content['polarity']=sentiment_scores['polarity']
            publication.content['subjectivity']=sentiment_scores['subjectivity']
            #Keywords
            publication.content['keyphrases']=self.keyphrase_model.extract(publication.concatenate_content(self.config['KEYPHRASES']['USE']))
            #Clustering, feature generation
            tokenized_text = self.preprocessor.clean_and_tokenize(publication.concatenate_content(self.config['CLUSTERING']['USE']))
            publication.clust_features = self.text2vector_model.transform(tokenized_text)

        # Get words of top topic
        print('Getting words of top topic')
        publications_with_topics=self.topic_detection_model.assign_topics(publications)

        # Cluster assignment
        print("Clustering")
        publications_with_cluster_assignments = self.cluster_model.assign_clusters(publications_with_topics)

        # Text summarization
        print("Clustering based text summarization")
        publications_after_text_summarization=self.text_summarization_model.summarize_publications_clusters(publications_with_cluster_assignments)

        return publications_after_text_summarization



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
    writer=data_connector.NewsWriter(app=flow_config['TARGET']['APP'], db=flow_config['TARGET']['DB'], collection=flow_config['TARGET']['COLLECTION'])

    news_reader=data_connector.NewsReader(app=flow_config['SOURCE']['APP'], db=flow_config['SOURCE']['DB'],collection=flow_config['SOURCE']['COLLECTION'])

    print('# Run pipeline')
    while(True):
        news_stream = news_reader.read_news()
        l=list(news_stream)
        publications_result=aggregator.run_pipeline(publications=l)
        i=0
        for p in publications_result:
            i=i+1
            print(p.content['cluster']+"       "+str(i))
            writer.write_news(p)
        print('looping..')
        news_reader.reader.commit()
        time.sleep(300)
    news_reader.reader.close()

    print("End of run")