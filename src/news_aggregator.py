import json
import os
from src.methods import ner_extraction, preprocessing, text_representation, clustering, keywords_extraction, sentiment_analysis, topic_detection, text_summarization
from src.utils import data_connector
import time
import datetime



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
        print('L '+ str(datetime.datetime.now().time())+' Processing component is initialized successfuly')

        # Initialize text to vector model
        self.text2vector_model=text_representation.TextToVector(word_representation=config['TEXT_REPRESENTATION']['REPRESENTATION'], \
                          models_path=config['TEXT_REPRESENTATION']['MODELS_PATH'], model_name=config['TEXT_REPRESENTATION']['MODEL'])
        print('L '+ str(datetime.datetime.now().time())+' Text2vector component is added: ' + config['TEXT_REPRESENTATION']['MODEL'])

        #Initialize NER
        self.ner_model=ner_extraction.NERExtractor(ner_model=config['NER']['MODEL_NAME'])
        print('L  '+ str(datetime.datetime.now().time())+' Named entities recognizer is added: '+config['NER']['MODEL_NAME'])

        #Initialize key phrases model
        self.keyphrase_model=keywords_extraction.KeyPhraseExtractor(model_name=config['KEYPHRASES']['MODEL_NAME'], num_of_phrases=config['KEYPHRASES']['NUM_OF_PHRASES'])
        print('L '+ str(datetime.datetime.now().time())+' Keyphrases model is added: '+config['KEYPHRASES']['MODEL_NAME'])

        #Intialize topic detection model
        self.topic_detection_model=topic_detection.TopicDetector(model_name=config['TDT']['MODEL_NAME'], representation=config['TDT']['REPRESENTATION'], \
                                                             num_topics=config['TDT']['NUM_OF_TOPICS'], time_range=config['TDT']['TIME_RANGE'])
        print('L '+ str(datetime.datetime.now().time())+' Topic detection model is added: '+ config['TDT']['MODEL_NAME'])

        #Initialize sentiment analysis model
        self.sentiment_analysis_model=sentiment_analysis.SentimentAnalyzer(model_name=config['SENTIMENT_ANALYSIS']['MODEL_NAME'])
        print('L '+ str(datetime.datetime.now().time())+' Sentiment analysis model is added: '+ config['SENTIMENT_ANALYSIS']['MODEL_NAME'])

        #Initialize cluster model
        self.cluster_model=clustering.CluterModel(model_name=config['CLUSTERING']['MODEL_NAME'], threshold=config['CLUSTERING']['THRESHOLD'] , \
                            similarity_measure=config['CLUSTERING']['SIMILARITY_MEASURE'], time_range=config['CLUSTERING']['TIME_RANGE'], model_params=config['CLUSTERING']['MODEL_PARAM'])
        print('L '+ str(datetime.datetime.now().time())+' Clustering model is added: ' + config['CLUSTERING']['MODEL_NAME'])

        #Initialize text summarization model
        self.text_summarization_model=text_summarization.TextSummarizer(model_name=config['TEXT_SUMMARIZATION']['MODEL_NAME'], num_of_sentences=config['TEXT_SUMMARIZATION']['NUM_OF_SENTENCES'])
        print('L '+ str(datetime.datetime.now().time())+' Text summarization model is added: ' + config['TEXT_SUMMARIZATION']['MODEL_NAME'])


    def run_pipeline(self, publications):

        print('L  '+ str(datetime.datetime.now().time())+' Event description extraction and feature generation')
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
            #Keyphrases
            publication.content['keyphrases']=self.keyphrase_model.extract(publication.concatenate_content(self.config['KEYPHRASES']['USE']))
            #Clustering, feature generation
            tokenized_text = self.preprocessor.clean_and_tokenize(publication.concatenate_content(self.config['CLUSTERING']['USE']))
            publication.clust_features = self.text2vector_model.transform(tokenized_text)

        if  publications:
            # Get words of top topic
            print('L '+ str(datetime.datetime.now().time())+' Getting words of top topic')
            publications_with_topics=self.topic_detection_model.assign_topics(publications)

            # Cluster assignment
            print('L '+ str(datetime.datetime.now().time())+' Clustering')
            publications_with_clusters = self.cluster_model.assign_clusters(publications_with_topics)

            # Text summarization
            print('L '+ str(datetime.datetime.now().time())+' Clustering based text summarization')
            publications_with_text_summaries=self.text_summarization_model.summarize_publications_clusters(publications_with_clusters)

        return publications_with_text_summaries



if __name__ == '__main__':
    print("Run...")
    flow_config_path = os.path.join(ROOT_PATH, 'flow_config.json')
    with open(flow_config_path, 'r') as f:
        flow_config = json.load(f)

    news_aggregator_file_name=flow_config['NEWS_AGGREGATOR_CONFIG']+".json"
    news_aggregator_config_path=os.path.join(ROOT_PATH, news_aggregator_file_name)
    with open(news_aggregator_config_path, 'r') as f:
        news_aggregator_config = json.load(f)

    print('# '+ str(datetime.datetime.now().time())+' Initilize news aggregator')
    aggregator=NewsAggregator(news_aggregator_config)

    print('# '+ str(datetime.datetime.now().time())+' Create reader and writer for input and output streams')
    # Write back results to mongo
    target_writer=data_connector.NewsWriter(app=flow_config['TARGET']['APP'], db=flow_config['TARGET']['DB'], collection=flow_config['TARGET']['COLLECTION'])

    backup_writer=data_connector.NewsWriter(app=flow_config['TARGET_BACKUP']['APP'], db=flow_config['TARGET_BACKUP']['DB'], collection=flow_config['TARGET_BACKUP']['COLLECTION'])

    news_reader=data_connector.NewsReader(app=flow_config['SOURCE']['APP'], db=flow_config['SOURCE']['DB'],collection=flow_config['SOURCE']['COLLECTION'])

    while(True):
        start=datetime.datetime.now().second
        print('#  '+ str(datetime.datetime.now().time())+' Get news')
        news_stream = news_reader.read_news()
        publications=list(news_stream)
        news_reader.reader.commit()

        # Process publications in chunks, only for the initial run to get faster the first results. In streaming modus, it will be never (with current crawling :P) 1000 publications withtin 5 minutes
        offset = 0
        chunk_size = 1000
        last_element = 0
        while (last_element < len(publications)):
            last_element = (offset + chunk_size) if (offset + chunk_size < len(publications)) else len(publications)
            publications_chunk = publications[offset:last_element]
            print('#  '+ str(datetime.datetime.now().time())+' Run pipeline for '+str(len(publications_chunk))+" publications")
            publications_chunk=aggregator.run_pipeline(publications_chunk)
            print('# '+ str(datetime.datetime.now().time())+'  Write results: '+str(len(publications_chunk))+" publications")
            for publication in publications_chunk:
                target_writer.write_news(publication)
                backup_writer.write_news(publication)
            offset = last_element

        end=datetime.datetime.now().second
        if (300-(end-start))>0:
            sleep_time=300-(end-start)
            print('#  ' + str(datetime.datetime.now().time()) + ' looping.. ' + str(round((sleep_time/60),0))+ ' minutes')
            time.sleep(sleep_time)

    news_reader.reader.close()

    print("End of run")