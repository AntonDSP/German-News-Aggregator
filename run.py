import os
import json
import datetime
from src.news_aggregator import NewsAggregator
from src.utils import data_connector
import time

ROOT_PATH=os.path.abspath('./')
CONFIG_PATH = os.path.join(ROOT_PATH, 'news_aggregator_config.json')
with open(CONFIG_PATH, 'r') as f:
    default_config = json.load(f)

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