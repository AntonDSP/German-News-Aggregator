import json

from aclust import mclust

from src.utils import data_connector

with open('aggregator_config.json', 'r') as f:
    default_config = json.load(f)

if __name__ == '__main__':
    print("Run...")
    config=default_config
    news_stream= data_connector.NewsReader(config['SOURCE']).read_news()
    mclust(news_stream, max_dist=400, max_skip=1)
    print("Finished")