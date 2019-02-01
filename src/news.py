# Model of news item. News item represents news published in web
from src.utils import data_connector
import os
import json
import numpy as np
import dateutil.parser

class NewsItem:
    features={}
    clust_features=None
    similarity_measure='cosine'
    threshold=0.1
    def __init__(self, content):
        self.content=content

    def distance(self, other):
        if isinstance(self.clust_features, np.ndarray)==False or isinstance(other.clust_features, np.ndarray)==False:
            distance=1
        elif abs((dateutil.parser.parse(self.content['published']).replace(tzinfo=None)-dateutil.parser.parse(other.content['published']).replace(tzinfo=None)).days)>3:
            # If difference between two publications more than 3 days, then it can be about the same story
            distance=1
        elif self.similarity_measure=='cosine':
            distance=1-(np.dot(self.clust_features,other.clust_features)/(np.linalg.norm(self.clust_features)*np.linalg.norm(other.clust_features)))
        else:
            distance=None
        return distance

    def is_correlated(self, other):
        if isinstance(self.clust_features, np.ndarray) == False or isinstance(other.clust_features,np.ndarray) == False:
            is_similiar=False
        elif abs((dateutil.parser.parse(self.content['published']).replace(tzinfo=None)-dateutil.parser.parse(other.content['published']).replace(tzinfo=None)).days)>3:
            # If difference between two publications more than 3 days, then it can be about the same story
            is_similiar=False
        elif self.similarity_measure=='cosine':
            cos_similarity=(np.dot(self.clust_features,other.clust_features)/(np.linalg.norm(self.clust_features)*np.linalg.norm(other.clust_features)))
            is_similiar= cos_similarity>self.threshold
        else:
            is_similiar=False
        return is_similiar

    def concatenate_content(self, content_parts):
        joint_content=""
        for part in content_parts:
            if self.content[part] is not None:
                if isinstance(self.content[part], list):
                    joint_content=joint_content.join(self.content[part])
                else:
                    joint_content=joint_content+self.content[part]
        return joint_content



if __name__ == '__main__':
    print("Run...")
    #create_doc2bow_lda_model(config)
    ROOT_PATH = os.path.abspath('../')
    flow_config_path = os.path.join(ROOT_PATH, 'flow_config.json')
    with open(flow_config_path, 'r') as f:
        flow_config = json.load(f)

    news_aggregator_file_name=flow_config['NEWS_AGGREGATOR_CONFIG']+".json"
    news_aggregator_config_path=os.path.join(ROOT_PATH, news_aggregator_file_name)
    with open(news_aggregator_config_path, 'r') as f:
        news_aggregator_config = json.load(f)

    news_stream = data_connector.NewsReader(flow_config['SOURCE']).read_news()
    for news_item in news_stream:
        print(news_item.content['url'])

    print("Finished")