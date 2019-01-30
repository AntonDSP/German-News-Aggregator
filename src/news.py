# Model of news item. News item represents news published in web
from gensim.matutils import cossim
from src.utils import data_connector
import os
import json
import uuid

class NewsItem:
    features={}
    clust_features=None
    similarity_measure='cosine'
    threshold=0.1
    def __init__(self, content):
        self.content=content
    #    self.content['publication_id']= str(uuid.uuid3(uuid.NAMESPACE_URL, (content['url']+content['published'])))

    def distance(self, other):
        if self.similarity_measure=='cosine':
            distance=1-cossim(self.clust_features, other.clust_features)
        else:
            distance=None
        print("Distnace: "+str(distance))
        return distance

    def is_correlated(self, other):
        if self.similarity_measure=='cosine':
            is_similiar= cossim(self.clust_features, other.clust_features)>self.threshold
        else:
            is_similiar=False
        print("Is similiar: "+str(is_similiar))
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