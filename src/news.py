# Model of news item. News item represents news published in web
from gensim.matutils import cossim
from src.utils import data_connector

class NewsItem:
    features={}
    def __init__(self, content):
        self.content=content

    def distance(self, other):
        return 1-cossim(self.features['topics'], other.features['topics'])

    def is_correlated(self, other):
        return cossim(self.features['topics'], other.features['topics'])>0.5

    def concatenate_content(self, content_parts):
        joint_content=""
        for part in content_parts:
            joint_content=joint_content+self.content[part]
        return joint_content



if __name__ == '__main__':
    print("Run...")
    config=default_config
    #create_doc2bow_lda_model(config)
    news_stream = data_connector.NewsReader(config['SOURCE']).read_news()
    for news_item in news_stream:
        print(news_item.content['url'])

    print("Finished")