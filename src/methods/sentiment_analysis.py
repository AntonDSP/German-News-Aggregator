# Scripts to score text polarity and subjectivity
from src.utils import data_connector
from textblob_de import TextBlobDE as TextBlob

class SentimentAnalyzer:
    def __init__(self, model_name):
        self.model_name=model_name


    def score(self, text):
        result={}
        if self.model_name=='blob':
            sentiment_score = TextBlob(text).sentiment
            result.update({'polarity':float(sentiment_score.polarity,2), 'subjectivity':sentiment_score.subjectivity})
        else:
            result=None
        return result


"""Main part to test this module"""
if __name__ == '__main__':
    news_reader = data_connector.NewsReader(app='mongo',db='german_news', collection='publications')
    publications = news_reader.read_news()
    sentiment_analysis_model=SentimentAnalyzer(model_name='blob')
    for publication in publications:
        try:
            scores = sentiment_analysis_model.score(publication.content['text'])
            print(scores)
        except Exception as ex:
            print(str(ex))
            print("Sentiment assessment failed")
