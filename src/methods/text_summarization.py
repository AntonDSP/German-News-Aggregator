# Scripts to extract keywords
from src.utils import data_connector
from summa.summarizer import summarize as textrank_summarize
from summarize import summarize as lexrank_summarize


class TextSummarizer:
    def __init__(self, model_name, num_of_sentences):
        self.model_name=model_name
        self.num_of_sentences=num_of_sentences


    def summarize(self, text):
        if self.model_name=='textrank':
            summary=textrank_summarize(text, words=self.num_of_sentences*16, language='german')
        elif self.model_name=='lexrank':
            summary=lexrank_summarize(text, sentence_count=self.num_of_sentences, language='german')
        else:
            summary=None
        return summary

    def summarize_publications_clusters(self, publications):
        print()
        return result_publications

"""Main part to test this module"""
if __name__ == '__main__':
    news_reader = data_connector.NewsReader(app='mongo',db='german_news', collection='publications')
    publications = news_reader.read_news()
    text_summarization_model=TextSummarizer(model_name='textrank', num_of_sentences=5)
    for publication in publications:
        try:
            summary = text_summarization_model.summarize(publication.content['text'])
            print(summary)
        except Exception as ex:
            print(str(ex))
            print("No summary was generated")
