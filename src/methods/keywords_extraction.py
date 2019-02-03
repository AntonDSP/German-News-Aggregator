# Scripts to extract keywords
from src.utils import data_connector
from gensim.summarization import keywords
from multi_rake import Rake
from nltk.corpus import stopwords
from textblob_de import TextBlobDE as TextBlob

class KeyPhraseExtractor:
    def __init__(self, model_name, num_of_phrases):
        self.model_name=model_name
        self.num_of_phrases=num_of_phrases
        if model_name=='gensim':
            self.pos_filter=('FW','JJ','NN','NNS','NNP','NNPS','VB','VBD','VBG','VBN','VBP','VBZ')
        if model_name=='rake':
            self.rake=Rake(language_code='de', stopwords=stopwords.words('german'))


    def extract(self, text):
        if self.model_name=='gensim':
            kphrases=keywords(text, words=self.num_of_phrases, pos_filter=self.pos_filter).split('\n')
        elif self.model_name=='rake':
            kphrases_l=self.rake.apply(text)[:self.num_of_phrases]
            kphrases=[w[0] for w in kphrases_l]
        elif self.model_name=='blob':
            kphrases= list(TextBlob(text).noun_phrases[:self.num_of_phrases])
        else:
            kphrases=None
        return kphrases


"""Main part to test this module"""
if __name__ == '__main__':
    news_reader = data_connector.NewsReader(app='mongo',db='german_news', collection='publications')
    publications = news_reader.read_news()
    keyphrase_model=KeyPhraseExtractor(model_name='rake',num_of_phrases=5)
    for publication in publications:
        try:
            kwords = keyphrase_model.extract(publication.content['text'])
            print(kwords)
        except Exception as ex:
            print(str(ex))
            print("No keyword was extracted")
