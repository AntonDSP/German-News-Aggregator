# Preprocessing and clean-up
import gensim
from nltk.corpus import stopwords
from nltk.stem.snowball import GermanStemmer
from src.utils import data_connector
from textblob_de import TextBlobDE as TextBlob

"""Class responsible for preprocessing"""
class Preprocessor:
    def __init__(self, remove_stopwords_model, stemming_model,lemmatization_model):
        self.remove_stopwords_model=remove_stopwords_model
        self.stemming_model=stemming_model
        self.lemmatization_model=lemmatization_model
        if remove_stopwords_model=='nltk':
            self.stop_words=stopwords.words('german')
        if stemming_model=='nltk':
            self.stemmer=GermanStemmer()


    def default_preprocessing_gensim(self, text: str)->list:
        """Tokenazition and removal of punctations and other specific characters
        Args:
            text (str): Input text to preprocess
        Returns:
            list: List of strings as tokens
        """
        text_tokens=[]
        try:
            text_tokens=gensim.parsing.preprocess_string(text)
        except Exception as ex:
            print(str(ex))
            print('Exception while preprocessing with gensim')
        finally:
            return text_tokens


    def remove_stop_word_nltk(self, text: list)->list:
        """Remove stop words using nltk
        Args:
            text (list): Input text as list of tokens
        Returns:
            list: List of strings as tokens
        """
        return  [t for t in text if t not in self.stop_words]

    def stemming_nltk(self, text: list)->list:
        """Stemming using nltk
        Args:
            text (list): Input text as list of tokens
        Returns:
            list: List of strings as tokens
        """
        stemmed_list=[]
        try:
            stemmed_list=[self.stemmer.stem(t) for t in text]
        except Exception as ex:
            print(str(ex))
            print('Exception while stemming')
        finally:
            return stemmed_list

    def lemmatization_blob(self, text):
        return TextBlob(text).words.lemmatize()

    def clean_and_tokenize(self, text: str)->list:
        """Run preprocessing as defined in the aggregator configuration file
        Args:
            text (str): Text to be cleaned up
        Returns:
            list: List of tokens after text preprocessing
        """
        text_as_tokens = self.default_preprocessing_gensim(text)
        if self.remove_stopwords_model=='nltk':
            text_as_tokens = self.remove_stop_word_nltk(text_as_tokens)
        if self.stemming_model=='nltk':
            text_as_tokens = self.stemming_nltk(text_as_tokens)
        if self.lemmatization_model=='blob':
            text_as_tokens=list(self.lemmatization_blob(text))
        return text_as_tokens



if __name__ == '__main__':

    config_reader = {'APP':'mongo'
              ,'DB':'german_news'
              ,'COLLECTION':'publication'}
    config_writer = {'APP':'mongo',
                     'DB':'german_news',
                     'COLLECTION':'publications_test_preprocessing'}


    news_reader=data_connector.NewsReader(config=config_reader)
    news_writer=data_connector.NewsWriter(config=config_writer)

    news=news_reader.news_stream

    preprocessor=Preprocessor(remove_stopwords_model="nltk", stemming_model="nltk",lemmatization_model=None)

    for news_item in news:
        news_item.content['text']=preprocessor.clean_and_tokenize(news_item.content['text'])
        news_item.content['title'] = preprocessor.clean_and_tokenize(news_item.content['title'])
        news_item.content['description'] = preprocessor.clean_and_tokenize(news_item.content['description'])
        news_writer.write_news(news_item)

