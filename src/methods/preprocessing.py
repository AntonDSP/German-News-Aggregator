# Preprocessing and clean-up
import gensim
from nltk.corpus import stopwords
from nltk.stem.snowball import GermanStemmer
from src.utils import data_connector


stop_words = stopwords.words('german')
stemmer = GermanStemmer()

def default_preprocessing_gensim(text: str)->list:
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

def remove_stop_word_nltk(text: list)->list:
    """Remove stop words using nltk
    Args:
        text (list): Input text as list of tokens
    Returns:
        list: List of strings as tokens
    """
    return  [t for t in text if t not in stop_words]

def stemming_nltk(text: list)->list:
    """Stemming using nltk
    Args:
        text (list): Input text as list of tokens
    Returns:
        list: List of strings as tokens
    """
    stemmed_list=[]
    try:
        stemmed_list=[stemmer.stem(t) for t in text]
    except Exception as ex:
        print(str(ex))
        print('Exception while stemming')
    finally:
        return stemmed_list

def clean_and_tokenize(text: str, remove_stopwords_model: str, stemming_model: str)->list:
    """Run preprocessing as defined in the aggregator configuration file
    Args:
        text (str): Text to be cleaned up
    Returns:
        list: List of tokens after text preprocessing
    """
    text_as_tokens = default_preprocessing_gensim(text)
    if remove_stopwords_model=='nltk':
        text_as_tokens = remove_stop_word_nltk(text_as_tokens)
    if stemming_model=='nltk':
        text_as_tokens = stemming_nltk(text_as_tokens)
    return text_as_tokens



if __name__ == '__main__':
    config=default_config
    news_reader= data_connector.NewsReader(config['SOURCE']['COLLECTION'])
    news_writer= data_connector.NewsWriter(config['TARGET']['COLLECTION'])
    news=news_reader.news_stream
    for news_item in news:
        news_item.content['text']=clean_and_tokenize(news_item.content['text'], remove_stopwords=config['PREPROCESSING']["REMOVE_STOPWORDS"],
                                                     stemming_model=config["PREPROCESSING"]["STEMMING"])
        news_item.content['title'] = clean_and_tokenize(news_item.content['title'], remove_stopwords=config['PREPROCESSING']["REMOVE_STOPWORDS"],
                                                     stemming_model=config["PREPROCESSING"]["STEMMING"])
        news_item.content['description'] = clean_and_tokenize(news_item.content['description'], remove_stopwords=config['PREPROCESSING']["REMOVE_STOPWORDS"],
                                                     stemming_model=config["PREPROCESSING"]["STEMMING"])
        news_writer.write_news(news_item)

