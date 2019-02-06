# Provide code for differen text representations
import os
from gensim import corpora
from gensim.models import KeyedVectors
from gensim.test.utils import datapath
from src.utils import data_connector
from src.methods import preprocessing
import numpy

class TextToVector:
    def __init__(self, word_representation, models_path, model_name):
        self.word_representation=word_representation
        self.models_path=models_path
        self.model_name=model_name
        if self.word_representation=='doc2bow':
            dictionary_path = os.path.join(self.models_path, self.model_name)
            self.dictionary = corpora.Dictionary.load(dictionary_path)
        elif self.word_representation=='word2vec':
                wv_model_path = os.path.join(self.models_path, self.model_name)
                path=os.path.abspath(wv_model_path)
                self.model=KeyedVectors.load_word2vec_format(datapath(path), binary=False)
        elif self.word_representation=='tfidf':
            print("Do something")
        else:
            print('Unknown word representation')


    def transform(self, tokenized_document):
        if self.word_representation=='doc2bow':
            self.dictionary.add_documents(tokenized_document)
            return self.dictionary.doc2bow(tokenized_document)
        elif self.word_representation=='word2vec':
            list_of_vectors=[]
            for token in tokenized_document:
                try:
                    list_of_vectors.append(self.model[token])
                except:
                    pass # do nothing as some words ar simply not in dictionary
            # Compute doc vector as average of word vectors
            arr_vectors=numpy.mean(list_of_vectors,axis=0)
            return arr_vectors
        elif self.word_representation=='tfidf':
            print("Do something")
        else:
            print("Unknown word representation")


"""Main part to test this module"""
if __name__ == '__main__':

    #Change directory to root directory
    os.chdir('/home/vdesktop/PycharmProjects/German-News-Aggregator/')

    news_reader = data_connector.NewsReader(app='mongo', db='german_news', collection='publications')

    publications = news_reader.read_news()

    text2vector_model=TextToVector(word_representation='word2vec', models_path='models', model_name='cc.de.300.vec')
    preprocessor=preprocessing.Preprocessor(remove_stopwords_model='nltk', stemming_model=None,lemmatization_model=None)
    for publication in publications:
        try:
            tokenized_document=preprocessor.clean_and_tokenize(text=publication.content['text'])
            print(tokenized_document)
            doc_vector = text2vector_model.transform(tokenized_document=tokenized_document)
            print(doc_vector)
        except Exception as ex:
            print(str(ex))
            print("No vector")


