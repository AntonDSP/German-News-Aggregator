# Scripts to extract named entities
import de_core_news_sm
from src.utils import data_connector

class NERExtractor:
    def __init__(self, ner_model):
        self.ner_model=ner_model
        if self.ner_model=='spacy':
            self.recognizer=de_core_news_sm.load()

    def _splitCount(s: str, count: int)->list:
        """Split large text in smaller parts
        Args:
            s (str): Input text as string
            count (int): Number of maximal chracters in one part of the text
        Returns:
            list: List of strings as parts of the input string
        """
        return [''.join(x) for x in zip(*[list(s[z::count]) for z in range(count)])]


    def extract_spacy(self, text: str)->dict:
        """Extracte ners give input string using spacy
        Args:
            text (list): Input text as list of tokens
        Returns:
            dict: dictionary which consists of lists of persons, locations, orgs and other recognized named entities
        """
        ners=None
        try:
            persons=[]
            locations=[]
            orgs=[]
            misc=[]
            docs=[]
            if len(text)>1000000:
                    docs=self._splitCount(text,1000000)
            else:
                docs.append(text)
            for doc in docs:
                doc_spacy = self.recognizer(doc)
                for token in doc_spacy:
                    if token.ent_type_ == "PER":
                        persons.append(token.text)
                    if token.ent_type_ == "LOC":
                        locations.append(token.text)
                    if token.ent_type_ == "ORG":
                        orgs.append(token.text)
                    if token.ent_type_ == "MISC":
                        misc.append(token.text)
            ners={"persons":list(set(persons)), "locations":list(set(locations)),"orgs":list(set(orgs)), "misc":list(set(misc))}
        except Exception as ex:
            print('Exception while extracting NERs')
            print(str(ex))
        finally:
            return ners

    def extract(self, text: str) -> list:
        """Run NER as defined in the aggregator configuration file
        Args:
            text (str): Text for named entity extraction
            config (dict): Configuration for NER
        Returns:
            list: List of recognized named entities
        """
        nes={}
        if self.ner_model == 'spacy':
            nes=self.extract_spacy(text, self.recognizer)
        return nes


"""Main part to test this module"""
if __name__ == '__main__':
    reader_config={'APP':'mongo', 'DB':'german_news', 'COLLECTION':'publications'}
    writer_config={'APP':'mongo', 'DB':'german_news', 'COLLECTION':'publications_test_ners'}
    news_reader = data_connector.NewsReader(config=reader_config)
    news_writer = data_connector.NewsWriter(config=writer_config)
    news = news_reader.read_news()
    nerextractor=NERExtractor(ner_model='spacy')
    for news_item in news:
        try:
            news_item.content['nes'] = nerextractor.extract(news_item.content['text'])
            print(news_item.content['nes'])
        except Exception as ex:
            print(str(ex))
            print("No NER was extracted")
        news_writer.write_news(news_item)