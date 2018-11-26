# Ner extraction
import de_core_news_sm
from src.utils import data_connector

def splitCount(s: str, count: int)->list:
    """Split large text in smaller parts
    Args:
        s (str): Input text as string
        count (int): Number of maximal chracters in one part of the text
    Returns:
        list: List of strings as parts of the input string
    """
    return [''.join(x) for x in zip(*[list(s[z::count]) for z in range(count)])]

def load_ner_recognizer(model: str):
    if model=='spacy':
        recognizer=de_core_news_sm.load()
    recognizer.name=model
    return recognizer


def extract_nes_spacy(text: str, recognizer: object)->dict:
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
                docs=splitCount(text,1000000)
        else:
            docs.append(text)
        for doc in docs:
            doc_spacy = recognizer(doc)
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

def named_entity_extraction(text: str, recognizer: object) -> list:
    """Run NER as defined in the aggregator configuration file
    Args:
        text (str): Text for named entity extraction
        config (dict): Configuration for NER
    Returns:
        list: List of recognized named entities
    """
    nes={}
    if recognizer.name == 'spacy':
        nes=extract_nes_spacy(text, recognizer)
    return nes


if __name__ == '__main__':
    news_reader = data_connector.NewsReader('scraped_news')
    news_writer = data_connector.NewsWriter('ner_enriched_news')
    news = news_reader.read_news()
    for news_item in news:
        try:
            news_item.content['nes'] = named_entity_extraction(news_item.content['text'])
            print(news_item.content['nes'])
        except Exception as ex:
            print(str(ex))
            print("No NER was extracted")
        news_writer.write_news(news_item)