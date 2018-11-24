from time import sleep
import pydeepl
import deepl
from kafka import KafkaConsumer, KafkaProducer
from bson.json_util import loads, dumps

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = key.encode("utf-8")
        value_bytes = dumps(value).encode("utf-8")
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully: ' + key)
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def splitCount(s, count):
    return [''.join(x) for x in zip(*[list(s[z::count]) for z in range(count)])]


if __name__ == '__main__':
    print('Started translation')
    translated_records = []
    consume_topic_name = 'scraped_news'
    produce_topic_name = 'translated_news'

    consumer = KafkaConsumer(consume_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    producer = connect_kafka_producer()

    from_language = 'EN'
    to_language = 'DE'

    for msg in consumer:
        record = loads(msg.value)
        try:
            record["description"]=pydeepl.translate(record["description"], to_language, from_lang=from_language)

            record["title"]=pydeepl.translate(record["title"], to_language, from_lang=from_language)
            translated_keywords=[]
            for keyword in record["keywords"]:
                translated_keyword=pydeepl.translate(keyword, to_language, from_lang=from_language)
                translated_keywords.append(translated_keyword)
            record["keywords"]=translated_keywords
            text_pieces=splitCount(record["text"],4762)
            translated_text=""
            for text in text_pieces:
                translated_text_part= pydeepl.translate(text, to_language, from_lang=from_language)
                translated_text=translated_text.join(translated_text_part)
            record["text"]=translated_text
            print("Translation succesful")
            sleep(10)
            publish_message(producer, produce_topic_name, record["url"], record)
        except Exception as ex:
            print('Translation failed')
            print(str(ex))

    print("Finished")