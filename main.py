import settings
import tweepy
import json
import logging
import sys

from kafka import KafkaProducer


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

class KafkaSender(object):

    def __init__(self, host='localhost', port=9092):
        self.producer = KafkaProducer(bootstrap_servers=f"{host}:{port}")

    def send_message(self, topic, message):
        self.producer.send(topic, message.encode('utf-8'))

    def close(self):
        self.producer.close()


class StreamListener(tweepy.StreamListener):

    def __init__(self, api=None, **kwargs):
        super(StreamListener, self).__init__(api)
        self.kafka_producer = kwargs.get('kafka')
        self.kafka_topic = kwargs.get('kafka_topic')

    def on_status(self, status):
        msg = json.dumps(status._json)
        self.kafka_producer.send_message(self.kafka_topic, msg)
        #logging.info(msg)


    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False


if __name__ == '__main__':
    auth = tweepy.OAuthHandler(settings.TWITTER_APP_KEY, settings.TWITTER_APP_SECRET)
    auth.set_access_token(settings.ACCESS_TOKEN, settings.ACCESS_SECRET)
    api = tweepy.API(auth)
    kafka_sender = KafkaSender()
    kafka_topic = 'twitterApp'
    stream_listener = StreamListener(kafka=kafka_sender, kafka_topic=kafka_topic)
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=[settings.TRACK_TERMS])
