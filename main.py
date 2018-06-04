import json
import logging
import sys
import argparse

import tweepy
from kafka import KafkaProducer

from config import read_config

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
        min_msg = {
            'text': status.text,
            'created': str(status.created_at),
            'retweeted_status': status.retweet_count,
            'is_quote_status': status.is_quote_status,
            'quote_count': status.quote_count,
            'reply_count': status.reply_count,
            'retweet_count': status.retweet_count,
            'favorite_count': status.favorite_count,
            'entities': status.entities,
            'favorited': status.favorited,
            'retweeted': status.retweeted,
            'filter_level': status.filter_level,
            'lang': status.lang,
            'timestamp_ms': status.timestamp_ms,
            'user': {
                'id': status.user.id,
                'name': status.user.name,
                'screen_name': status.user.screen_name
            }
        }
        msg = json.dumps(min_msg)
        self.kafka_producer.send_message(self.kafka_topic, msg)
        #logging.info(msg)


    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

def main(kafka_topic, twitter_conf, track_items=[], kafka_host="localhost", kafka_port=9092):
    auth = tweepy.OAuthHandler(twitter_conf['app_key'], twitter_conf['app_secret'])
    auth.set_access_token(twitter_conf['access_token'], twitter_conf['access_secret'])
    api = tweepy.API(auth)
    kafka_sender = KafkaSender()
    stream_listener = StreamListener(kafka=kafka_sender, kafka_topic=kafka_topic)
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=track_items)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    configs = read_config('twitterapp.cfg')
    parser.add_argument("-t", "--topic", help="Kafka topic output stream", required=True)
    parser.add_argument("-i", "--track_items", action='append', help="List track twitter items", required=True)
    args = parser.parse_args()
    main(args.topic, configs['TwitterAPI'], track_items=args.track_items)
