import json
import logging
import sys
import argparse

import tweepy

from config import read_config
from kafka_script import KafkaSender

logger = logging.getLogger('twitterapp')
logger.setLevel(logging.INFO)


class StreamListener(tweepy.StreamListener):

    def __init__(self, api=None, **kwargs):
        super(StreamListener, self).__init__(api)
        self.kafka_producer = kwargs.get('kafka')
        self.kafka_topic = kwargs.get('kafka_topic')

    def on_status(self, status):
        fields = ['text', 'created_at', 'is_quote_status', 'quote_count',
         'reply_count', 'retweet_count', 'favorite_count', 'entities', 'favorited',
         'retweeted', 'filter_level', 'lang', 'timestamp_ms', 'user']
        map_fields = {
            'created_at': lambda x: str(x),
            'user': lambda x: {'id': x.id, 'name': x.name, 'screen_name': x.screen_name}
        }
        min_msg = { f: getattr(status, f) for f in fields}
        for m, f in map_fields.items():
            if min_msg.get(m):
                min_msg[m] = f(min_msg[m])
        msg = json.dumps(min_msg)
        self.kafka_producer.send_message(self.kafka_topic, msg)
        logger.info(msg)


    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False


def main(topic, host, port, app_key, app_secret, access_token, access_secret, track_items=[]):
    auth = tweepy.OAuthHandler(app_key, app_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    kafka_sender = KafkaSender(host=host, port=port)
    stream_listener = StreamListener(kafka=kafka_sender, kafka_topic=topic)
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=track_items)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    configs = read_config('twitterapp.cfg')
    parser.add_argument("-t", "--topic", help="Kafka topic output stream", required=True)
    parser.add_argument("-i", "--track_items", action='append', help="List track twitter items", required=True)
    args = parser.parse_args()
    twitter_conf = configs['TwitterAPI']
    kafka_conf = configs['Kafka']
    main(
        args.topic,
        kafka_conf['host'],
        int(kafka_conf['port']),
        twitter_conf['app_key'],
        twitter_conf['app_secret'],
        twitter_conf['access_token'],
        twitter_conf['access_secret'],
        track_items=args.track_items
    )
