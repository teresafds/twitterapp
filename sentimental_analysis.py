from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import argparse


def enrich_twitter_mc(mc_key, twitter):
    url_mc = "http://api.meaningcloud.com/sentiment-2.1?"
    payload = "key=%s&lang=%s&of=json&txt=%s" % (mc_key, tweet_lang, tweet['text'].encode('utf-8'))
    response = requests.request("POST", url_mc+payload)
    #date_object = datetime.strptime(tweet['created_at'][:20]+tweet['created_at'][25:], '%a %b %d %H:%M:%S %Y')
    tweet['created_at'] = datetime.strftime(date_object, '%Y-%m-%dT%H:%M:%S')
    tweet['sent-analysis'] = json.loads(response.content.decode('utf-8'))


def main(
        input_topic_name,
        mc_key,
        micro_secs_batch=10,
        output_file=None,
        checkpoint_file='/tmp/sentimental_analysis',
        zk_host='localhost',
        zk_port=2181,
    ):
    sc = SparkContext(appName="StreamingSentimentAnalysis")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, micro_secs_batch)
    ssc.checkpoint(checkpoint_file)
    kafkaStream = KafkaUtils.createStream(ssc, f"{zk_host}:{zk_port}", 'spark-streaming', {input_topic_name:1})

    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    enriched_tweets = parsed.map(lambda v: enrich_twitter_mc(mc_key, v))
    if output_file:
        (parsed.map(lambda x: json.dumps(x))).saveAsTextFiles(output_file)
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--topic", help="Kafka topic output stream", required=True)
    parser.add_argument("-m", "--micro-batch", help="Micro batch in seconds. Default: 10 secs", type=int, default=10)
    parser.add_argument("-j", "--json-file", help="Output JSON file where to put the received tweets", default=None)
    args = parser.parse_args()
    main(args.topic, micro_secs_batch=args.micro_batch, output_file=args.json_file)
