from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import argparse

from config import read_config


def main(topic_name, app_name, zk_host='localhost', zk_port=2181, slide_interval=30, micro_secs_batch=10, output_file=None, checkpoint_file='/tmp/spark_twitter', **kwargs):
    sc = SparkContext(appName=app_name)
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, micro_secs_batch)
    ssc.checkpoint(checkpoint_file)
    kafkaStream = KafkaUtils.createStream(ssc, '{host}:{port}'.format(host=zk_host, port=zk_port), 'spark-streaming', {topic_name:1})

    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    if output_file:
        (parsed.map(lambda x: json.dumps(x))).saveAsTextFiles(output_file)
    parsed.window(slide_interval, micro_secs_batch).count().map(lambda x: 'Tweets count: %s' % x).pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-file", help="Config File")
    parser.add_argument("-t", "--topic", help="Kafka topic output stream")
    parser.add_argument("-a", "--app-name", help="Spark Streaming App Name", default='PythonSparkCounter')
    parser.add_argument("-m", "--micro-batch", help="Micro batch in seconds. Default: 10 secs", type=int, default=10)
    parser.add_argument("-w", "--slide-interval", help="Sliding window in seconds from where to count. Default: 30 secs", type=int, default=30)
    parser.add_argument("-j", "--output-file", help="Output JSON file where to put the received tweets", default=None)
    parser.add_argument("-zk", "--zk-host", help="Output JSON file where to put the received tweets", default='localhost')
    parser.add_argument("-zkp", "--zk-port", help="Output JSON file where to put the received tweets", default=2181)
    args = parser.parse_args()
    config_counter = read_config(args.config_file).get('CounterApp')
    topic = args.topic or config_counter.get('topic')
    app_name = args.app_name or config_counter.get('app_name')
    other_options = {k: v or config_counter.get(k) for k,v in args.__dict__.iteritems() if k not in ['topic', 'app_name']}
    main(topic, app_name, **other_options)
