from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import argparse


def main(topic_name, slide_interval=30, micro_secs_batch=10, output_file=None, checkpoint_file='/tmp/spark_twitter'):
    sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, micro_secs_batch)
    ssc.checkpoint(checkpoint_file)
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {topic_name:1})

    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    if output_file:
        (parsed.map(lambda x: json.dumps(x))).saveAsTextFiles(output_file)
    parsed.window(slide_interval, micro_secs_batch).count().map(lambda x:'Tweets count: %s' % x).pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--topic", help="Kafka topic output stream", required=True)
    parser.add_argument("-m", "--micro-batch", help="Micro batch in seconds. Default: 10 secs", type=int, default=10)
    parser.add_argument("-w", "--sliding-window", help="Sliding window in seconds from where to count. Default: 30 secs", type=int, default=30)
    parser.add_argument("-j", "--json-file", help="Output JSON file where to put the received tweets", default=None)
    args = parser.parse_args()
    main(args.topic, slide_interval=args.sliding_window, micro_secs_batch=args.micro_batch, output_file=args.json_file)
    main(**args)
