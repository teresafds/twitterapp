from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import argparse

def main(topic_name, slide_interval=30, micro_secs_batch=10):
    sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, micro_secs_batch)
    ssc.checkpoint("/tmp/spark_twitter")
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {topic_name:1})

    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    parsed.countByWindow(micro_secs_batch, slide_interval).map(lambda x:'Tweets in this batch: %s' % x).pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--topic", help="Kafka topic output stream", required=True)
    args = parser.parse_args()
    main(args.topic)
