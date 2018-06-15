from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import argparse

def main(topic_name):
    sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 10)

    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitterApp':1})

    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--topic", help="Kafka topic output stream", required=True)
    args = parser.parse_args()
    main(args.topic)
