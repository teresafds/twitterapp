import json
import argparse
import requests

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StringType


def getSparkSessionInstance(sparkConf):
    """
    Singleton Instance of sparksession, in order to use the same one and another time.
    """
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']



def get_sent_analysis(twitter):
    url_mc = 'http://api.meaningcloud.com/sentiment-2.1?'
    mc_key = ''
    payload = "key=%s&lang=%s&of=json&txt=%s" % (mc_key, twitter['lang'], twitter['text'])
    #response = requests.request("POST", url_mc+payload)
    #sentimental_analysis = json.loads(response.content.decode('utf-8'))
    return json.dumps({
        'sentimental_analysis': {
            'score_tag': 10,
            'agreement': 'P+',
            'subjectivity': 'OBJECTIVE',
            'confidence': 100,
            'irony': 'NON-IRONY'
        }
    })
    """return json.dumps({
        'sentimental_analysis': {
            'score_tag': sentimental_analysis.get('score_tag'),
            'agreement': sentimental_analysis.get('agreement'),
            'subjectivity': sentimental_analysis.get('subjectivity'),
            'confidence': sentimental_analysis.get('confidence'),
            'irony': sentimental_analysis.get('irony')
        }
    })"""


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
    spark_session = getSparkSessionInstance(sc.getConf())
    spark_session.udf.register('sentiment_analysis', get_sent_analysis)
    ssc.checkpoint(checkpoint_file)
    kafkaStream = KafkaUtils.createStream(ssc, "{zk_host}:{zk_port}".format(zk_host=zk_host, zk_port=zk_port), 'spark-streaming', {input_topic_name:1})

    tweets = kafkaStream.map(lambda v: json.loads(v[1]))

    # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda tweet: Row(**tweet))
            #spark.udf.register('sentiment_analysis', get_sent_analysis, StringType)
            tweetsDataFrame = spark.createDataFrame(rowRdd)

            # Creates a temporary view using the DataFrame.
            tweetsDataFrame.createOrReplaceTempView("tweets")

            # Do word count on table using SQL and print it
            tweetsCountsDataFrame = \
                spark.sql("select sentiment_analysis(text) from tweets limit 10")
            tweetsCountsDataFrame.show()
        except:
            pass
    tweets.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    meaning_cloud_key = ''
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--topic", help="Kafka topic output stream", required=True)
    parser.add_argument("-m", "--micro-batch", help="Micro batch in seconds. Default: 10 secs", type=int, default=10)
    parser.add_argument("-j", "--json-file", help="Output JSON file where to put the received tweets", default=None)
    args = parser.parse_args()
    main(args.topic, meaning_cloud_key, micro_secs_batch=args.micro_batch, output_file=args.json_file)
