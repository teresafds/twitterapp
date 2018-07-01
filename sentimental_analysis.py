import json
import argparse
import requests

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils


ENABLED_LANGUAGES = ['en', 'es', 'fr', 'it', 'pt', 'ca']


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


def get_sent_analysis(text, language):
    if language not in (ENABLED_LANGUAGES):
        return json.dumps({})
    url_mc = 'http://api.meaningcloud.com/sentiment-2.1?'
    mc_key = ''
    payload = "key=%s&lang=%s&of=json&txt=%s" % (mc_key, language, text)
    response = requests.request("POST", url_mc+payload)
    sentimental_analysis = json.loads(response.content.decode('utf-8'))
    return json.dumps({
        'score_tag': sentimental_analysis.get('score_tag'),
        'agreement': sentimental_analysis.get('agreement'),
        'subjectivity': sentimental_analysis.get('subjectivity'),
        'confidence': sentimental_analysis.get('confidence'),
        'irony': sentimental_analysis.get('irony')
    })


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
    kafkaStream = KafkaUtils.createStream(ssc, "{zk_host}:{zk_port}".format(zk_host=zk_host, zk_port=zk_port), 'spark-streaming', {input_topic_name:1})
    tweets = kafkaStream.map(lambda v: json.loads(v[1]))

    # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print("========= %s =========" % str(time))
        print(rdd.count())
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())
            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda tweet: Row(**tweet))
            spark.udf.register('sentiment_analysis', get_sent_analysis)
            tweetsDataFrame = spark.createDataFrame(rowRdd)
            # Creates a temporary view using the DataFrame.
            tweetsDataFrame.createOrReplaceTempView("tweets")
            # Do word count on table using SQL and print it
            tweetsCountsDataFrame = \
                spark.sql("""
                    SELECT
                        current_timestamp(),
                        created_at AS created,
                        favorite_count,
                        favorited,
                        is_quote_status,
                        lang,
                        quote_count,
                        reply_count,
                        retweet_count,
                        retweeted,
                        user,
                        sentiment_analysis(text, lang) AS sentimental_analysis
                    FROM tweets
                    LIMIT 10
                """)
            tweetsCountsDataFrame.show()
            #tweetsDataFrame.printSchema()
        except Exception as e:
            print(e.message)
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
