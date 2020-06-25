# twitterapp
This repository shows a Consumer Producer architecture using Kafka and Spark on Python language.
In this case, the Twitter Streaming API is applied to extract most recent tweets, store them into a queue and consume them later.

It's composed by one producer:
* **main.py**: extracts tweets in real-time and load them as JSON messages into a Kafka topic.

And some PySpark consumers:
* **sentimental_analysis.py**: consumes tweets from a Kafka topic and enriches them with Sentiment Analysis MeaningCloud API. The tweets are JSON format messages.

* **pyspark_counters.py**: consumes tweets from a Kafka topic and outputs counters on jSON files.


## Installation
### Prerequisites
To install and execute this project, the computer or virtualenv must have the following prerequisites:
* Installed Python 2.7 version or higher
* Installed Spark 2.3 version or higher
* Kafka installed and running on localhost
* Created a [registered Twitter app](https://developer.twitter.com/en/docs/labs/filtered-stream/quick-start) with its credentials
* A valid [MeaningCloud API Key](https://www.meaningcloud.com/developer/create-account), for running sentimental analysis consumer


### Steps
Inside code directory, install the required Python libraries using:
```bash
pip install -r requirements.txt
```


## Usage

### Executing Producer
To run the Python Producer, first create a `hj` file like below First execute the Producer Program:

```
[Kafka]
host = localhost
port = 9092

[TwitterAPI]
access_token = <access-token>
access_secret = <access-secret>
app_key = <twitter-app-key>
app_secret = <twitter-app-secret>
track_terms = <hashtags>
```

Then execute the producer indicating the output kafka topic and the number of tweets to retain per call, like for example:

```bash
cd twitterapp
python main.py -t test -i 10
```

### Executing Consumers
After that, execute one (or both) of the consumers:

**Pyspark Counters Consumer**
First create a config file called `counter.cfg`:
```
[CounterApp]
topic = <topic-source-name>
app_name = SparkCounter
slide_interval = 30
micro_secs_batch = 10
output_file = output_json
checkpoint_file = '/tmp/spark_twitter'
zk_host = localhost
zk_port = 2181
```
Then submit a Spark job using pyspark_counter.py and counter.cfg file in the following way:
```bash
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark_counter.py --config-file counter.cfg
```

**Sentiment Analysis Consumer**
Submit a Spark job using sentimental_analysis.py and counter.cfg file in the following way:

```bash
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 sentimental_analysis.py -t <topic-input>
```


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.


## License
[MIT](https://choosealicense.com/licenses/mit/)
