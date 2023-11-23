----
Setting up Kafka
----

Step 1: Download from here: https://kafka.apache.org/downloads

Step 2: Unzip it

Step 3: cd kafka-3.6.0-src (Take note of your file version)

Step 4: Start Zookeeper:

bin/zookeeper-server-start.sh config/zookeeper.properties

Step 5: Start Kafka broker:

bin/kafka-server-start.sh config/server.properties

Step 6: Create topics: 

In this project folder, cd kafka_utils, python3 create_topic.py

Step 7: Create consumers to read:
python3 test_tweet_ingestion.py --company Microsoft (Replace it with your company name)

Step 7: Write tweets:
python3 write_tweets.py --dataset test_tweets.csv  (Give your dataset name)

Alternatively, you could also use orchestrator.py