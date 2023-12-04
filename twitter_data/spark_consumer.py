from pyspark.sql import SparkSession
import sys
import os
from pyspark.accumulators import AccumulatorParam
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
import json
from transformers import pipeline

# Initialize an empty list to collect JSON objects
output_data = {"google": {"pos": 0, "neg": 0, "neutral": 0}, "verizon": {"pos": 0, "neg": 0, "neutral": 0},
               "microsoft": {"pos": 0, "neg": 0, "neutral": 0}, "nvidia": {"pos": 0, "neg": 0, "neutral": 0},
               "facebook": {"pos": 0, "neg": 0, "neutral": 0}}

class SentimentAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return {"pos": 0, "neg": 0, "neutral": 0}

    def addInPlace(self, v1, v2):
        v1["pos"] += v2["pos"]
        v1["neg"] += v2["neg"]
        v1["neutral"] += v2["neutral"]
        return v1
    # Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",  # Kafka broker(s)
    "subscribe": "Google1",  # Kafka topic to subscribe to
    "startingOffsets": "earliest"  # Start reading from the beginning of the topic
}

spark = SparkSession.builder \
        .config("spark.jars", os.getcwd() + "/twitter_data/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar" + "," +
                os.getcwd() + "/twitter_data/jars/kafka-clients-3.4.1.jar" + "," +
                os.getcwd() + "/twitter_data/jars/commons-pool2-2.11.1.jar" + "," +
                os.getcwd() + "/twitter_data/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar") \
        .appName("TwitterSentimentAnalysis") \
        .getOrCreate()

sentiment_accumulator = spark.sparkContext.accumulator({"pos": 0, "neg": 0, "neutral": 0}, SentimentAccumulatorParam())

def subscribe_event():
    # Initialize a Spark session
    
    # Read data from Kafka
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_params) \
        .load()

    # Perform processing on the received data
    # For example, you can display the Kafka messages
    query = df.selectExpr("CAST(value AS STRING)").writeStream \
        .outputMode("append") \
        .foreach(process_row) \
        .start()

    # Start the Spark streaming query
    query.awaitTermination(timeout=60)
    query.stop()


def process_row(row):
    value_str = row["value"]
    json_obj = json.loads(value_str)
    tweet_content = json_obj.get("tweet", "")
    company_name = json_obj.get("company", "")
    # pretrained sentiment analysis model - https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest
    sentiment_analysis = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
    print(tweet_content)
    result = sentiment_analysis(tweet_content)[0]['label']
    update_accumulator(company_name, result)


def update_accumulator(company_name, result):
    global sentiment_accumulator

    # Update the accumulator
    sentiment_accumulator.add({"pos": 1 if result == "positive" else 0,
                               "neg": 1 if result == "negative" else 0,
                               "neutral": 1 if result == "neutral" else 0})

    # Print the updated accumulator
    # print(sentiment_accumulator.value)
start_time = time.time()
subscribe_event()
end_time = time.time()
duration = end_time - start_time
final_value = sentiment_accumulator.value
print("Final Sentiment Analysis Result:", final_value)
print('Duration is ', duration)
spark.stop()
