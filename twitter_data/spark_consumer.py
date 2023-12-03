from pyspark.sql import SparkSession
import os
import json
from transformers import pipeline

# Initialize an empty list to collect JSON objects
output_data = {"google": {"pos": 0, "neg": 0, "neutral": 0}, "verizon": {"pos": 0, "neg": 0, "neutral": 0},
               "microsoft": {"pos": 0, "neg": 0, "neutral": 0}, "nvidia": {"pos": 0, "neg": 0, "neutral": 0},
               "facebook": {"pos": 0, "neg": 0, "neutral": 0}}


def subscribe_event():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getcwd() + "/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar" + "," +
                os.getcwd() + "/jars/kafka-clients-3.4.1.jar" + "," +
                os.getcwd() + "/jars/commons-pool2-2.11.1.jar" + "," +
                os.getcwd() + "/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar") \
        .appName("TwitterSentimentAnalysis") \
        .getOrCreate()

    # Define the Kafka parameters
    kafka_params = {
        "kafka.bootstrap.servers": "localhost:9092",  # Kafka broker(s)
        "subscribe": "Google, Verizon, Microsoft, Nvidia, Facebook",  # Kafka topic to subscribe to
        "startingOffsets": "earliest"  # Start reading from the beginning of the topic
    }

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
    query.awaitTermination()
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
    update_dict(company_name, result)


def update_dict(company_name, result):
    company = output_data[company_name.lower()]
    if result == "positive":
        company["pos"] = company["pos"] + 1
    elif result == "negative":
        company["neg"] = company["neg"] + 1
    if result == "neutral":
        company["neutral"] = company["neutral"] + 1
    print(output_data)
