from pyspark.sql import SparkSession
import sys
import os
from pyspark.accumulators import AccumulatorParam
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
import json
from transformers import pipeline

#Accumulator to aggregate the output of all the tasks
class SentimentAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return {"pos": 0, "neg": 0, "neutral": 0}

    def addInPlace(self, v1, v2):
        v1["pos"] += v2["pos"]
        v1["neg"] += v2["neg"]
        v1["neutral"] += v2["neutral"]
        return v1
#Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",  # Kafka broker(s)
    "subscribe": "Google, Verizon, Microsoft, Nvidia, Facebook",  # Kafka topics to subscribe to.
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
    #Read the streamed data from kafka
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_params) \
        .load()

    # Process the tweets using the custom defined method, that performs the sentiment analysis and saves the output using the accumulator
    query = df.selectExpr("CAST(value AS STRING)").writeStream \
        .outputMode("append") \
        .foreach(process_row) \
        .start()

    #Waits for 45minutes to complete the execution and then timesout after. Change this value if the number of cores are being increased.
    query.awaitTermination(timeout=2800)
    query.stop()

#Process the row to read the necessary content, perform sentiment analysis and update the results using the accumulator 
def process_row(row):
    value_str = row["value"]
    json_obj = json.loads(value_str)
    tweet_content = json_obj.get("tweet", "")
    company_name = json_obj.get("company", "")
    sentiment_analysis = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
    # print(tweet_content)
    result = sentiment_analysis(tweet_content)[0]['label']
    update_accumulator(company_name, result)

#Update the result
def update_accumulator(company_name, result):
    global sentiment_accumulator

    # Update the aggregated result
    sentiment_accumulator.add({"pos": 1 if result == "positive" else 0,
                               "neg": 1 if result == "negative" else 0,
                               "neutral": 1 if result == "neutral" else 0})

start_time = time.time()
subscribe_event()
end_time = time.time()
#calculating the duration for the processing
duration = end_time - start_time
final_value = sentiment_accumulator.value
#Printing the output of the process
print("Final Sentiment Analysis Result:", final_value)
print('Duration is ', duration)

#calculating the final sentiment of the stock
for company, sentiments in final_value.items():
    max_sentiment = max(sentiments, key=sentiments.get)

    if max_sentiment == 'pos':
        summary = f"{company}'s stock is likely to go up in value because the sentiment is positive."
    elif max_sentiment == 'neg':
        summary = f"{company}'s stock is likely to go down in value because the sentiment is negative."
    else:
        summary = f"{company}'s stock sentiment is neutral."

    print(summary)
spark.stop()
