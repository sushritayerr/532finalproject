from pyspark.sql import SparkSession
import os
import json
from transformers import pipeline
# from transformers import BertTokenizer, TFBertForSequenceClassification
# from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import StringType
import tensorflow as tf

# Initialize an empty list to collect JSON objects
output_data = []

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
    print('schema is')
    df.printSchema()
    print('schema end')
    # Perform processing on the received data
    # For example, you can display the Kafka messages
    query = df.selectExpr("CAST(value AS STRING)").writeStream \
        .outputMode("append") \
        .foreach(process_row) \
        .start()

    ##test
    # analyze_sentiment_udf = udf(analyze_sentiment, StringType())
    # analyzed_tweets_df = df.withColumn("sentiment", analyze_sentiment_udf("tweet"))


    ##test
    # Start the Spark streaming query
    print('output_data is ', output_data)
    query.awaitTermination()
    query.stop()

# def analyze_sentiment(tweet_text):
#     sentiment_analysis = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")
#     result = sentiment_analysis(tweet_text)
#     return result[0]['label']

def process_row(row):
    print('row is ', row)
    value_str = row["value"]
    json_obj = json.loads(value_str)
    tweet_content = json_obj.get("tweet", "")
    company_name = json_obj.get("company", "")

    sentiment_analysis = pipeline("sentiment-analysis")
    result = sentiment_analysis(tweet_content)

    # tokenizer = BertTokenizer.from_pretrained("nlptown/bert-base-multilingual-uncased-sentiment")
    # model = TFBertForSequenceClassification.from_pretrained("nlptown/bert-base-multilingual-uncased-sentiment")

    # Example of using the model for sentiment analysis
    # inputs = tokenizer(tweet_content, return_tensors="tf")
    # outputs = model(sentiment_analysis)
    output_data.append(company_name + ' ' + result)

subscribe_event()
