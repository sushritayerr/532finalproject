from pyspark.sql import SparkSession
import os

# Initialize a Spark session
spark = SparkSession.builder \
    .config("spark.jars", os.getcwd() + "/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar" + "," +
            os.getcwd() + "/jars/kafka-clients-3.4.1.jar" + "," +
            os.getcwd() + "/jars/commons-pool2-2.11.1.jar" + "," +
            os.getcwd() + "/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar") \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",  # Kafka broker(s)
    "subscribe": "Google_Tweet",              # Kafka topic to subscribe to
    "startingOffsets": "earliest"               # Start reading from the beginning of the topic
}

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Perform processing on the received data
# For example, you can display the Kafka messages
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Start the Spark streaming query
query.awaitTermination()