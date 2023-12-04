# Real-time Sentiment Analysis of Business Reviews using Kafka and PySpark

## Overview

The goal of this project is to use PySpark to perform sentiment analysis from tweets obtained from twitter-sentiment-dataset-en dataset, focusing on sentiments related to five major companies: Verizon, Microsoft, Google, NVIDIA, and META. The analysis will then be used to predict potential fluctuations in the stock values of these companies. The high-level design of this will involve: gathering Twitter data, using a streaming pipeline to organize and stream tweets, and integrating a sentiment analysis model to process and classify tweets as positive, negative or neutral. The tools that will be used consist of Kafka Producer to organize and stream tweets, PySpark to read and process the tweets in a distributed fashion, Python for the orchestration. The expected outcome of this project is to perform sentiment analysis for each company, providing insights into public perception and predicting fluctuations in the stock value. The significance of this project is that it can provide valuable insight to stakeholders such as investors, traders, financial analysts, and the general public in their decision-making processes. 

## Objectives achieved

#### Producer:

1. Setup kafka to store data in a distributed setup for efficient parallel processing.

#### Consumer:

1. Perform sentiment analysis on the stream of tweets.
2. Aggregate the sentiments for each tweet to predict the overrall sentiment and thereby the movement of stock.

#### Overall:

1. Compare performance differences between distributed setup vs non-distributed setup.
2. Compare performance of the application changing the number of cores and amount of memory being allocated for the spark instance. The results of these are present in the Plotting.Pynb file.

## Methodology

The project will be implemented using the following methodology:
1. **Streaming Pipeline:** 

Simulate streaming of tweets about companies in real-time using Kafka.

2. **Model Building:**

    * Experiment with pretrained sentiment analysis models to identify the most suitable one for the twitter dataset.
    * Here, the focus is not on the model performance but on studying the impact of distributed processing.
    * From our preliminary testing, we found that the model:twitter-roberta-base-sentiment-latest performed the best
      Model: https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest

3. **Workflow:**

    * Process the dataset to extract the data for the companies in consideration and remove unrequired data.

    * Clean the tweets, and preprocess them to perform prediction.

    * Perform sentiment analysis on these reviews using the chosen sentiment analysis model and pyspark.

    * Aggregate the sentiment predictions and display them on the console.

4. **Dataset source:**

    * https://huggingface.co/datasets/yogiyulianto/twitter-sentiment-dataset-en/tree/main


## TOOLS AND TECHNOLOGIES

The following tools and technologies will be used in this project:

1. **PySpark:** for distributed processing of data
2. **Apache Kafka:** to simulate real-time streaming of data
3. **Python:** for programming the project
4. **CSV / MySQL:** for storing the analyzed data

**Steps required to run the project:**

    Start Zookeeper -
   
    ./bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
    
    Start Kafka -
   
    ./bin/windows/kafka-server-start.bat ./config/server.properties
    
    Execute orchestrator.py
   
    python twitter_data/orchestrator.py # this filters the data, creates the topics, publishes the data

    Execute Spark Consumer

    python twitter_data/spark_consumer.py #This reads the tweets from the kafka topics, processes them, aggregates them and displays the results.This is also used for measuring the duration of the processing

 **Step by step execution in project:**

    - filtering the dataset - orchestrator
    - preprocessing tweets - orchestrator
    - writing to kafka topics - orchestrator
    - subscribing from kafka topics - spark consumer
    - sending tweets to sentiment analysis model - spark consumer
    - Aggregating the results and printing them - spark consumer

   ## Experiments
   
   *** Experiment 1:
   Studying the performance using a vanilla processing of the entire dataset(pretrained_model.py) and using a distributed setup, using Spark and Kafka. We observed(as expected) that the distributed processing was faster than the vanilla processing.

   *** Experiment 2:
   Studying the performance of the system by varying the number of cores at disposal of Spark for distributed processing. We observed that, as the number of cores are increased the throughput of the system increased due to the increased parallelism. As a result, overall, the duration for processing decreased with increase in the number of cores.

   *** Experiment 3:
   Studying the performance of the system by varying the memory available to the spark instance. We observed that increasing the memory improved the performance slightly but not in correlation with increasing the number of cores.

   ## Test cases

   *** Correctness test:
   We have verified the correctness of the system using the pretrained_model.py to compare the output and to verify if the aggregation is correct.

   *** Integration test:
   ** Verifying Kafka:
   We have verified the tweets reading, filtering and writing to the kafka topics using a custom kafka consumer script(test_tweet_ingestion.py) that verified the tweets from the custom dataset we have prepared to validate.

   ** Verifying Spark + Kafka Integration
   We have used the same kafka consumer script to compare the spark integration and validate if the data is being read correctly.

   ** Complete test
   Using the custom dataset, we have validated if the final aggregated results are matching the input provided. The dataset includes tweets that dont belong to the companies in question, different preprocessing requirements and tweets of different sentiments.

