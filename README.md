532finalproject
---
 Dataset - https://huggingface.co/datasets/yogiyulianto/twitter-sentiment-dataset-en/tree/main

 Sentiment analysis model - https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest
 
 **Steps required to run the project:**

    Start Zookeeper -
   
    ./bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
    
    Start Kafka -
   
    ./bin/windows/kafka-server-start.bat ./config/server.properties
    
    Execute orchestrator
   
    python orchestrator.py 

 **Step by step execution in project:**

    - filtering the dataset
    - preprocessing tweets
    - writing to kafka topics
    - subscribing from kafka topics
    - sending tweets to sentiment analysis model

---

