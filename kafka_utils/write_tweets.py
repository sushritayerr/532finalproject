from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import json
import argparse

def write_tweets(ds):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    dataset = pd.read_csv(ds)
    columns = ['company', 'tweet', 'sentiment']
    for index, row in dataset.iterrows():
        company_name = row['company']
        tweet = row['tweet']
        sentiment = row['sentiment']

        data = {
            'tweet': tweet,
            'company': company_name
        }
        serialized_data = json.dumps(data).encode('utf-8')
        producer.send(company_name, value=serialized_data)

    producer.close()

def main():
    parser = argparse.ArgumentParser(description='Read tweets from a Kafka topic')
    parser.add_argument('--dataset', type=str, required=True, help='Name of the company for which to read tweets')
    args = parser.parse_args()
    write_tweets(args.dataset)

if __name__ == "__main__":
    main()

