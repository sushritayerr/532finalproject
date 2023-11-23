from kafka import KafkaProducer, KafkaConsumer
import argparse

def read_tweets(company):
    consumer = KafkaConsumer(company, bootstrap_servers='localhost:9092', group_id='my-group')

    print(f'Reading {company} tweets')
    for message in consumer:
        print(f"Received message: {message.value.decode('utf-8')}")
    consumer.close()
def main():
    parser = argparse.ArgumentParser(description='Read tweets from a Kafka topic')
    parser.add_argument('--company', type=str, required=True, help='Name of the company for which to read tweets')
    args = parser.parse_args()
    read_tweets(args.company)

if __name__ == "__main__":
    main()