import sys
import os
from twitter_data import filter_dataset
from kafka_utils import write_tweets
from kafka_utils import test_tweet_ingestion
from kafka_utils import create_topic
from twitter_data.spark_consumer import subscribe_event

sys.path.append('.')

create_topic.create_company_topics()

# filter dataset
filter_dataset.clean_dataset("twitter_training.csv")

companies_for_analysis = ["Verizon", "Microsoft", "Google", "Nvidia", "Facebook"]
# for company in companies_for_analysis:
#     test_tweet_ingestion.read_tweets(company)

# writing tweets and publishing event
write_tweets.write_tweets('filtered_twitter_training.csv')

# subscribing event using spark consumer
subscribe_event()

