import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from twitter_data import filter_dataset
from kafka_utils import write_tweets
from kafka_utils import create_topic

sys.path.append('.')

create_topic.create_company_topics()
# filter dataset
filter_dataset.clean_dataset("twitter_data/twitter_training.csv")

companies_for_analysis = ["Verizon", "Microsoft", "Google", "Nvidia", "Facebook"]
# 
# writing tweets and publishing event
write_tweets.write_tweets('filtered_twitter_training.csv')

# subscribing event using spark consumer
# subscribe_event()

