# import sys
# import os
# from twitter_data import filter_dataset
# from kafka_utils import write_tweets
# from kafka_utils import test_tweet_ingestion
# from kafka_utils import create_topic
#
# sys.path.append('.')
#
# create_topic.create_company_topics()
#
# file_path = os.path.join('twitter_data', 'twitter_training.csv')
# filter_dataset.clean_dataset(file_path)
#
# companies_for_analysis = ["Verizon", "Microsoft", "Google", "Nvidia", "Facebook"]
# for company in companies_for_analysis:
#     test_tweet_ingestion.read_tweets(company)
#
# write_tweets.write_tweets('./twitter_data/filtered_twitter_training.csv')