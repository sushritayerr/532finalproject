from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import string


custom_stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren", "couldn", "didn", "doesn", "hadn", "hasn", "haven", "isn", "ma", "mightn", "mustn", "needn", "shan", "shouldn", "wasn", "weren", "won", "wouldn"]

def preprocess_text(text):
    
    text = text.lower()
    
    text = ''.join([char for char in text if char not in string.punctuation])
    
    text = ' '.join([word for word in text.split() if word not in custom_stopwords])
    return text

def update_tweets(topic, bootstrap_servers='localhost:9092'):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest', group_id='update_tweets_group')

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    for message in consumer:
        tweet = message.value.decode('utf-8')
        updated_tweet = preprocess_text(tweet)

        
        updated_topic = f"{topic}_updated"
        producer.send(updated_topic, updated_tweet.encode('utf-8'))
        print(f"Original Tweet ({topic}): {tweet}")
        print(f"Updated Tweet ({updated_topic}): {updated_tweet}")
        print("-----")

if __name__ == "__main__":
    
    companies = ["Verizon", "Microsoft", "Google", "Nvidia", "Facebook"]

    for company in companies:
        
        kafka_topic = company
        update_tweets(kafka_topic)