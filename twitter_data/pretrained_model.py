from transformers import pipeline
import pandas as pd

data = pd.read_csv("filtered_twitter_training.csv")
data.head()
for d, s in zip(data['tweet'], data['sentiment']):
    sentiment_analysis = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")
    result = sentiment_analysis(d)
    print(d)
    print(result, s)

