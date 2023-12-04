from transformers import pipeline
import pandas as pd

data = pd.read_csv("filtered_twitter_training.csv")
# data = data.head(400)

output_data = {"google": {"pos": 0, "neg": 0, "neutral": 0}, "verizon": {"pos": 0, "neg": 0, "neutral": 0},
               "microsoft": {"pos": 0, "neg": 0, "neutral": 0}, "nvidia": {"pos": 0, "neg": 0, "neutral": 0},
               "facebook": {"pos": 0, "neg": 0, "neutral": 0}}
sentiment_analysis = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")

n = 0
correct_predictions = 0
#Predicts the sentiment and aggregates the data without spark and any parallelism
for d, s in zip(data['tweet'], data['company']):
    n = n + 1
    print("################## ", n)
    print(d)
    if type(d) == str:
        result = sentiment_analysis(d)
        print(result, s)
        predicted_sentiment = result[0]['label']
        company = output_data[s.lower()]
        if predicted_sentiment == "positive":
            company["pos"] = company["pos"] + 1
        elif predicted_sentiment == "negative":
            company["neg"] = company["neg"] + 1
        if predicted_sentiment == "neutral":
            company["neutral"] = company["neutral"] + 1
        # output_data.update(company)
        print(output_data)




