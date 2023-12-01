import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
import nltk
# nltk.download('averaged_perceptron_tagger')
# nltk.download('maxent_ne_chunker')
# nltk.download('all')


count = CountVectorizer()
#
data = pd.read_csv("filtered_twitter_training.csv")
data.head()
for d in data['tweet']:
    tokens = nltk.word_tokenize(d)
    tagged = nltk.pos_tag(tokens)
    entities = nltk.chunk.ne_chunk(tagged)
    print(entities)
