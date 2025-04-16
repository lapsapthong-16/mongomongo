#------------------------
# Author: Tan Zhi Wei
#------------------------

import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer

# Load the CSV file
df = pd.read_csv('tweets_output.csv')

# Initialize the Vader SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()

# Define a function to classify sentiment
def classify_sentiment(tweet):
    # Check if the tweet is a string
    if pd.isna(tweet) or not isinstance(tweet, str):
        return 'Unknown'  # Handle NaN or non-string values
    
    sentiment_score = analyzer.polarity_scores(tweet)
    compound_score = sentiment_score['compound']
    
    if compound_score >= 0.05:
        return 'Positive'
    elif compound_score <= -0.05:
        return 'Negative'
    else:
        return 'Neutral'

# Apply sentiment analysis on the 'Tweet' column
df['Sentiment'] = df['Tweet'].apply(classify_sentiment)

# Count the number of tweets with each sentiment
sentiment_counts = df['Sentiment'].value_counts()
print("Sentiment distribution:")
print(sentiment_counts)

# Display the first few rows of the dataframe with sentiment
print("\nSample of tweets with sentiment:")
print(df.head())

# Calculate sentiment distribution by user
print("\nSentiment distribution by user:")
sentiment_by_user = df.groupby(['User ID', 'Sentiment']).size().unstack(fill_value=0)
print(sentiment_by_user)

# Calculate the percentage of positive, negative, and neutral tweets for each user
sentiment_percentage = sentiment_by_user.div(sentiment_by_user.sum(axis=1), axis=0) * 100
print("\nPercentage of sentiment by user:")
print(sentiment_percentage.round(2))

# Save the result to a new CSV
df.to_csv('tweets_output_with_sentiment.csv', index=False)