#------------------------
# Author: Tan Zhi Wei
#------------------------
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

def analyze_tweet_sentiment(input_csv_path, output_csv_path=None):
    """
    Perform sentiment analysis on tweets from a CSV file and generate statistics.
    
    Parameters:
    input_csv_path (str): Path to the input CSV file containing tweets.
    output_csv_path (str, optional): Path to save the output CSV with sentiment analysis.
                                    If None, will use input filename + '_with_sentiment.csv'.
    
    Returns:
    pandas.DataFrame: DataFrame with the original data plus sentiment analysis results.
    """
    # Set default output path if not provided
    if output_csv_path is None:
        output_csv_path = input_csv_path.replace('.csv', '_with_sentiment.csv')
    
    # Load the CSV file
    df = pd.read_csv(input_csv_path)
    
    # Ensure VADER lexicon is downloaded
    try:
        # Initialize the Vader SentimentIntensityAnalyzer
        analyzer = SentimentIntensityAnalyzer()
    except LookupError:
        print("Downloading required NLTK resources...")
        nltk.download('vader_lexicon')
        # Try again after downloading
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
    if output_csv_path:
        df.to_csv(output_csv_path, index=False)
        print(f"\nResults saved to {output_csv_path}")
    
    return df

# Your main function can call it like this:
# if __name__ == "__main__":
#     analyzed_data = analyze_tweet_sentiment('tweets_output.csv')
