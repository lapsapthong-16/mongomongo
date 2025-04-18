#------------------------
# Author: Tan Zhi Wei
#------------------------
import requests  # For making HTTP requests
import csv       # For writing data to CSV
import re        # For regular expression to remove URLs

def fetch_tweets(user_ids=None, count_per_user=50, output_file='tweets_output.csv', api_key=None):
    """
    Fetch tweets from specified Twitter users and save them to a CSV file.
    
    Args:
        user_ids (list): List of Twitter user IDs to fetch tweets from.
        count_per_user (int): Number of tweets to fetch per user.
        output_file (str): Path to the output CSV file.
        api_key (str): RapidAPI key for Twitter API.
        
    Returns:
        dict: Dictionary mapping user IDs to their tweets.
    """
    if user_ids is None:
        user_ids = ["22594051", "55186601", "18040230", "61083422", "102098902", "145550026"]
    
    if api_key is None:
        api_key = "9696f35cd0msh4a07aee7562146dp1d6a82jsnb04a0cc2e2d8"
    
    url = "https://twitter241.p.rapidapi.com/user-tweets"
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "twitter241.p.rapidapi.com"
    }
    
    total_tweets_fetched = 0
    all_tweets_by_user = {}

    # Open a CSV file to write the output
    with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write the header with the additional columns
        csvwriter.writerow(['User ID', 'Name', 'Followers Count', 'Tweet', 'Location', 'Time', 'Friends Count'])
        
        for user_id in user_ids:
            # We'll potentially make multiple requests with pagination to get more tweets
            cursor = None
            tweets_for_this_user = 0
            full_texts_for_user = []
            
            # Make multiple requests until we reach our desired count or run out of tweets
            while tweets_for_this_user < count_per_user:
                # Add cursor to querystring if we have one
                if cursor:
                    querystring = {"user": user_id, "count": "20", "cursor": cursor}
                else:
                    querystring = {"user": user_id, "count": "20"}
                    
                response = requests.get(url, headers=headers, params=querystring)
                
                # Check if the request was successful
                if response.status_code == 200:
                    # Parse the JSON response
                    data = response.json()
                    
                    # Initialize variables for this batch
                    batch_tweets = 0
                    next_cursor = None
                    
                    # Extract tweets and look for cursor for pagination
                    instructions = data.get("result", {}).get("timeline", {}).get("instructions", [])
                    
                    for instruction in instructions:
                        if instruction.get("type") == "TimelineAddEntries":
                            entries = instruction.get("entries", [])
                            for entry in entries:
                                # Check if this is a cursor entry
                                if entry.get("entryId", "").startswith("cursor-bottom"):
                                    content = entry.get("content", {})
                                    if content.get("cursorType") == "Bottom":
                                        next_cursor = content.get("value")
                                    continue
                                    
                                tweet_result = entry.get("content", {}).get("itemContent", {}).get("tweet_results", {}).get("result", {})
                                user_legacy = tweet_result.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {})
                                tweet_legacy = tweet_result.get("legacy", {})
                                
                                # Extract the original user data
                                full_text = tweet_legacy.get("full_text", "")
                                user_name = user_legacy.get("name", "")
                                followers_count = user_legacy.get("followers_count", 0)
                                
                                # Extract the additional requested data
                                location = user_legacy.get("location", "")
                                created_at = tweet_legacy.get("created_at", "")
                                friends_count = user_legacy.get("friends_count", 0)
                                
                                # Limit followers count to 10 digits
                                if followers_count > 9999999999:
                                    followers_count = 9999999999
                                
                                if full_text:
                                    # Remove URLs from the full_text and strip whitespace
                                    full_text = re.sub(r'http\S+', '', full_text).strip()
                                    # Append the full_text to the list
                                    full_texts_for_user.append(full_text)
                                    # Write to CSV with the additional columns
                                    csvwriter.writerow([user_id, user_name, followers_count, full_text, location, created_at, friends_count])
                                    batch_tweets += 1
                                    # Break if we've reached our limit
                                    if len(full_texts_for_user) >= count_per_user:
                                        break
                    
                    tweets_for_this_user += batch_tweets
                    
                    # If we didn't get any tweets in this batch or no next cursor, break
                    if batch_tweets == 0 or not next_cursor or len(full_texts_for_user) >= count_per_user:
                        break
                        
                    # Set the cursor for the next request
                    cursor = next_cursor
                else:
                    print(f"Error for user {user_id}: {response.status_code}")
                    print(response.text)
                    break
            
            # Store tweets for this user
            all_tweets_by_user[user_id] = full_texts_for_user
            total_tweets_fetched += len(full_texts_for_user)
            
            print(f"Fetched {len(full_texts_for_user)} tweets for user {user_id}")

    # Checka if we have data for all users
    if len(all_tweets_by_user) == len(user_ids):
        print("\nSuccessfully fetched data for all users!")
    else:
        print(f"\nFetched data for {len(all_tweets_by_user)} out of {len(user_ids)} users")
    print(f"Total tweets fetched: {total_tweets_fetched}")
    
    return all_tweets_by_user


