import requests
import csv
import re

def fetch_tweet_title(user_ids, count_per_user=50, output_filename='tweets_output.csv'):
    """
    Fetches tweets for the provided list of user IDs and stores them in a CSV file.
    
    Parameters:
        user_ids (list): List of user IDs to fetch tweets for.
        count_per_user (int): Number of tweets to fetch per user.
        output_filename (str): The name of the output CSV file where tweets will be stored.
    
    Returns:
        dict: A dictionary where the keys are user IDs and the values are lists of fetched tweet titles.
    """
    url = "https://twitter241.p.rapidapi.com/user-tweets"
    headers = {
        "x-rapidapi-key": "a5b5a0340fmshc2902e9a96fe36cp18bef4jsnba70e7dad432",
        "x-rapidapi-host": "twitter241.p.rapidapi.com"
    }
    
    total_tweets_fetched = 0
    all_tweets_by_user = {}

    # Open a CSV file to write the output
    with open(output_filename, mode='w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write the header
        csvwriter.writerow(['User ID', 'Name', 'Followers Count', 'Tweet'])

        for user_id in user_ids:
            cursor = None
            tweets_for_this_user = 0
            full_texts_for_user = []
            
            while tweets_for_this_user < count_per_user:
                if cursor:
                    querystring = {"user": user_id, "count": "20", "cursor": cursor}
                else:
                    querystring = {"user": user_id, "count": "20"}
                
                response = requests.get(url, headers=headers, params=querystring)
                
                if response.status_code == 200:
                    data = response.json()
                    batch_tweets = 0
                    next_cursor = None
                    
                    instructions = data.get("result", {}).get("timeline", {}).get("instructions", [])
                    
                    for instruction in instructions:
                        if instruction.get("type") == "TimelineAddEntries":
                            entries = instruction.get("entries", [])
                            for entry in entries:
                                if entry.get("entryId", "").startswith("cursor-bottom"):
                                    content = entry.get("content", {})
                                    if content.get("cursorType") == "Bottom":
                                        next_cursor = content.get("value")
                                    continue
                                
                                tweet_result = entry.get("content", {}).get("itemContent", {}).get("tweet_results", {}).get("result", {})
                                full_text = tweet_result.get("legacy", {}).get("full_text", "")
                                user_name = tweet_result.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {}).get("name", "")
                                followers_count = tweet_result.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {}).get("followers_count", 0)
                                
                                if followers_count > 9999999999:
                                    followers_count = 9999999999
                                
                                if full_text:
                                    full_text = re.sub(r'http\S+', '', full_text).strip()
                                    full_texts_for_user.append(full_text)
                                    csvwriter.writerow([user_id, user_name, followers_count, full_text])
                                    batch_tweets += 1
                                    
                                    if len(full_texts_for_user) >= count_per_user:
                                        break
                    
                    tweets_for_this_user += batch_tweets
                    
                    if batch_tweets == 0 or not next_cursor or len(full_texts_for_user) >= count_per_user:
                        break
                    
                    cursor = next_cursor
                
                else:
                    print(f"Error for user {user_id}: {response.status_code}")
                    print(response.text)
                    break
            
            all_tweets_by_user[user_id] = full_texts_for_user
            total_tweets_fetched += len(full_texts_for_user)
            
            print(f"Fetched {len(full_texts_for_user)} tweets for user {user_id}")
    
    # Final status report
    if len(all_tweets_by_user) == len(user_ids):
        print("\nSuccessfully fetched data for all users!")
    else:
        print(f"\nFetched data for {len(all_tweets_by_user)} out of {len(user_ids)} users")
    print(f"Total tweets fetched: {total_tweets_fetched}")

    return all_tweets_by_user
