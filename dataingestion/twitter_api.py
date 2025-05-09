#------------------------
# Author: Tan Zhi Wei
#------------------------
import requests

def get_user_data(username):
    url = "https://twitter241.p.rapidapi.com/user"

    # Set the querystring with the user input
    querystring = {"username": username}
    print(f"Querystring set to: {querystring}")

    headers = {
        "x-rapidapi-key": "9696f35cd0msh4a07aee7562146dp1d6a82jsnb04a0cc2e2d8",
        "x-rapidapi-host": "twitter241.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    # Check if the request wass successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Navigate through the JSON structure to extract username and rest_id
        user_data = data.get("result", {}).get("data", {}).get("user", {}).get("result", {})
        username = user_data.get("legacy", {}).get("screen_name", "")
        rest_id = user_data.get("rest_id", "")
        
        # Return the extracted username and rest_id
        return username, rest_id
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None, None
