import requests
import os
from dotenv import load_dotenv
import json
load_dotenv()
# Replace with your NYT API key
API_KEY = os.getenv("NYT_API_KEY")

# TimesWire API endpoint (English-only NYT content, world section)
url = "http://api.nytimes.com/svc/news/v3/content/nyt/world.json"

# Parameters for the API request
params = {
    "api-key": API_KEY,
    "source": "nyt"  # Ensure English content from NYT
}
response = requests.get(url, params=params)
result=response.json()['results']
for item in result:
    print(f"Title: {item['title']}")
    print(f"Published Date: {item['published_date']}")
    print(f"Abstract: {item['abstract']}\n")