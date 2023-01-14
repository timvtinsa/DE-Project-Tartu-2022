import time

from utils import *

SERP_API_BASE_URL = "https://serpapi.com"
SERP_API_KEY = "e58bca596eac011ac17c5c0624c349a76db7cbc8d810193832a24c15c0257dab"

def query_serpapi_title(title):
    print("[SERP API] Querying paper: " + title)
    url = SERP_API_BASE_URL + "/search.json?engine=google_scholar&q="
    query_title = "+".join(title.split())
    while True:
        try:
            return get_JSON(url + query_title + "&hl=en&api_key=" + SERP_API_KEY)
        except Exception as e:
            print(e)
            print("Request error, retrying...")
            time.sleep(5)

def query_serpapi_author(author_id):
    print("[SERP API] Querying author: " + author_id)
    url = SERP_API_BASE_URL + "/search.json?engine=google_scholar_author&author_id="
    while True:
        try:
            return get_JSON(url + author_id + "&hl=en&api_key=" + SERP_API_KEY)
        except Exception as e:
            print(e)
            print("Request error, retrying...")
            time.sleep(5)

