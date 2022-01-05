import praw
import re

def lambda_handler(event, context):   
    # provide reddit credentials to access the API
    reddit = praw.Reddit(client_id="bugSUScyyC98vFTHpyZXMQ", 
                        client_secret="KDhBF8XvjxL11WZmHIuhnaNQ3nzzSg", 
                        user_agent="Scraping")
    
    # count the number of keyword mentions
    acc = 0
    # for submission in reddit.subreddit(event["subreddit_name"]).search(event["search_terms"], sort="new"): # time_filter="day" limit=None
    # for submission in reddit.subreddit(event["subreddit_name"]).new():
    for submission in reddit.subreddit(event["subreddit_name"]).hot():
    
        # check if our search terms are in these posts
        search_pattern = "|".join(event["search_terms"].split(", "))
        if re.search(search_pattern, submission.title.lower()) or re.search(search_pattern, submission.selftext.lower()):
            acc += 1
            
    return event["subreddit_name"], event["movement"], acc
