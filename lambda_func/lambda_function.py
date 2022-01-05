import praw
import datetime

def lambda_handler(event, context):   
    reddit = praw.Reddit(client_id="bugSUScyyC98vFTHpyZXMQ", 
                        client_secret="KDhBF8XvjxL11WZmHIuhnaNQ3nzzSg", 
                        user_agent="Scraping")
    
    df_list = []
    for submission in reddit.subreddit("all").search("communist", syntax='lucene', limit=None):
        # if re.search(pattern, submission.title.lower()) or re.search(pattern, submission.selftext.lower()):
        # to not record duplicates of posts
        # if submission.id not in id_list:
            # convert UTC time to local time
            local_datetime_converted = datetime.datetime.fromtimestamp(submission.created_utc)
            # add to df_list
            new_row = [submission.id, submission.author, submission.title, submission.selftext,
                        local_datetime_converted, submission.score]
            # print_info(submission)
            # id_list.append(submission.id)
            df_list.append(new_row)
            
    return df_list