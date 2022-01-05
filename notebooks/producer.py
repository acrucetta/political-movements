import boto3
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd
import praw
# import simplejson as json

# must remove any spaces
subreddits = ["WorldNews",
              "WorldPolitics",
              "Politics", 
              "USPolitics", 
              "AmericanPolitics", 
              "Republican", 
              "Democrats", 
              "Conservative", 
              "Progressive", 
              "Libertarian"]

# key terms to look for in each movement
key_terms = pd.DataFrame([["Nationalism", "nationalist, nationalism"],
                        ["Socialism", "socialist, socialism"],
                        ["Communism", "communist, communism"],
                        ["Fascism", "fascist, fascism"],
                        ["Liberalism", "liberal, liberalism"],
                        ["Conservatism", "conservative, conservatism"],
                        ["Authoritarianism", "authoritarian, authoritarianism"],
                        ["Populism", "populist, populism"],
                        ["Progressivism", "progressive, progressivism"],
                        ["QAnon", "qanon"],
                        ["Libertarianism", "libertarian, libertarianism"],
                        ["Marxism", "marxist, marxism"],
                        ["Neoliberalism", "neoliberal, neoliberalism"]],
                        columns=["Movement", "Key Terms"])

all_data = []
# create the parameters to pass to the lambda workers
for subreddit in subreddits:
    for idx, row in key_terms.iterrows():
        all_data.append({"subreddit_name": subreddit,
                         "search_terms": row["Key Terms"],
                        "movement": row["Movement"]})

# how many lambda workers to use
workers = len(all_data)

def invoke_lambdas(test_data):
    aws_lambda = boto3.client('lambda', region_name='us-east-1')

    r = aws_lambda.invoke(FunctionName='scrape_reddit',
                       InvocationType='RequestResponse',
                       Payload=json.dumps(test_data))
    return json.loads(r['Payload'].read())


def getReferrer():
    # invoke lambda workers in parallel
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = executor.map(invoke_lambdas, all_data)

    # time.sleep(2)
    
    # gather the results
    df_list = []
    for result in results:
        df_list.append(result)
    df = pd.DataFrame(df_list, columns=["subreddit", "movement", "mentions"])
    summary = df.groupby("movement").mentions.sum()

    # reformat into a dictionary so we can json serialize it
    movement_list = summary.index.values.tolist()
    values = [int(x) for x in df.groupby("movement").mentions.sum()]
    output = {m:v for m,v in zip(movement_list, values)}

    return output


def stream_posts():
    # connect to Reddit API
    reddit = praw.Reddit(client_id="bugSUScyyC98vFTHpyZXMQ", 
                        client_secret="KDhBF8XvjxL11WZmHIuhnaNQ3nzzSg", 
                        user_agent="Scraping")

    # PRAW built in streaming
    for submission in reddit.subreddit("all").stream.submissions():
        yield submission.title

# connect to AWS Kinesis
kinesis = boto3.client('kinesis', region_name='us-east-1')

while True:
    kinesis.put_record(StreamName = "reddit",
                      Data = json.dumps(getReferrer()),
                      PartitionKey = "shard")