import boto3
import datetime
import pandas as pd
import ast

# connect to AWS Kinesis
kinesis = boto3.client('kinesis', region_name='us-east-1')

shard_it = kinesis.get_shard_iterator(StreamName = 'reddit',
                                      ShardId = 'shardId-000000000000',
                                      ShardIteratorType = 'LATEST')["ShardIterator"]

while True:
    out = kinesis.get_records(ShardIterator = shard_it,
                             Limit = 1) # read one at a time

    try:
        # extract data
        summary = out['Records'][0]['Data']
        results_dict = ast.literal_eval(summary.decode("UTF-8"))
        results = pd.DataFrame.from_dict(results_dict, orient='index').T
        # print the current time and the summed results
        print(datetime.datetime.now())
        print(results)
    except:
        # this is to prevent errors when the shard has no data in it
        pass

    # go to next shard
    shard_it = out['NextShardIterator']