from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import os
import time
import random
import math
import json
import tweepy
import tweepy

################################### Inital Input & Output Files #######################################################
hashtag_dict = {}
tweets_dict = {}
sequence_number= 0
fixed_size_sample = 100
# System input
port_number = int(sys.argv[1])
# Set up output file
output_file = sys.argv[2]
file = open(output_file, 'w',  encoding="utf-8")
file.close()


######################################## Function Definitions ##########################################################
#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        # Obtain Global Variables
        global sequence_number
        global hashtag_dict
        global tweets_dict
        # Extract Hashtags from each Tweet
        tweet_hashtags = status.entities['hashtags']

        # Ignore Tweets w/o Hashtags
        if len(tweet_hashtags) > 0:
            # Increase sequence count
            sequence_number += 1
            # Store First 100 Tweets
            if len(tweets_dict) < 100:
                # Store Tweet
                tweets_dict[len(tweets_dict) + 1] = status
                # Store Tags and Increment Counter
                for i in tweet_hashtags:
                    tag = i['text']
                    # Add Hashtag to dict if not exist else increment tag counter
                    hashtag_dict[tag] = hashtag_dict.get(tag, 0) + 1
            # Check to keep nth element
            else:
                # Decide if keep with probability of s/n or 100/n
                discard_new = random.randint(1,sequence_number)
                # Keep new element
                # Else discard new tweet & do nothing
                if discard_new <= 100:
                    # Uniformly pick one sample to discard from dict
                    discard_old = random.randint(1,fixed_size_sample)
                    # Obtain discard tweet
                    discard_tweet = tweets_dict[discard_old]
                    # Remove hashtag count for old hashtags
                    discard_tags = discard_tweet.entities['hashtags']
                    for i in discard_tags:
                        tag = i['text']
                        # Decrease count
                        hashtag_dict[tag] -= 1
                        # If count == 0 remove from dict
                        if hashtag_dict[tag] == 0:
                            del hashtag_dict[tag]

                    # Replace discard tweet with current tweet
                    tweets_dict[discard_old] = status

                    # Add counters for new hashtags
                    for i in tweet_hashtags:
                        tag = i['text']
                        # Add Hashtag to dict if not exist else increment tag counter
                        hashtag_dict[tag] = hashtag_dict.get(tag, 0) + 1


            # Output to CSV and Terminal from 1st Tweet
            file = open(output_file, 'a',  encoding="utf-8")
            print('The number of tweets with tags from the beginning: ', str(sequence_number))
            file.write('The number of tweets with tags from the beginning: '+ str(sequence_number) + '\n')
            # Top 3 frequencies hashtags
            top3_freq = sorted(list(set([v for k, v in hashtag_dict.items()])), reverse=True)[:3]
            output = [[],[],[]]
            for i in range(len(top3_freq)):
                for item,value in hashtag_dict.items():
                    if value == top3_freq[i]:
                        output[i].append((item,value))
            # Sort each output
            output = [sorted(i) for i in output]
            for i in output:
                for j in i:
                    print(j[0], ' : ', j[1])
                    file.write(str(j[0]) + ' : ' + str(j[1])+ '\n')
            print()
            file.write('\n')
            file.close()



########################## Set up Authentication for Twitter App & Start Streaming #####################################
# API Key + Secret Key
api_key = "jrXU3zvSwuRoyIWyc5RLOZ8uu"
api_secret = "SP68rANNVSw6yWhnK4G1QZB8wNftSG6hnnANB3tWjfthsJm8RN"

# Access Token + Token Secret
accessToken = '2402693480-XuoqKhqjryC0KT5sj5LBINWzPoX2EH4Qc6rrCwo'
accessSecret  = 'UImPmnd3rLDX38iyj24NAnLqNkl1l1e0HzhfpqQfAYQMo'

# Set up Authentication
auth = tweepy.OAuthHandler(api_key, api_secret)
auth.set_access_token(accessToken, accessSecret)

# Initiate API
api = tweepy.API(auth)

# Initiate Streaming data
myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

# Filter tweets with geo location enabled
tweets = myStream.filter(locations=[-180,-90,180,90])

