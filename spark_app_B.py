#!/usr/bin/env python
# All the import statements
import sys, requests, socket, csv
import pyspark as ps
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import pyspark.streaming as pss
from pyspark.sql import Row,SQLContext
import re
import math
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA


# Setting up the required modules
nltk.download('vader_lexicon')
sia = SIA()

# Defining topics based on the theme "SPORTS" and for ech topics categorizing hashtags
topics = ['#basketball', '#baseball', '#cricket', '#soccer', '#tennis']
basketball_hashtags = ['#NBA', '#raptors', '#lakers', '#jordan', '#warriors', '#stephcurry', '#dunk', '#bball', '#kaizen', '#hoops']
baseball_hashtags = ['#bluejays', '#RonaldAcuna', '#atlanta', '#rookiecard', '#beisbol', '#mlb', '#nfl', '#mikeTrout', '#mookie', '#derekJeter']
cricket_hashtags = ['#virat', '#babarAzam', '#worldCup', '#kohli', '#dhoni', '#batting', '#bowlingmachine', '#ipl', '#cricketnation', '#pitch']
soccer_hashtags = ['#worldCup', '#championsLeague', '#messi', '#ronaldo', '#cr7', '#manchesterUnited', '#barcelona', '#realMadrid', '#chelsea', '#neymarJR']
tennis_hashtags = ['#rogerFederer', '#rogersCup', '#wimbledon', '#Federer', '#davisCupMadridFinals', '#novak', '#saniaMirza', '#nadal', '#davisCup', '#serenaWilliams']
hashtags_list = basketball_hashtags + baseball_hashtags + cricket_hashtags + soccer_hashtags + tennis_hashtags

# Variables to check against in the "determine_topic" function
topic_one = 'basketball'
topic_two = 'baseball'
topic_three = 'cricket'
topic_four = 'soccer'
topic_five = 'tennis'

# Checking if word is in the "hashtags_list" defined above
def check_word(text):
    text = clean_input(text)
    for word in text.split(" "):
        if word.lower() in hashtags_list:
            return True
    return False

# Determining which topic does the particular hashtag belongs to
def determine_topic(text):
    text = clean_input(text)
    print("\n--------"+text+"--------")
    for word in text.split(" "):
        if word.lower() in basketball_hashtags:
            return topic_one
        if word.lower() in baseball_hashtags:
            return topic_two
        if word.lower() in cricket_hashtags:
            return topic_three
        if word.lower() in soccer_hashtags:
            return topic_four
        if word.lower() in tennis_hashtags:
            return topic_five

# This function will clean the input by removing unnecessary punctuation
def clean_input(input):
    # input must be of type string
    if type(input) != str:
        raise TypeError("input not a string type")
    
    # next, the input must be stripped of punctuation excluding hashtags since we need them
    temp = re.sub(r'[^\w#]', ' ', input)

    # now, any contractions are collapsed by removing the apostrophes
    temp = re.sub(r'[\']', '', input)
    
    # any numbers will also be removed since they hold very little meaning later on
    temp = re.sub(r'[\d]', '', temp)

    # replace all whitespace with the space character, this joins all the text into one scenetence
    temp = re.sub(r'[\s]', ' ', temp)
    return temp

# Performing sentiment analysis on the Tweet, returning 1 for +ve, -1 for -ve, 0 for neutral tweet
def sentiment_analysis(text):
    text = clean_input(text)
    # If negative return -1
    if(sia.polarity_scores(text)['compound'] < 0):
        print("Its negative")
        return -1
    # If positive return +1
    elif(sia.polarity_scores(text)['compound'] > 0):
        print("Its positive")
        return 1
    # If neutral return 0
    elif(sia.polarity_scores(text)['compound'] == 0):
        print("Its neutral")
        return 0  

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 2)

# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("twitter",9009)

# gets one whole tweet
tweets = dataStream

# filter the words to tweets with the hashtags we are looking for
filtered_tweets = tweets.filter(check_word)  

# map each hashtag to be a pair of (Topic, sentiment value)
tweet_sentiScore_counts = filtered_tweets.map(lambda x: (determine_topic(x), sentiment_analysis(x)))

# map each hashtag to be a pair of (Topic, hashtag count)
tweet_count_by_category = filtered_tweets.map(lambda x: (determine_topic(x), 1))

# adding the count of each hashtag
def aggregate_tags_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)	

# do the aggregation, note that now this is a sequence of RDDs
tweet_totals = tweet_sentiScore_counts.updateStateByKey(aggregate_tags_count)
tweet_totals_by_category = tweet_count_by_category.updateStateByKey(aggregate_tags_count)


def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']   

# Truncating the value of 'n' to 2 decimal places
def truncate(n, decimals=2):
    multiplier = 10 ** decimals
    return int(n * multiplier) / multiplier

# Each process interval get the data from the DF and show the results
def process_interval(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        for tag in rdd.collect():
            sql_context = get_sql_context_instance(rdd.context)
            row_rdd = rdd.map(lambda w: Row(hashtag=w[0], average_sentiment=truncate(w[1][0]/w[1][1])))

            hashtags_df = sql_context.createDataFrame(row_rdd)
            hashtags_df.registerTempTable("hashtags")
            hashtag_counts_df = sql_context.sql("SELECT hashtag, average_sentiment FROM hashtags")


            hashtag_counts_df.show()
            send_df_to_dashboard(hashtag_counts_df)
            break
    except:
        e = sys.exc_info()[0]
        print("Error: {}".format(e))

def send_df_to_dashboard(df):
	# extract the hashtags from dataframe and convert them into array
	top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
 
	# extract the counts from dataframe and convert them into array
	tags_count = [p.average_sentiment for p in df.select("average_sentiment").collect()]
 
	# initialize and send the data through REST API
	url = 'http://server:5001/updateData'
	request_data = {'label': str(top_tags), 'data': str(tags_count)}
	response = requests.post(url, data=request_data)

# Joining both the datasets and do this for each specified interval
tweet_totals.join(tweet_totals_by_category).foreachRDD(process_interval)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()