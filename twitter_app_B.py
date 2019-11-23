"""
    This script connects to Twitter Streaming API, gets tweets with '#' and
    forwards them through a local connection in port 9009. That stream is
    meant to be read by a spark app for processing. Both apps are designed
    to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --name twitter -p 9009:9009 python bash
        pip install -U git+https://github.com/tweepy/tweepy.git
        python twitter_app.py
        
    and in the Second docker to plot the graph (optional):
        docker run -it -v $PWD:/app --name server -p 5001:5001 python bash
    
    and in the Third docker do:    
        docker run -it -v $PWD:/app --link twitter:twitter --link server:server eecsyorku/eecs4415
        spark-submit spark_app.py

    (we don't do pip install tweepy because of a bug in the previous release)
    For more instructions on how to run, refer to final slides in tutorial 8

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Author: Fahad Qayyum

"""

# from __future__ import absolute_import, print_function

import socket
import sys
import json
import re


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream

# Replace the values below with yours
consumer_key="yHpyc9zLr41D0ZxqVVu47QtCr"
consumer_secret="reK0UUMLnPjx1UH7MooR0slPgi1rnRxnwjUl4czLQ4lgYwpoqi"
access_token="970740682771005441-6StbOi8ZvBF3pIgdO0eNHtECsqRySj9"
access_token_secret="jRLsfXrYXBUnwcPqzyaj9vymTSPiiRbcMQ6C96Lsqnu5x"


class TweetListener(StreamListener):
    """ A listener that handles tweets received from the Twitter stream.

        This listener prints tweets and then forwards them to a local port
        for processing in the spark app.
    """

    def on_data(self, data):
        """When a tweet is received, forward it"""
        try:

            global conn
            global hashtags
            hashtags_re = re.compile("|".join(hashtags))

            # load the tweet JSON, get pure text
            full_tweet = json.loads(data)
            tweet_text = full_tweet['text']

            # print the tweet plus a separator
            if hashtags_re.search(tweet_text.lower()):
                print ("------------------------------------------")
            
                print(tweet_text + '\n')

            # send it to spark
            conn.send(str.encode(tweet_text + '\n'))
        except:

            # handle errors
            e = sys.exc_info()[0]
            print("Error: %s" % e)


        return True

    def on_error(self, status):
        print(status)



# ==== setup local connection ====

# IP and port of local machine or Docker
TCP_IP = socket.gethostbyname(socket.gethostname()) # returns local IP
TCP_PORT = 9009

# setup local connection, expose socket, listen for spark app
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")

# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting getting tweets.")


# ==== setup twitter connection ====
listener = TweetListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, listener)

# setup search terms
topics = ['#basketball', '#baseball', '#cricket', '#soccer', '#tennis']
basketball_hashtags = ['#NBA', '#raptors', '#lakers', '#jordan', '#warriors', '#stephcurry', '#dunk', '#bball', '#kaizen', '#hoops']
baseball_hashtags = ['#bluejays', '#RonaldAcuna', '#atlanta', '#rookiecard', '#beisbol', '#mlb', '#nfl', '#mikeTrout', '#mookie', '#derekJeter']
cricket_hashtags = ['#virat', '#babarAzam', '#worldCup', '#kohli', '#dhoni', '#batting', '#bowlingmachine', '#ipl', '#cricketnation', '#pitch']
soccer_hashtags = ['#worldCup', '#championsLeague', '#messi', '#ronaldo', '#cr7', '#manchesterUnited', '#barcelona', '#realMadrid', '#chelsea', '#neymarJR']
tennis_hashtags = ['#rogerFederer', '#rogersCup', '#wimbledon', '#Federer', '#davisCupMadridFinals', '#novak', '#saniaMirza', '#nadal', '#davisCup', '#serenaWilliams']
hashtags = basketball_hashtags + baseball_hashtags + cricket_hashtags + soccer_hashtags + tennis_hashtags

# setting up language and 
language = ['en']
locations = [-130,-20,100,50]

# get filtered tweets, forward them to spark until interrupted
try:
    stream.filter(track=hashtags, languages=language, locations=locations)
except KeyboardInterrupt:
    s.shutdown(socket.SHUT_RD)

