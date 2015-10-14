__author__ = 'nlelab'
import os
import MySQLdb
import time
import json
import config as cfg
print os.getcwd()

db = MySQLdb.connect(host=cfg.mysql['host'], # your host, usually localhost
             user=cfg.mysql['user'], # your username
             passwd=cfg.mysql['passwd'], # your password
             db=cfg.mysql['db']) # name of the data base

cur = db.cursor()
db.set_character_set('utf8')
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
db.commit()

cur.execute("SELECT * FROM tweet UNION SELECT * FROM reply")
tweets = cur.fetchall()

for tweet in tweets:
    jsonTweet=json.loads(tweet[1])
    #print jsonTweet
    if 'retweeted_status' not in jsonTweet: #else it is a retweet
        for user_mention in jsonTweet['entities']['user_mentions']:
            #print user_mention
            cur.execute("INSERT relation (user_source, user_target,type,date,tweet_id) VALUES (%s,%s,%s,%s,%s) on duplicate key update tweet_id=tweet_id",
                        (jsonTweet['user']['id'],
                         user_mention['id'],
                         'MT',
                         time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(jsonTweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
                         jsonTweet['id'])
                        )
            db.commit()