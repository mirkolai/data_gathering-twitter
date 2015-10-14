import os
import MySQLdb
import oauth2 as oauth
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

CONSUMER_KEY    = cfg.twitter['CONSUMER_KEY']
CONSUMER_SECRET = cfg.twitter['CONSUMER_SECRET']
ACCESS_KEY      = cfg.twitter['ACCESS_KEY']
ACCESS_SECRET   = cfg.twitter['ACCESS_SECRET']

consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
access_token = oauth.Token(key=ACCESS_KEY, secret=ACCESS_SECRET)
client = oauth.Client(consumer, access_token)

cur.execute("SELECT * FROM tweet limit 40060,100000")
tweets = cur.fetchall()
i=40060
for tweet in tweets:
    print i
    i=i+1
    jsonTweet=json.loads(tweet[1])
    if 'retweeted_status' not in jsonTweet: #else it is a retweet
        if jsonTweet['retweet_count']>0:
            max_id=-1
            while max_id!=0:

                if max_id<0:

                    endpoint = "https://api.twitter.com/1.1/statuses/retweets/"+tweet[0]+".json"
                else:
                    endpoint = "https://api.twitter.com/1.1/statuses/retweets/"+tweet[0]+".json?max_id="+str(max_id)

                print endpoint
                response, data = client.request(endpoint)
                #print response
                if response['status']=='200':

                    if 'x-rate-limit-remaining' in response and 'x-rate-limit-reset' in response:
                       if int(response['x-rate-limit-remaining'])<2:
                            print 'id rescue: wait '+str(int(response['x-rate-limit-reset'])-int(time.time()))+' seconds'
                            time.sleep(int(response['x-rate-limit-reset'])-int(time.time()))

                    max_id = 0
                    retweets = json.loads(data)
                    print str(len(retweets))+' recovered'
                    for retweet in retweets:

                        #print retweet
                        cur.execute("INSERT retweet (id, json) VALUES (%s,%s) on duplicate key update id=id",(retweet['id'],json.dumps(retweet)))
                        db.commit()

                        if max_id==0:
                            max_id=int(retweet['id'])-1
                        elif max_id>int(retweet['id']):
                            max_id=int(retweet['id'])-1

                    if len(retweets)<100:
                        max_id=0

                    if 'x-rate-limit-remaining' in response:
                        print 'id rescue: wait '+str((15*60)/int(response['x-rate-limit-limit']))+' seconds'
                        time.sleep((15*60)/int(response['x-rate-limit-limit']))

                elif response['status']==400 or response['status']==403 or response['status']==404 or response['status']==401:
                    print 'E '+response['status']
                    max_id=0
                else:
                    print 'E '+response['status']
                    max_id=0
    #else:
    #    print 'it is a retweet'

db.close()