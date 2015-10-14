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


cur.execute("SELECT * FROM reply")
tweets = cur.fetchall()
arethereotherreplytoceck=1

while arethereotherreplytoceck:
    arethereotherreplytoceck=0
    for tweet in tweets:

        jsonTweet=json.loads(tweet[1])

        if jsonTweet['in_reply_to_status_id']!=None:

            cur.execute("select * from reply where id=%s",(jsonTweet['in_reply_to_status_id']))
            result = cur.fetchall()
            if len(result)==0:

                endpoint = "https://api.twitter.com/1.1/statuses/show.json?id="+str(jsonTweet['in_reply_to_status_id'])
                print endpoint
                response, data = client.request(endpoint)
                #print response
                print response['status']
                #print response['x-rate-limit-limit']
                #print data
                if response['status']=='200':
                    arethereotherreplytoceck=1
                    if int(response['x-rate-limit-remaining'])<2:
                        print 'id rescue: wait '+str( int(response['x-rate-limit-reset']) - int(time.time()) )+' seconds'
                        time.sleep(int(response['x-rate-limit-reset'])-int(time.time()))

                    reply=json.loads(data)
                    cur.execute("INSERT reply (id, json) VALUES (%s,%s) on duplicate key update id=id",(reply['id'],json.dumps(reply)))
                    db.commit()

                print 'id rescue: wait '+str((15*60)/int(response['x-rate-limit-limit']))+' seconds'
                time.sleep((15*60)/int(response['x-rate-limit-limit']))
            else:
                print 'tweet in db'
        else:
            print 'it is not a reply'

db.close()