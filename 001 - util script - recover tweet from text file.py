import glob
import codecs
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

filelist = glob.glob("./data/*.txt")

#print filelist
print str(len(filelist))+' files to elaborate'
ids=[]

for file in filelist:
    #print file
    file_data=codecs.open(file, 'r', encoding='utf-8');

    for row in file_data:
        if len(row)>0:
            ids.append(row.replace("\n",""))

print str(len(ids))+' ids to gather'


while len(ids) > 0:
    parameter = ','.join(ids[0:99]) #max 100 id per request
    ids[0:99] =[]
    #print parameter
    endpoint ="https://api.twitter.com/1.1/statuses/lookup.json?id="+parameter
    response, data = client.request(endpoint)
    if response['status']=='200':
        if int(response['x-rate-limit-remaining'])<2:
            print 'id rescue: wait '+str( int(response['x-rate-limit-reset']) - int(time.time()) )+' seconds'
            time.sleep(int(response['x-rate-limit-reset'])-int(time.time()))

        jsonTweet=json.loads(data)
        for tweet in jsonTweet:
            #print json.dumps(tweet)
            cur.execute("INSERT tweet (id, json) VALUES (%s,%s) on duplicate key update id=id",(tweet['id'],json.dumps(tweet)))
            db.commit()

    print 'id rescue: wait '+str((15*60)/int(response['x-rate-limit-limit']))+' seconds'
    time.sleep((15*60)/int(response['x-rate-limit-limit']))


db.close()