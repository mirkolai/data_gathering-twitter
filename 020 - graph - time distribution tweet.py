__author__ = 'nlelab'
from bokeh.plotting import figure, output_file, show
from bokeh.models import DatetimeTickFormatter
import os
import MySQLdb
from datetime import datetime, timedelta
import json
import config as cfg
from math import pi
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

cur.execute("SELECT * FROM tweet")
tweets = cur.fetchall()

data={}

d = datetime.strptime("2012-01-01","%Y-%m-%d")
delta = timedelta(days=1)
while d <= datetime.strptime("2016-01-01","%Y-%m-%d"):
    data[d.strftime("%Y-%m-%d")]=0
    d += delta

print len(tweets)
for tweet in tweets:
    jsonTweet = json.loads(tweet[1])
    date = datetime.strptime(jsonTweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y').strftime("%Y-%m-%d")
    if date not in data:
        data[date]=1
    else:
        data[date]+=1

x=[]
y=[]
for key, value in sorted(data.iteritems()):
    x.append(datetime.strptime(key,"%Y-%m-%d"))
    if value==0:
        y.append(float('nan'))
    else:
        y.append(value)

# output to static HTML file
output_file("020 - graph - twitter daily frequency.html", title="twitter daily frequency")

# create a new plot with a title and axis labels
p = figure(title="Twitter daily frequency",
           x_axis_type="datetime",
           x_axis_label='x',
           y_axis_label='y')

p.xaxis.axis_label = 'time'
p.xaxis.major_label_orientation = pi/4
p.xaxis[0].formatter = DatetimeTickFormatter(
                                    formats=dict(
                                        hours=["%Y-%m-%d"],
                                        days=["%Y-%m-%d"],
                                        months=["%Y-%m"],
                                        years=["%Y"],
                                    )
                                )
p.yaxis.axis_label = 'daily frequency'

# add a line renderer with legend and line thickness
p.line(x, y, legend="tweets", line_width=2)

# show the results
show(p)