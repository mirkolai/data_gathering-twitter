__author__ = 'nlelab'
import csv
import networkx as nx
import matplotlib.pyplot as plt
import MySQLdb
import config as cfg
import community # --> http://perso.crans.org/aynaud/communities/

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

cur.execute("SELECT user_source, user_target, count(*) from relation "
            "where date < '2014-01-01 00:00:00'"
            "group by user_source,user_target ")
records = cur.fetchall()
#the recordset return an array of user relations (user_id -> user_id) that can be RT, RP or MT

ids_rendundant = list(set([row[0] for row in records]))+list(set([row[1] for row in records]))
#takes the reduantices of the ids networks
ids=list(enumerate(ids_rendundant))
# creates a list of tuples with unique ids

nodes=[]
for key, value in ids:
    nodes.append(value)

links = [] #creates a blank list
for row in records:
    links.append([row[0], row[1]])


G = nx.Graph()

G.add_nodes_from(nodes)#creates nodes for the graph.

for r in records:
    G.add_edge(r[0],r[1],weight=r[2])
"""

    Louvain Modularity

"""
partition = community.best_partition(G)
print "Louvain Modularity: ", community.modularity(partition, G)
print "Louvain Partition: ", partition
print "Number of nodes: ", len(nodes)
print "Number of edges: ", len(links)

#number of nodes in each partition
partitions_frequency={}
for key,value in partition.items():
    if value not in partitions_frequency:
        partitions_frequency[value]=1
    else:
        partitions_frequency[value]+=1

print "Number of partitions: ", len(partitions_frequency)

#number of nodes in each partition, but only partition big than 1%
partitions_frequency_big_than_1=partitions_frequency.copy()
for key,value in partitions_frequency_big_than_1.items():
    if (value/float(len(nodes)))*100 < 1: #remove partition less than 1%
        partitions_frequency_big_than_1.pop(key,0)

print "Number of partitions big than 1%: ", \
    len(partitions_frequency_big_than_1),\
    "(",len(partitions_frequency_big_than_1)/float(len(partitions_frequency))*100,")"

#number of nodes in  partition big than 1%
partitions_big_than_1=partition.copy()
for key,value in partitions_big_than_1.items():
    if value not in partitions_frequency_big_than_1:
        partitions_big_than_1.pop(key)

print "Number of nodes in partitions big than 1%: ",\
    len(partitions_big_than_1),\
    "(",len(partitions_big_than_1)/float(len(partition))*100,")"

#Number of edges between partitions big than 1%
links_big_than_1=links[:]
for link in links_big_than_1:
    if link[0] not in partitions_big_than_1 or link[1] not in partitions_big_than_1:
        links_big_than_1.remove(link)

print "Number of edges between partitions big than 1%: ",\
    len(links_big_than_1),\
    "(",len(links_big_than_1)/float(len(links))*100,")"


#Number of edges between nodes belonging to the same/different partition (only partition big than 1%)
link_internal=[]
link_external=[]

for link in links:

    if partition[link[0]]==partition[link[1]]:
        link_internal.append([link[0],link[1]])
    else:
        link_external.append([link[0],link[1]])

print "Number of edges between nodes belonging to the same partition: ",\
    len(link_internal),\
    "(",len(link_internal)/float(len(links))*100,")"

print "Number of edges between nodes belonging to different partition: ",\
    len(link_external),\
    "(",len(link_external)/float(len(links))*100,")"

#Number of edges between nodes belonging to the same/different partition (only partition big than 1%)
link_internal_big_than_1=[]
link_external_big_than_1=[]

for link in links_big_than_1:
    if partition[link[0]]==partition[link[1]]:
        link_internal_big_than_1.append([link[0],link[1]])
    else:
        link_external_big_than_1.append([link[0],link[1]])


print "Number of edges between nodes belonging to the same partition (only partition big than 1%): ",\
    len(link_internal_big_than_1),\
    "(",len(link_internal_big_than_1)/float(len(links_big_than_1))*100,")"

print "Number of edges between nodes belonging to different partition (only partition big than 1%): ",\
    len(link_external_big_than_1),\
    "(",len(link_external_big_than_1)/float(len(links_big_than_1))*100,")"



cur.execute("SELECT user_source,user_target, tweet_id, type from relation where date < '2014-01-01 00:00:00' ")
records = cur.fetchall()



with open('tweet_id_internal_edges.csv', 'w') as fp:
    a = csv.writer(fp, delimiter=',')
    a.writerow(['Id'])
    unique = []
    [unique.append(item) for item in link_internal if item not in unique]
    for link in unique:
        for r in records:
            if link==[r[0],r[1]]:
                a.writerow([r[2]])

with open('tweet_id_external_edges.csv', 'w') as fp:
    a = csv.writer(fp, delimiter=',')
    a.writerow(['Id'])
    unique = []
    [unique.append(item) for item in link_external if item not in unique]
    for link in unique:
        for r in records:
            if link==[r[0],r[1]]:
                a.writerow([r[2]])

with open('nodes.csv', 'w') as fp:
    a = csv.writer(fp, delimiter=',')
    a.writerow(['Id','Label'])
    for key, value in partitions_big_than_1.items():
        a.writerow([key, key])

with open('edge.csv', 'w') as fp:
    a = csv.writer(fp, delimiter=',')
    a.writerow(['Source','Target'])
    for link in links_big_than_1:
        a.writerow(link)
