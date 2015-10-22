__author__ = 'nlelab'
import csv
import igraph
import MySQLdb
import MySQLdb.cursors
import config as cfg
import random

# Network-Centric Community Detection Algorithms
#Modularity Maximisation
#infomap

db = MySQLdb.connect(host=cfg.mysql['host'], # your host, usually localhost
             user=cfg.mysql['user'], # your username
             passwd=cfg.mysql['passwd'], # your password
             db=cfg.mysql['db'],# name of the data base
             )

cur = db.cursor()
db.set_character_set('utf8')
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
db.commit()

cur.execute("SELECT user_source as source, user_target as target, count(*) as weight from relation "
            "where type='RP'AND date < '2014-01-01 00:00:00'"
            "group by user_source,user_target "
            )
records = cur.fetchall()


graph=igraph.Graph.TupleList(records, weights=True, directed=True)

print "Is graph weighted? ",graph.is_weighted()

community_infomap = graph.community_infomap(edge_weights='weight', vertex_weights=None, trials=10)
print community_infomap.summary()

#Returns a boolean vector where element i is True iff edge i lies between clusters, False otherwise.
is_edges_between_clusters = community_infomap.crossing()

print "Finding edges between and not-between communities"
link_internal=[]
link_external=[]

for index in range(0,len(is_edges_between_clusters)):
    source = graph.vs(graph.es[index].source)['name'][0]
    target = graph.vs(graph.es[index].target)['name'][0]
    if not is_edges_between_clusters[index]:
        link_internal.append([source,target])
    else:
        link_external.append([source,target])



print "Number of internal edges", len(link_internal)
print "Number of external edges", len(link_external)

print "Extract tweet id for internal and external edges"

cur.execute("SELECT user_source,user_target, tweet_id, type from relation where type='RP'AND date < '2014-01-01 00:00:00' ")
records = cur.fetchall()
for r in records:

    for link in link_internal:
        if link==[r[0],r[1]] or link==[r[1],r[0]]:
            cur.execute("INSERT is_edge_between (tweet_id, infomap) "
                        "VALUES (%s,%s) "
                        "on duplicate key update infomap=%s",
                        (r[2], 1, 1)
                        )
            db.commit()

    for link in link_external:
        if link==[r[0],r[1]] or link==[r[1],r[0]]:
            cur.execute("INSERT is_edge_between (tweet_id, infomap) "
                        "VALUES (%s,%s) "
                        "on duplicate key update infomap=%s",
                        (r[2], 1, 0)
                        )
            db.commit()





