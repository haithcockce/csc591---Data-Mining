# Centrality measures are a way to determine nodes that are important based on the structure of
# the graph. Closeness centrality measures the distance of a node to all other nodes. We will
# define the closeness centrality as

# C(x) = 1 / (sum over all y distance(y, x))

# where d(u, v) is the shortest path distance between u and v.
# Implement the function closeness, which takes a GraphFrame object as input and computes
# the closeness centrality of every node in the graph. The function should return a DataFrame
# with two columns: id and closeness.

# Consider a small network of 10 computers, illustrated below, with nodes representing computers
# and edges representing direct connections of two machines. If two computers are not connected
# directly, then the information must flow through other connected machines.

import sys, errno, ipdb

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import FloatType

from graphframes import *



def closeness(g):
    # Grab the nodes so we can get the shortest paths between all of them
    nodes = g.vertices.map(lambda row: row['id']).collect()
    # Graph the shortest path between all nodes (and get rid of the useless name column)
    distances = g.shortestPaths(nodes).drop('name')
    # Sum the total distance for each node to all other nodes
    distances = distances.select(explode(distances['distances']))\
                         .groupBy('key')\
                         .sum('value')

    # Rename the table 
    old_names = distances.schema.names
    new_names = ['id', 'closeness']
    distances =\
        reduce(lambda distances, idx:\
            distances.withColumnRenamed(\
                old_names[idx], new_names[idx]), xrange(len(old_names)), distances\
            )
    
    # Closeness centrality is measured 1 divided by the sum os all distances from x to all 
    # other nodes (done above). This next bit divides 1 by each entry in 'closeness' 
    # and then reattaches it to the table
    normed = UserDefinedFunction(lambda x: (1.0 / x) if x != 0 else 0.0, FloatType())
    distances = distances.withColumn('closeness', normed(distances['closeness']))
    
    return distances



def main():
    print ''
    print 'not implemented'
    print ''


if __name__ == "__main__":
    main()
