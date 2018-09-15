import sys, errno, ipdb
from pyspark import SparkContext
from pyspark.sql import SQLContext, types
from pyspark.sql.functions import *
from graphframes import *
from graphframes.examples import Graphs
import networkx as netx
import matplotlib.pyplot as plt

debug = False

def make_graph(file_name, sc, sqlc):
    entries = sc.textFile(file_name)
    # Determine way to split on as the graphs provided are inconsistent
    sample = entries.first()
    if ',' in sample:
        delim = ','
    elif ' ' in sample:
        delim = ' '
    else:
        print "ERROR:"
        print "    Nothing to split on."
        print "    Expected ' ' or ',' to split on."
        sys.exit(errno.EINVAL)

    # Grab the vertices
    # Here, we use the vertex name as the same as the id. Specifically, we 
    # make a series of tuples formed as (X, X), (Y, Y), ... and grab the unique tuples.
    # The tuples are required as GraphFrames can not simply use the `id` of the node and
    # another field must exist. 
    vertices = (
        entries.map(lambda entry: (entry.split(delim)[0], entry.split(delim)[0]))
        .union(entries.map(lambda entry: (entry.split(delim)[1], entry.split(delim)[1])))
        .distinct()
        .collect()
    )
    vertices = sqlc.createDataFrame(vertices, ['id', 'name'])
    if debug: vertices.show()

    # Grab the edges
    # Similar to grabbing the vertices. Since GraphFrames has no way of naturally 
    # representing undirected graphs, we have to "fake it" by making edges go both
    # ways. IE if (X)-[edge]-(Y), then we make (X, Y), (Y, X). 
    edges = (
        entries.map(lambda entry: (entry.split(delim)[0], entry.split(delim)[1]))
        .union(entries.map(lambda entry: (entry.split(delim)[1], entry.split(delim)[0])))
        .distinct()
        .collect()
    )
    edges = sqlc.createDataFrame(edges, ['src', 'dst'])
    if debug: edges.show() 
    return GraphFrame(vertices, edges)


def make_plot(g):
    deg_dist_list = g.inDegrees.select('inDegree').map(lambda degree: (degree, 1)).reduceByKey(lambda a, b: a + b).collect()
    deg_dist_list_cleaned = sorted([(entry[0].inDegree, entry[1]) for entry in deg_dist_list], key=lambda x: x[0])
    x_axis = [entry[0] for entry in deg_dist_list_cleaned]
    y_axis = [entry[1] for entry in deg_dist_list_cleaned]
    plt.plot(x_axis, y_axis)
    plt.show()

def rename_to(df, new_columns):
    old_columns = df.schema.names
    return reduce(lambda df, idx:\
            df.withColumnRenamed(old_columns[idx], new_columns[idx]), xrange(len(old_columns)), df)
