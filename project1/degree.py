#!/bin/python

# Degree Distribution
# The degree distribution is a measure of the frequency of nodes that have a
# certain degree. Implement a function degreedist, which takes a GraphFrame
# object as input and computes the degree distribution of the graph. The 
# function should return a DataFrame with two columns: degree and count. The 
# degree column should span from 0 to largest degree in the graph and the 
# count should be the number of nodes that have that degree
# http://graphframes.github.io/user-guide.html

# Keep in mind the pyspark tutorial showed how to do sometihng similar 
import sys, errno, ipdb

from pyspark import SparkContext
from pyspark.sql import SQLContext, types
from pyspark.sql.functions import *

from graphframes import *
from graphframes.examples import Graphs
import networkx as netx

import matplotlib.pyplot as plt

debug = False


def degreedist(g):
    # Get the counts. This is easy. 
    degree_distribution = g.inDegrees.groupBy('inDegree').count()

    # Reassign the column names
    # The columns via inDegree will return as [inDegree, count]
    degree_distribution = degree_distribution.select(col('inDegree').alias('degree'), col('count').alias('count'))

    # deg_dist_list = g.inDegrees.select('inDegree').map(lambda degree: (degree, 1)).reduceByKey(lambda a, b: a + b).collect()
    # deg_dist_list_cleaned = sorted([(entry[0].inDegree, entry[1]) for entry in deg_dist_list], key=lambda x: x[1])
    # return sqlc.createDataFrame(deg_dist_list_cleaned, ['degree', 'count'])
    return degree_distribution

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
    # make a series of tuples formed as (X, X), (Y, Y), ... and grab the unique ones
    vertices = entries.map(lambda entry: (entry.split(delim)[0], entry.split(delim)[0])).distinct().collect()
    vertices = sqlc.createDataFrame(vertices, ['id', 'name'])
    if debug: vertices.show()
    edges_1 = entries.map(lambda entry: (entry.split(delim)[0], entry.split(delim)[1]))
    edges_2 = entries.map(lambda entry: (entry.split(delim)[1], entry.split(delim)[0]))
    edges_1 = sqlc.createDataFrame(edges_1, ['src', 'dst'])
    edges_2 = sqlc.createDataFrame(edges_2, ['src', 'dst'])
    edges = edges_1.unionAll(edges_2)
    if debug: edges.show() 
    return GraphFrame(vertices, edges)


def make_plot(g):
    deg_dist_list = g.inDegrees.select('inDegree').map(lambda degree: (degree, 1)).reduceByKey(lambda a, b: a + b).collect()
    deg_dist_list_cleaned = sorted([(entry[0].inDegree, entry[1]) for entry in deg_dist_list], key=lambda x: x[0])
    x_axis = [entry[0] for entry in deg_dist_list_cleaned]
    y_axis = [entry[1] for entry in deg_dist_list_cleaned]
    plt.plot(x_axis, y_axis)
    plt.show()

# Generate 10 random histograms 
# In the form of (node id, degree)
def random_histograms():
    graphs = [netx.fast_gnp_random_graph(1000, .5) for i in range(0, 10)]
    histograms = []
    hist = []
    for g in graphs:
        for n in g.nodes():
            hist.append((n, len(g.edges(n))))
        histograms.append(hist)
        hist = []
    return histograms

if __name__ == '__main__':
    # If ran in interpreter, will need to create the Spark Context 
    if 'sc' not in locals() or 'sc' not in globals():
       sc = SparkContext('local', 'degreedist')
    sqlc = SQLContext(sc)
    
    # Check through and clean up args
    if len(sys.argv) < 2:
        print 'ERROR: Inappropriate amount of args.'
        sys.exit(errno.EINVAL)

    # enable debugging
    if '-d' in sys.argv or '--debug' in sys.argv:
        debug = True
        sys.argv.remove('-d') if '-d' in sys.argv else sys.argv.remove('--debug')

    if 'degree.py' in sys.argv[0]:
        sys.argv.pop(0)
    graph = make_graph(sys.argv[0], sc, sqlc)
    dist = degreedist(graph)
    dist.show()


