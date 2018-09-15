# Articulation points, or cut vertices, are vertices in the graph that, when removed, create more
# components than there were originally. For example, in the simple chain 123, there
# single component. However, if vertex 2 were removed, there would be 2 components. Thus,
# vertex 2 is an articulation point.

# Implement the function articulations, which takes a GraphFrame object as input and finds all
# the articulation points of a graph. The function should return a DataFrame with two columns, id
# and articulation, where articulation is a 1 if the node is an articulation point, otherwise a 0.

from graphframes import *
from pyspark import SparkContext
from pyspark.sql.functions import countDistinct, col, UserDefinedFunction, udf
from pyspark.sql.types import IntegerType

def articulations(g):
    """Determines which points are articulation points in graph `g` (1 if so, 0 otherwise) and 
       returns a dataframe recording results in the form of (id, articulation).
    """

    # Need to first build the 'base' dataframe to append to 

    # Grab the first node
    first_node = g.vertices.filter(col('id') == g.vertices.first()['id']).select('id')
    # Grab the original connected component count of `g`
    orig_comp_cnt = (
            #count_distinct_components(g.connectedComponents().drop('name'))
            #.withColumnRenamed('component', 'orig_comp_cnt')
            count_distinct_components(g.connectedComponents().select('component'))
            .first()['component']
    )
    # calculate the connected component count if the first node is removed
    new_comp_cnt = (
            count_distinct_components(sub_graph(first_node.first()['id'], g).connectedComponents())
            .withColumnRenamed('component', 'new_comp_cnt')
    )

    # Bind them together but empty the results. This is a janky way of building the table 
    #aggregated_results = first_node.join(orig_comp_cnt).join(new_comp_cnt).filter('False')
    aggregated_results = first_node.join(new_comp_cnt).filter('False')


    # Now to iterate through them 

    # grab the vertices so we can iterate through them. 
    nodes = g.vertices.map(lambda row: row['id']).collect()
    # For each node, create a subgraph form `g` with node removed and calculate the connected components
    # appending the results to the aggregated_results
    for node in nodes:
        sub_g = sub_graph(node, g)
        node_id = g.vertices.select('id').filter(col('id') == node)
        new_comp_cnt = count_distinct_components(sub_g.connectedComponents()).withColumnRenamed('component', 'new_comp_cnt')
        #to_add = node_id.join(orig_comp_cnt).join(new_comp_cnt)
        to_add = node_id.join(new_comp_cnt)
        aggregated_results = aggregated_results.unionAll(to_add)

    
    # Merge the old and new count 
    #is_art_point = udf(lambda x,y: 1 if x - y != 0 else 0, IntegerType())
    is_art_point = udf(lambda x: 1 if x - orig_comp_cnt != 0 else 0, IntegerType())
    aggregated_results = (
            #aggregated_results.withColumn('articulation', is_art_point(aggregated_results['orig_comp_cnt'], aggregated_results['new_comp_cnt']))
            #.drop('orig_comp_cnt')
            #.drop('new_comp_cnt')
            aggregated_results.withColumn('articulation', is_art_point(aggregated_results['new_comp_cnt']))
            .drop('new_comp_cnt')
    )
    return aggregated_results


def count_distinct_components(components):
    """Count the distinct components. 'components' must be a 
       RDD made from graph.connectedComponents()"""
    return  (
        # aggregate all the components based on counting the distinct components
        components.agg(
            countDistinct(components['component'])
            .alias('component')   # rename to `component`
        )
        #.first()     # returns a dataframe of the count
        #.first()['component']     # returns a dataframe of the count
    )


def sub_graph(to_leave_out, g):
    """Create a subgraph from the original graph 'g' with the
       node 'to_leave_out' left out. 
       
       Apparently, since Spark 1.4.1, composite logical 
       expressions are just not a thing and will cause exceptions to be thrown despite 
       the pyspark.sql.DataFrame.filter docs not forbidding it. As such, need to chain 
       multiple filters together to emulate logical 'or' filtering."""
    v = g.vertices.filter(col('id') != to_leave_out)
    e = g.edges.filter(col('src') != to_leave_out).filter(col('dst') != to_leave_out)
    return GraphFrame(v, e)


def determine_articulation(node, g, orig_comp_cnt):
    """First creates a sub_graph of `g` with `node` left out then "count_distinct_components"
    against the new "sub_graph". Returns 1 if the new component count is greater than the 
    orig_comp_cnt and 0 otherwise"""
    sub_g = sub_graph(node, g)
    new_comp_cnt = count_distinct_components(sub_g.connectedComponents().select('id'))
    return 1 if new_comp_cnt > orig_comp_cnt else 0
