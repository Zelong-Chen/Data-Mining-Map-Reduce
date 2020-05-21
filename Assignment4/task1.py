from pyspark import SparkContext, SQLContext
import random
import sys
import os
import time

# Graphframes Library
# pip install graphframes
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
from graphframes import *

filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]
output_file = sys.argv[3]

sc = SparkContext('local[3]')
sqlContext = SQLContext(sc)
sc.setLogLevel('OFF')

# Start timer
t1 = time.time();

# Read input file and remove csv header
textRDD = sc.textFile(input_file)
rdd = textRDD.map(lambda x: tuple(x.split(','))).filter(lambda x: x[0]!= "user_id").persist()

# List of Unique Users
users = rdd.map(lambda x: (x[0])).distinct()


##################################### Find Valid Edge Pairs  ########################################################
def find_edge_pairs(x, dictionary, thresh):
    res = []
    for item, value in dictionary.items():
        if x != item:
            bus_list_1 = dictionary[x]
            bus_list_2 = dictionary[item]
            inter = set(bus_list_1).intersection(set(bus_list_2))
            if len(inter) >= thresh:
                res.append(item)
    return (x, res)


user_business_dict = rdd.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b).collectAsMap()

# Edge Pairs (Intersection >= 7 on reviewed businesses)
users_edge = users.map(lambda x: find_edge_pairs(x, user_business_dict, filter_threshold)).filter(lambda x: len(x[1]) > 0)
    

##################################### Vertices DF (User_ids) #########################################################
users_nodes = users_edge.map(lambda x: (x[0],)).distinct()

# Generate Vertices DF
vertices = sqlContext.createDataFrame(users_nodes, ['id'])

##################################### Edge DF (Undirected) #########################################################
edges_formated = users_edge.flatMapValues(lambda x: x)

# Generate Edge DF
edges = sqlContext.createDataFrame(edges_formated, ['src','dst'])


####################################### Generate Graph #################################################################
g = GraphFrame(vertices, edges)
results = g.labelPropagation(maxIter=5)

test = results.rdd.map(lambda x: (x[1],[x[0]]))\
   .reduceByKey(lambda a,b: a+b)\
   .map(lambda x: (x[0],sorted(x[1])))\
   .sortBy(lambda x: x[1][0])\
   .sortBy(lambda x: len(x[1])).collect()


################################## Output Detected Communities  #######################################################
with open(output_file, 'w') as file:
   for item in test:
       file.write(str(item[1]).replace("[","").replace("]","") + '\n')


# Output Runtime
t1_end = time.time()
print('Duration: ', t1_end - t1)

sc.stop()
# End Program