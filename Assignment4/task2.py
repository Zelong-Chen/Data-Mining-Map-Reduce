from pyspark import SparkContext
import random
import sys
import os
import time
import copy

filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]
betweeen_output_file = sys.argv[3]
community_output_file = sys.argv[4]

sc = SparkContext()
sc.setLogLevel('OFF')

# Start timer
t1 = time.time();

# Read input file and remove csv header
textRDD = sc.textFile(input_file)
rdd = textRDD.map(lambda x: tuple(x.split(','))).filter(lambda x: x[0]!= "user_id").persist()

##################################### Vertices DF (User_ids) #########################################################
# List of Unique Users
users = rdd.map(lambda x: (x[0])).distinct()


##################################### Edges DF (User_ids pairs) ########################################################
def find_edge_pairs(x, dictionary, thresh):
    res = []
    for item, value in dictionary.items():
        if x != item:
            test = tuple((x,item))
            if test[0] == x:
                bus_list_1 = dictionary[x]
                bus_list_2 = dictionary[item]
                inter = set(bus_list_1).intersection(set(bus_list_2))
                if len(inter) >= thresh:
                    res.append(item)
    return (x, res)


# Edge DF (Intersection >= 7 on reviewed businesses)
user_business_dict = rdd.map(lambda x: (x[0],[x[1]])).reduceByKey(lambda a, b: a+b).collectAsMap()

temp = users.map(lambda x: find_edge_pairs(x,user_business_dict,filter_threshold)).filter(lambda x: len(x[1]) > 0)
# Save User Edge Links
users_edge = temp.collectAsMap()
# Output: ( User, [list of connected users] )
# Save User Nodes in Graph
users_nodes = temp.map(lambda x: x[0]).distinct()

##################################### Betweenness Calculation #########################################################
def betweenness(x, dictionary):
    res = []
    # Initalize Tree with Root Node + Child Node
    root_node = [x]
    tree = {0: root_node}
    used_nodes = root_node

    # Expand Tree
    tree_level = 1
    num_shortest_path = {x: 1}
    child_dict = {}
    while 1:
        child_list = []
        for item in root_node:
            child_dict[item] = [node for node in dictionary[item] if node not in used_nodes]
            child_list += child_dict[item]

        # If reached end of tree; break
        if len(child_list) == 0:
            # Go back up a level since current level empty
            tree_level -= 1
            break
        # Update tree level with child nodes
        tree[tree_level] = list(set(child_list))
        # Update used_nodes array with
        used_nodes = used_nodes + tree[tree_level]
        # Increment # shortest path to parent node
        for i in child_list:
            num_shortest_path[i] = num_shortest_path.get(i, 0) + 1
        # Move down one level in tree
        root_node = child_list
        tree_level += 1

    # Initialize dict with nodes and pass through value of 0
    pass_through_dict = {}
    for i in used_nodes:
        pass_through_dict[i] = 0

    for i in range(tree_level, 0, -1):
        child_level = tree[i]
        parent_level = tree[i - 1]
        # Every parent of current level
        for parent in parent_level:
            if len(child_dict[parent]) > 0:
                for child in child_dict[parent]:
                    # (Current node + Previous passes from child) / # shortest path up to parent
                    edge_val = (num_shortest_path[parent] * (1 + pass_through_dict[child])) / (num_shortest_path[child])
                    # Update # of pass through with parent node
                    pass_through_dict[parent] += edge_val
                    temp = sorted([parent, child])
                    res.append(((temp[0], temp[1]), edge_val))
    return res


# Calculate Betweenness using every node as root then suming all values and divide by 2 to get edge betweeness value
edge_betweenness = users_nodes.flatMap(lambda x: betweenness(x, users_edge))\
    .reduceByKey(lambda a,b: a+b)\
    .map(lambda x: (x[0],float(x[1]/2)))\
    .sortByKey()\
    .sortBy(lambda x: x[1], ascending=False)

temp = edge_betweenness.collect()
################################## Output Original Graph Betweenness  ##################################################
with open(betweeen_output_file, 'w') as file:
    for item in temp:
        file.writelines(str(item[0]) + ', ' + str(item[1]) + '\n')


####################################### Community Detection  ###########################################################
def community_detection(adjacency):
    nodes = list(adjacency.keys())
    res = []
    counted_nodes = []
    for i in range(len(nodes)):
        if nodes[i] not in counted_nodes:
            root_node = [nodes[i]]
            # Used_nodes initialized with two root nodes
            used_nodes = [nodes[i]]
            while 1:
                child_list = []
                for item in root_node:
                    child_list += [node for node in adjacency[item] if node not in used_nodes]
                if len(child_list) == 0:
                    break
                # Update used_nodes array with
                used_nodes += list(set(child_list))
                # Move down one level in tree
                root_node = child_list
            res.append(used_nodes)
            # Save nodes that are already in community
            for item in used_nodes:
                counted_nodes.append(item)
    return res


def calc_modularity(adjacency_org, communities, m):
    Q = 0
    for community in communities:
        for i in community:
            # Q = sum_i sum_j (A_ij - k_i*k_j / 2m)
            for j in community:
                if j in adjacency_org[i]:
                    A_ij = 1
                else:
                    A_ij = 0
                 
                k_i = len(adjacency_org[i])
                k_j = len(adjacency_org[j])

                Q += A_ij - float((k_i * k_j) / (2 * m))
    Q = Q / float(2 * m)
    return round(Q, 4)


# Get nodes in graph
nodes_in_graph = sc.parallelize(list(users_edge.keys()))
# Total number of nodes in graph
num_nodes = nodes_in_graph.count()

# Get First edge cut with highest betweenness
top_edge = edge_betweenness.take(1)[0][0]

# Create Original Adjacency Matrix & Mutable Adjacency Matrix
adjacency_mutable = copy.deepcopy(users_edge)
adjacency_original = users_edge.copy()
# m = Number of Edges in Original Graph
m = edge_betweenness.count()

# Initialize Variables
max_mod = -1
community_res = []
while 1:
    # Remove Edge in Adjacency_mutable
    adjacency_mutable[top_edge[0]].remove(top_edge[1])
    adjacency_mutable[top_edge[1]].remove(top_edge[0])

    # Find Partitioned Communities
    partitioned_community = community_detection(adjacency_mutable)

    # Find Modularity Score
    mod_score = calc_modularity(adjacency_original, partitioned_community, m)

    # Reached Elbow Point + Some Slack
    if (max_mod - 0.1) > mod_score:
        break
    elif mod_score >= max_mod:
        # Update Max Modularity Score and Community if current mod score is higher
        max_mod = mod_score
        community_res = partitioned_community

    # If partitioned communities broken up the same number of nodes then done
    if len(partitioned_community) == num_nodes:
        break

    # Calculate New Betweenness in Partitioned Communities
    edge_betweenness = nodes_in_graph.flatMap(lambda x: betweenness(x, adjacency_mutable))\
        .reduceByKey(lambda a,b: a+b)\
        .map(lambda x: (x[0],float(x[1]/2)))\
        .sortByKey()\
        .sortBy(lambda x: x[1], ascending=False)
    # Next Edge to Cut
    top_edge = edge_betweenness.take(1)[0][0]

# Sort Solution for Output
sort_community = [sorted(i) for i in community_res]
sort_first_element = sorted(sort_community, key=lambda x:x[0])
sort_len = sorted(sort_first_element, key=len)

################################## Output Detected Communities  #######################################################
with open(community_output_file, 'w') as file:
   for item in sort_len:
       file.write(str(item).replace("[","").replace("]","") + '\n')


# Output Runtime
t1_end = time.time()
print('Duration: ', t1_end - t1)
