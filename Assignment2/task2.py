from pyspark import SparkContext
import math
import itertools
import sys
import os
import time

filter_support = int(sys.argv[1])
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]

sc = SparkContext('local[*]', 'task2')
sc.setLogLevel('OFF')

# Start timer
t1 = time.time();

# Read input file and remove csv header
textRDD = sc.textFile(input_file)
header = textRDD.first()

# user: [businesses,.....] and filter out users who reviewed less than filter_support businesses
rdd = textRDD.filter(lambda line: line != header)\
    .distinct() \
    .map(lambda line: (line.split(',')[0], {line.split(',')[1]})) \
    .reduceByKey(lambda a, b: a | b)\
    .filter(lambda x: len(x[1]) >= filter_support).persist()

# Get partition info
p = rdd.getNumPartitions()  # get number of partitions
# Extract basket information from each transaction
baskets = rdd.values()
# Threshold = support/num of partitions
threshold = math.floor(support / p)


#######################################################################################################################
# Functions for Phase 1 SON Algorithm using Apriori
#######################################################################################################################
def apriori(iterator, threshold):
    # Read in baskets in partition
    baskets_in_partition = list(iterator)
    singleton = {}
    # Access individual basket
    for basket in baskets_in_partition:
        # Access Items in basket
        for item in basket:
            # If item in counter then increase count if not, create counter in singleton dict
            if item in singleton:
                # Increase counter for item in dictionary
                singleton[item] += 1
            else:
                # Add item and 1 count to dictionary
                singleton[item] = 1
    # items with counter >= threshold is a frequent item
    prev_freq = sorted([str(v) for v in singleton if singleton[v] >= threshold])
    num_singleton = len(prev_freq)
    # Emit frequent singletons
    frequent_item = [(str(x),) for x in prev_freq]
    for i in frequent_item:
        yield (i, 1)

    # Initiate k=2 to find pairs
    k = 2
    while 1:
        # Generate Frequent Items using combinations from freq_single and eliminating combos with subsets that are not
        # frequent and not frequent in current partition of data
        current_freq = generate_freq_item(prev_freq, baskets_in_partition, threshold, k)
        # Stop when no more possible frequent item
        if (len(current_freq) == 0) or (num_singleton == k):
            break
        # Emit frequent item found during current k-combo and increment k
        prev_freq = list(dict.fromkeys(current_freq))
        for i in prev_freq:
            yield (i, k)
        k += 1


def generate_freq_item(prev_freq, baskets_in_partition, threshold, k):
    # Generate possible combos from using prev_freq items and prune by counting occurrence in partitioned baskets
    # If size k combo occurred more than k times in prev_item pass stage 1
    combos = []
    freq_items = []
    # Create combinations of previous frequent items and only consider combinations when the length of union equal
    # current k combination
    for item in itertools.combinations(prev_freq, 2):
        # For pairs takes all combo
        if k == 2:
            combos.append(item)
        else:
            temp = tuple(sorted(set(item[0]).union(set(item[1]))))
            if len(temp) == k:
                combos.append(temp)
    # Remove duplicates
    combos = list(dict.fromkeys(combos))

    # Further Prune by counting occurrence in partitioned baskets
    # If occurrence >= threshold then add to frequent item and move on to next candidate combo
    for i in combos:
        count = 0
        for basket in baskets_in_partition:
            if set(i).issubset(basket):
                count += 1
                if count >= threshold:
                    freq_items.append(i)
                    break
    return freq_items


def output_sol(sol):
    output = []
    max_len = max(len(x) for x in sol)
    for i in range(1, max_len + 1):
        arr = [x for x in sol if len(x) == i]
        line = ''
        for j in arr:
            line += str(j).replace(',)', ')') + ','
        output.append(line)
    return output


#######################################################################################################################
# SON Phase 1
#######################################################################################################################
SON_Phase1 = baskets.mapPartitions(lambda x: apriori(x, threshold)) \
    .reduceByKey(lambda a, b: a) \
    .sortByKey() \
    .sortBy(lambda x: len(x[0]), ascending=True) \
    .keys() \
    .collect()

candidates = output_sol(SON_Phase1)


#######################################################################################################################
# Functions for Phase 2 SON Algorithm
#######################################################################################################################
def verify_freq(iterator, frequents):
    # Read in baskets in partition
    baskets_in_partition = list(iterator)
    # Initiate counters
    counter = {}
    for i in frequents:
        counter[i] = 0

    for basket in baskets_in_partition:
        for item in frequents:
            if set(item).issubset(basket):
                counter[item] += 1

    # items with counter >= threshold is a frequent item
    for item, count in counter.items():
        yield (item, count)


#######################################################################################################################
# SON Phase 2
#######################################################################################################################
SON_Phase2 = baskets.mapPartitions(lambda x: verify_freq(x, SON_Phase1)) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda x: x[1] >= support) \
    .sortByKey() \
    .sortBy(lambda x: len(x[0]), ascending=True) \
    .keys() \
    .collect()

freq_item = output_sol(SON_Phase2)

#######################################################################################################################
# Output solution to file
#######################################################################################################################
with open(output_file, 'a') as f:
    f.write('Candidates: \n')
    f.writelines(s[:-1] + '\n' + '\n' for s in candidates)
    f.write('Frequent Itemsets: \n')
    f.writelines(s[:-1] + '\n' + '\n' for s in freq_item)

# Output Runtime
t1_end = time.time()
print('Duration: ', t1_end - t1)

sc.stop()