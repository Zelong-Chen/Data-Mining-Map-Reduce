from pyspark import SparkContext
import sys
import os
import time
import random
import json
import binascii

################################### Inital Setup and Input files #######################################################
input_file_1 = sys.argv[1]
input_file_2 = sys.argv[2]
output_file = sys.argv[3]

sc = SparkContext()
sc.setLogLevel('OFF')

# Start timer
t1 = time.time()

# Input file 1: business_first.json
textRDD = sc.textFile(input_file_1)
rdd = textRDD.map(json.loads).map(lambda row: (row['city'])).distinct().persist()


######################################## Function Definitions ##########################################################
def hash_func(x, a, b, m):
    # Convert string into integer then apply hash functions
    hash_val = []
    if x != '':
        city = int(binascii.hexlify(x.encode('utf8')),16)
        # Hash Functions
        # h(x,a,b) = (ax+b) mod m
        hash_val = [(a[i]*city+b[i]) % m for i in range(len(a))]
    return (x, hash_val)


def check_filter_array(x, array):
    appeared = 0
    if all(i in array for i in x[1]):
        appeared = 1
    return (x[0], appeared)

################################### Generate Bloom Filter Bit Array  ###################################################
# Initiate Hash Functions Variables
a = [random.randint(0, 1000) for i in range(6)]
b = [random.randint(0, 1000) for j in range(6)]
m = 7000

# Generate Hash Positions from distinct city inputs & then generate set of hash positions to represent filter bit array
filter_array = rdd.map(lambda x: hash_func(x, a, b, m)).flatMap(lambda x: x[1]).distinct().collect()

########################### Check if Cities in second file appeared in first file  #####################################
# Input file 1: business_first.json
textRDD2 = sc.textFile(input_file_2)
rdd2 = textRDD2.map(json.loads).map(lambda row: (row['city'])).persist()

# Generate Hash values for each city in input file
hash_values = rdd2.map(lambda x: hash_func(x, a, b, m))

# If all hash values exist in filter array then predict 1 else predict 0
prediction = hash_values.map(lambda x: check_filter_array(x,filter_array)).map(lambda x: x[1]).collect()

################################## Output Prediction Results  #######################################################
with open(output_file, 'w') as file:
    for item in prediction:
        file.write(str(item) + ' ')


# Output Runtime
t1_end = time.time()
print('Duration: ', t1_end - t1)

sc.stop()
# End Program