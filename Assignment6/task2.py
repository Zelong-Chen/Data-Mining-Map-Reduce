from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import os
import time
import random
import math
import json
import binascii
from datetime import datetime
from statistics import median

################################### Inital Input & Output Files #######################################################
port_number = int(sys.argv[1])
output_file = sys.argv[2]

file = open(output_file, 'w')
file.write('Time,Ground Truth,Estimation' + '\n')
file.close()

# Initiate Hash Functions Variables
a = [random.randint(0, 1000) for i in range(40)]
b = [random.randint(0, 1000) for j in range(40)]
m = 500
hash_partition = 10


######################################## Function Definitions ##########################################################
def trailing(x):
    return len(x) - len(x.rstrip('0'))


def flajolet_algorithm(x):
    # Initiate Current Window and Variables
    current_window = x.collect()
    ground_truth = []
    # Initiate dict to store trailing zeros per hash function
    trailing_dict = {}
    for i in range(len(a)):
        trailing_dict[i] = []

    for item in current_window:
        # Convert string into json format
        temp = json.loads(item)
        # Extract city from item and add to ground truth
        ground_truth.append(temp['city'])

        # Transform string into int to be hashed
        city = int(binascii.hexlify(temp['city'].encode('utf8')), 16)
        # Hash city into multiple hash functions and transform into binary
        binary_val = [bin((a[i] * city + b[i]) % m) for i in range(len(a))]
        # Generate number of trailing zeros in each binary and store
        trailing_zeros = [trailing(x) for x in binary_val]
        for i in range(len(trailing_zeros)):
            trailing_dict[i].append(trailing_zeros[i])

    # Obtain 2^r estimates per hash function
    r_values = []
    for key, item in trailing_dict.items():
        r_values.append(math.pow(2, max(item)))

    # Calculate average of each partition group of hash functions
    avg = []
    i_in_partition = int(len(r_values) / hash_partition)
    for i in range(hash_partition):
        avg.append(sum(r_values[i * i_in_partition:(i + 1) * i_in_partition]) / i_in_partition)
    # Estimate of m = median of the averages of each partition
    estimate = round(median(avg))

    # Remove duplicates in ground truth
    ground_truth = set(ground_truth)
    
    # Output # unique cities within data stream (Ground Truth + Estimate)
    file = open(output_file, 'a')
    file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ',' + str(len(ground_truth)) + ',' + str(estimate) + '\n')
    file.close()


############################################ Inital Setups ############################################################
sc = SparkContext()
sc.setLogLevel('OFF')

# Start timer
t1 = time.time()

############################################ Stream Starts ############################################################
ssc = StreamingContext(sc, 5)
stream = ssc.socketTextStream("localhost", port_number).window(30,10)

# Each item rdd in stream Run Flajolet Martin Algorithm
stream.foreachRDD(flajolet_algorithm)

# Start Stream
ssc.start()
# Wait for Stream to terminate
ssc.awaitTermination()

# Output Runtime
t1_end = time.time()
print('Duration: ', t1_end - t1)
# End Program
sc.stop()
