from pyspark import SparkContext
import json
import random
import sys
import time

input_file = sys.argv[1]
output_file = sys.argv[2]

sc = SparkContext()
sc.setLogLevel('OFF')

# Start timer
t1 = time.time();

# Load Input files
textRDD = sc.textFile(input_file)
rdd = textRDD.map(json.loads).map(lambda row: (row['user_id'], row['business_id'], row['stars'])).persist()
########################################################################################################################
# Characteristic Matrix
########################################################################################################################
# Generate list of users and business and use index values to represent char matrix
users = rdd.map(lambda x: x[0]).distinct().collect()

business_user_ratings = rdd.map(lambda x: (x[1], [users.index(x[0])])).reduceByKey(lambda a, b: a+b)
business_user_dict = business_user_ratings.collectAsMap()     # Save a copy as dict
# Output here: ('bZMcorDrciRbjdjRyANcjA', [11221, 20787, 7760, 18079, 2079, 18260, 3076, 15279, 9089, 487, 11221, 899,
# Business and the index of user who rated

########################################################################################################################
# MinHash
########################################################################################################################
# Hash Functions
# h(x,a,b) = ((ax+b) mod p) mod m
# x is key you want to hash
# a is any number you can choose between 1 to p-1 inclusive.
# b is any number you can choose between 0 to p-1 inclusive.
# m is a max possible value you want for hash code + 1
# Random Generated Numbers
m = 26189
a = [random.randint(0, m) for j in range(32)]
b = [random.randint(0, m) for j in range(32)]


def hash_func(x, a, b, m):
    # Generate hash value by taking the min hash value from the hashed user_indexes
    # Then repeat for every hash function, generating a list of hash values for the business
    min_val = [min((a[i]*k+b[i]) % m for k in x[1]) for i in range(0, len(a), 1)]
    return (x[0], min_val)


signatures = business_user_ratings.map(lambda x: hash_func(x, a, b, m))
# Output here: ('bZMcorDrciRbjdjRyANcjA', [357, 62, 306, 1864, 3029, 2513, 727, 2160, 157, 1487, 274, 2053, 2781, 265,
# Business and their signatures corresponding to # of hash func

########################################################################################################################
# Locality-Sensitive Hashing (LSH)
########################################################################################################################
n = len(a)              # n = number of hash functions
rows = 1                # row = number of rows in one band
bands = int(n/rows)      # band = number of bands


def split_by_bands(x, band):
    # Output: (Key, Value)
    # Key: (band #, signature)
    # Value: [business]
    temp = []
    for i in range(0, band, 1):
        temp.append(((i, (tuple(x[1][(i)*rows:(i+1)*rows]))), [x[0]]))
    return temp


def candidate_pair(x):
    # Generate all combinations of pairs from list of business candidates
    temp = []
    bus_list = sorted(x[1])
    for i in range(0, len(bus_list)-1, 1):
        for j in range(i+1, len(bus_list), 1):
            temp.append(((bus_list[i], bus_list[j]), 1))
    return temp


def jaccard_sim(x, dictionary):
    u1 = set(dictionary[x[0]])
    u2 = set(dictionary[x[1]])
    temp = float(len(u1.intersection(u2))/len(u1.union(u2)))
    return (x[0], x[1], temp)


# Generate list of business with same signature in one band
same_in_band = signatures.flatMap(lambda x: split_by_bands(x, bands))\
    .reduceByKey(lambda x, y: x+y)\
    .filter(lambda x: len(x[1]) > 1)
# Output here: ((4, (4441, 1536, 3531, 3996)), ['5WQZRY8UeN-lUKoBOn7yLQ', 'c7yTGHRgJQ2SquTFv95zNQ'])

# Generate list of candidates business pairs to compare
candidates = same_in_band.flatMap(lambda x: candidate_pair(x))\
    .reduceByKey(lambda x,y: x)
# Output here: (('ScOFpLvZeXNFnp7_ikQf_A', 'lYFpDTiVMNrpkpFyIEcc_w'), 1)


j_sim = candidates.map(lambda x: jaccard_sim(x[0], business_user_dict))\
    .filter(lambda x: x[2] >= 0.05)\
    .collect()

   

# Output File
with open(output_file, 'w') as file:
    for item in j_sim:
        ans_dict = {"b1": item[0], "b2": item[1], "sim": item[2]}
        file.write(json.dumps(ans_dict) + '\n')


# Output Runtime
t1_end = time.time()
print('Duration: ', t1_end - t1)

sc.stop()