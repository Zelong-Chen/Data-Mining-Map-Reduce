from pyspark import SparkContext
import sys
import json
import time

review_file = sys.argv[1]
output_file = sys.argv[2]
partition_type = str(sys.argv[3])
n_partitions = int(sys.argv[4])
n = int(sys.argv[5])

#review_file = 'review.json'
#output_file = 'output3.txt'
#partition_type = 'customized'
#n_partitions = int(5)
#n = int(1000)

if partition_type == 'default':
    # Start timer
    t1 = time.time();
    # Initialize Spark
    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel('OFF')

    # Review.json
    textRDD = sc.textFile(review_file)
    rdd = textRDD.map(json.loads).map(lambda row: (row['business_id'], row['review_id'])).persist()

    # Map by (business_id, 1) Reduce by adding count per business then filter by businesses with more than n reviews
    business = rdd.map(lambda row: (row[0], 1)).reduceByKey(lambda a, b: a+b).filter(lambda x:x[1]>n).collect()

    # Get partition info
    num_part = rdd.getNumPartitions()   # get number of partitions
    length = rdd.glom().map(len).collect()  # get length of each partition

    ans = []
    for i in business:
        ans.append([i[0], i[1]])

    # Output Dictionary
    ans_dict = {"n_partitions": num_part,
                "n_items": length,
                "result": ans}

    #print(ans_dict)
    # Output File
    with open(output_file, 'w') as file:
        file.write(json.dumps(ans_dict))

    t1_end = time.time()
    print(t1_end - t1)
    
    sc.stop()

elif partition_type == 'customized':
    # Start timer
    t2 = time.time();
    # Initialize Spark
    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel('OFF')

    # Review.json
    textRDD = sc.textFile(review_file)
    rdd = textRDD.map(json.loads).map(lambda row: (row['business_id'], row['review_id'])).persist()

    def business_partitioner(business_id):
        return hash(business_id)

    # Custom partition and partition func
    rdd2 = rdd.partitionBy(n_partitions, business_partitioner)

    # Map by (business_id, 1) Reduce by adding count per business then filter by businesses with more than n reviews
    business = rdd2.map(lambda row: (row[0], 1)).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] > n).collect()

    # Get partition info
    num_part = rdd2.getNumPartitions()  # get number of partitions
    length = rdd2.glom().map(len).collect()  # get length of each partition

    ans = []
    for i in business:
        ans.append([i[0], i[1]])

    # Output Dictionary
    ans_dict = {"n_partitions": num_part,
                "n_items": length,
                "result": ans}

    #print(ans_dict)
    # Output File
    with open(output_file, 'w') as file:
        file.write(json.dumps(ans_dict))

    t2_end = time.time()
    print(t2_end - t2)
    
    sc.stop()