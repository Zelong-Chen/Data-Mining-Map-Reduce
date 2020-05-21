from pyspark import SparkContext
import sys
import os
import time
import copy
import random
import math
import json

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

input_dir = sys.argv[1]
n_cluster = int(sys.argv[2])
cluster_res = sys.argv[3]
intermediate_res = sys.argv[4]

################################### Inital Setup and Input files #######################################################
input_files_arr = os.listdir(input_dir)
input_files_arr = sorted(input_files_arr)


# Save Intermediate Results
nof_cluster_discard = []
nof_point_discard = []
nof_cluster_compression = []
nof_point_compression = []
nof_point_retained = []

sc = SparkContext()
sc.setLogLevel('OFF')

# Start timer
t1 = time.time();

input_path = os.path.join(input_dir, input_files_arr[0])
print(input_path)

# Load File
textRDD = sc.textFile(input_path)
rdd = textRDD.map(lambda x: tuple(x.split(','))).map(
    lambda x: tuple([int(x[0]), tuple([float(i) for i in x[1:]])])).persist()
# (point index, (features.....))


######################################## Function Definitions ##########################################################
def distance_formula(a, b):
    dist = 0
    for i in range(0, len(a), 1):
        dist += (a[i] - b[i]) ** 2
    return math.sqrt(dist)


def assign_cluster(x, centroids):
    min_dist = 10000000
    cluster_assign = -1
    for i in range(len(centroids)):
        distance = distance_formula(x[1], centroids[i][1])
        if distance <= min_dist:
            min_dist = distance
            cluster_assign = i
    return (cluster_assign, x)


def k_means(points, centroids, k, indexes, dictionary):
    iteration = 0
    while 1:
        # Assign points to closest cluster centriod
        cluster_assignment = points.map(lambda x: assign_cluster(x, centroids))
        cluster_points = cluster_assignment.map(lambda x: (x[0], [x[1][1]])).reduceByKey(lambda a, b: a + b)
        # Generate new centroids by taking mean of all points in cluster
        new_centroid = cluster_points.map(lambda x: tuple([x[0], tuple([sum(y) / len(y) for y in zip(*x[1])])])) \
            .collect()

        # If # new centroid is less than k, regenerate centroid by randomly sampling original data
        if len(new_centroid) != k:
            test = random.sample(indexes, k - len(new_centroid))
            remain_cluster = [i[0] for i in new_centroid]
            need_cluster = [i for i in range(0, k) if i not in remain_cluster]
            for i in range(len(test)):
                new_centroid.append(tuple([need_cluster[i], dictionary[test[i]]]))
        new_centroid = sorted(new_centroid)

        # Break if convergence met
        if new_centroid == centroids:
            break
        else:
            # Break if centroids only changed minimally
            test_dist = []
            for i in range(0, len(centroids), 1):
                test_dist.append(distance_formula(centroids[i][1], new_centroid[i][1]))
            if all(x <= 8 for x in test_dist):
                break
            # Else update centriods
            centroids = new_centroid
        # Break if iteration threshold met
        iteration += 1
        if iteration == 100:
            break
    return cluster_assignment, iteration


def summarize_cluster(x):
    cluster_num = x[0]
    points = x[1]
    ds_n = len(points)
    ds_sum = []
    ds_sumsq = []
    ds_points = [x[0] for x in points]
    for j in range(len(points[0][1])):
        ds_sum.append(sum([x[1][j] for x in points]))
        ds_sumsq.append(sum([x[1][j] ** 2 for x in points]))
    return (cluster_num, [ds_n, ds_sum, ds_sumsq, ds_points])


def generate_centriod_std(x):
    cluster = x[0]
    n = x[1][0]
    sum_var = x[1][1]
    sumsq_var = x[1][2]

    centroid = [x / n for x in sum_var]
    # SUMSQ_i / N     (E[x^2])
    std_1 = [x / n for x in sumsq_var]
    # (SUM_i / N)^2   (E[x])^2
    std_2 = [(x / n) ** 2 for x in sum_var]
    # Std = sqrt( E[x^2] - (E[x])^2 )
    std = [math.sqrt(a - b) for a, b in zip(std_1, std_2)]

    return (cluster, [centroid, std])


def check_cluster(x, cent_std):
    point = x[1]
    dimension = len(point)
    alpha = 2
    threshold = alpha * math.sqrt(dimension)
    cluster = -1
    for i in range(len(cent_std)):
        temp_sum = 0
        centroid = cent_std[i][1][0]
        std = cent_std[i][1][1]
        for j in range(dimension):
            if std[j] == 0:
                temp_sum += ((point[j] - centroid[j]) / (1)) ** 2
            else:
                temp_sum += ((point[j] - centroid[j]) / (std[j])) ** 2
        d = math.sqrt(temp_sum)
        # Assign to Closest Cluster that also satisfy mahalanobis dist < alpha*sqrt(dimension)
        if d < threshold:
            cluster = cent_std[i][0]
            break
    return (cluster, [x])


def update_summary(temp_sum, ds_summary):
    for i in temp_sum:
        cluster = i[0]
        n = i[1][0]
        sum_var = i[1][1]
        sumsq_var = i[1][2]
        points = i[1][3]
        for j in ds_summary:
            if cluster == j[0]:
                j[1][0] += n
                for k in range(len(sum_var)):
                    j[1][1][k] += sum_var[k]
                    j[1][2][k] += sumsq_var[k]
                j[1][3] += points
                break
    return sorted(ds_summary)


def check_if_merge(new, old):
    res = []
    for i in range(len(new)):
        new_cluster_num = new[i][0]
        c_1 = new[i][1][0]
        std_1 = new[i][1][1]
        dimension = len(c_1)
        alpha = 2
        threshold = alpha * math.sqrt(dimension)
        cluster = -1
        for j in range(len(old)):
            old_cluster_num = old[j][0]
            c_2 = old[j][1][0]
            std_2 = old[j][1][1]
            temp_sum_1 = 0
            temp_sum_2 = 0
            for k in range(dimension):
                if std_2[k] == 0:
                    temp_sum_1 += ((c_1[k] - c_2[k]) / (1)) ** 2
                else:
                    temp_sum_1 += ((c_1[k] - c_2[k]) / (std_2[k])) ** 2
                if std_1[k] == 0:
                    temp_sum_2 += ((c_2[k] - c_1[k]) / (1)) ** 2
                else:
                    temp_sum_2 += ((c_2[k] - c_1[k]) / (std_1[k])) ** 2
            d = math.sqrt(min([temp_sum_1, temp_sum_2]))
            # Assign to Closest Cluster that also satisfy mahalanobis dist < alpha*sqrt(dimension)
            if (d < threshold):
                cluster = old_cluster_num
                break
        res.append(tuple([cluster, new_cluster_num]))
    return res


def combine_cluster(res, new_cs_summary, cs_summary):
    for i in res:
        old_clus = i[0]
        new_clus = i[1]
        new_index = [y[0] for y in new_cs_summary].index(new_clus)
        dim = len(cs_summary[0][1][1])
        if old_clus != -1:
            old_index = [y[0] for y in cs_summary].index(old_clus)
            cs_summary[old_index][1][0] += new_cs_summary[new_index][1][0]
            for j in range(dim):
                cs_summary[old_index][1][1][j] += new_cs_summary[new_index][1][1][j]
                cs_summary[old_index][1][2][j] += new_cs_summary[new_index][1][2][j]
            cs_summary[old_index][1][3] += new_cs_summary[new_index][1][3]
        else:
            length_cluster = len(cs_summary)+100
            temp = tuple([length_cluster, new_cs_summary[new_index][1]])
            cs_summary.append(temp)
    return cs_summary


def outlier(x, n_cluster):
    point = x[1]
    cluster = x[0]
    if cluster >= n_cluster:
        cluster = -1
    return (point, cluster)


def merge_final(cs, ds):
    res = []
    for i in cs:
        min_dist = 10000000
        cluster_assign = -1
        cs_cluster_num = i[0]
        cs_centriod = i[1][0]
        for j in ds:
            ds_cluster_num = j[0]
            ds_centriod = j[1][0]
            distance = distance_formula(cs_centriod, ds_centriod)
            if distance <= min_dist:
                min_dist = distance
                cluster_assign = ds_cluster_num
        res.append(tuple([cluster_assign, cs_cluster_num]))
    return res

################################ K-Means to Generate DS Cluster #######################################################
# Sample 20% of first file for K-Means
total_data = rdd.count()
sample_amount = int(total_data * 0.2)
random.seed(7)
sampled_indexes = random.sample(range(total_data), sample_amount)
sample_points = rdd.filter(lambda x: x[0] in sampled_indexes)


# Define K cluster for K-means
k = n_cluster
# Generate Initial Clusters
cluster_init = random.sample(sampled_indexes, k=6*k)
centroids_sample = sample_points.filter(lambda x: x[0] in cluster_init).map(lambda x: x[1]).collect()
centroids = []
for i in range(k):
    points = centroids_sample[i*6:(i+1)*6]
    avg = tuple([sum(y) / len(y) for y in zip(*points)])
    centroids.append(tuple([i,avg]))

# Run K-Means Algorithm
cluster_assignment, iteration = k_means(sample_points, centroids, k, sampled_indexes, sample_points.collectAsMap())

temp = cluster_assignment.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b)

# Summarized DS set
temp_summary = temp.map(lambda x: summarize_cluster(x)).sortBy(lambda x: x[0])
ds_summary = temp_summary.collect()
ds_cent_std = temp_summary.map(lambda x: generate_centriod_std(x)).sortBy(lambda x: x[0]).collect()

############################### K-Means to Generate CS Cluster & RS Points ############################################
# Run K-Means on remaining points
remaining_points = rdd.filter(lambda x: x[0] not in sampled_indexes)
remaining_index = remaining_points.map(lambda x: x[0]).collect()

# Define K cluster for K-means
k = 2 * n_cluster

# Generate Initial Clusters
cluster_init = random.sample(remaining_index, k=k)
centroids = rdd.filter(lambda x: x[0] in cluster_init).collect()
    
# Run K-Means Algorithm
cluster_assignment, iteration = k_means(remaining_points, centroids, k, remaining_index,
                                        remaining_points.collectAsMap())

temp = cluster_assignment.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b)
# Summarized CS set
temp_summary = temp.filter(lambda x: len(x[1]) > 1).map(lambda x: summarize_cluster(x)).sortBy(lambda x: x[0])
cs_summary = temp_summary.collect()
cs_cent_std = temp_summary.map(lambda x: generate_centriod_std(x)).sortBy(lambda x: x[0]).collect()

# Remaining Data to RS set
rs_points = temp.filter(lambda x: len(x[1]) <= 1).flatMap(lambda x: x[1])

#################################### Loop for All Remaining Files #################################################
for i in range(1, len(input_files_arr)):
    #################################### End Round & Record Summary ####################################################
    # Store Intermediate Results
    # DS
    nof_cluster_discard.append(len(ds_summary))
    num_points = 0
    for j in ds_summary:
        num_points += j[1][0]
    nof_point_discard.append(num_points)
    # CS
    nof_cluster_compression.append((len(cs_summary)))
    num_points = 0
    for j in cs_summary:
        num_points += j[1][0]
    nof_point_compression.append((num_points))
    # RS
    nof_point_retained.append(rs_points.count())

    # Output Runtime
    t1_end = time.time()
    print('Round Duration',i,': ', t1_end - t1)

    ############################################ Load Next File #######################################################
    # Generate Input Path
    input_path = os.path.join(input_dir, input_files_arr[i])

    # Load File
    textRDD = sc.textFile(input_path)
    rdd = textRDD.map(lambda x: tuple(x.split(','))).map(
        lambda x: tuple([int(x[0]), tuple([float(i) for i in x[1:]])])).persist()
    # (point index, (features.....))

    ########################################## Assign to DS & Update ###################################################
    # Assign to DS Clusters if point close enough & Update DS Summary
    ds_test = rdd.map(lambda x: check_cluster(x, ds_cent_std))
    assign_ds = ds_test.filter(lambda x: x[0] >= 0)

    if assign_ds.count() > 0:
        # First combine assigned points into summary
        temp_sum = assign_ds.reduceByKey(lambda a, b: a + b).map(lambda x: summarize_cluster(x)) \
            .sortBy(lambda x: x[0]).collect()

        # Update summary with new summary info
        ds_summary = update_summary(temp_sum, ds_summary)

    ########################################## Assign to CS & Update ###################################################
    # Assign Remaining points to CS Clusters if point close enough & Update CS Summary
    # Remaining Points
    cs_test = ds_test.filter(lambda x: x[0] == -1).map(lambda x: check_cluster(x[1][0], cs_cent_std))
    assign_cs = cs_test.filter(lambda x: x[0] >= 0)

    if assign_cs.count() > 0:
        # First combine assigned points into summary
        temp_sum = assign_cs.reduceByKey(lambda a, b: a + b).map(lambda x: summarize_cluster(x)) \
            .sortBy(lambda x: x[0]).collect()

        # Update summary with new summary info
        cs_summary = update_summary(temp_sum, cs_summary)

    ########################################## Assign to RS & K-Means ##################################################
    # Remaining Points
    remain = cs_test.filter(lambda x: x[0] == -1).map(lambda x: x[1][0])
    rs_points = rs_points.union(remain)

    length_rs = rs_points.count()
    if length_rs > 2 * 2 * n_cluster:
        # Define K cluster for K-means
        k = 2 * n_cluster
        indexes = rs_points.map(lambda x: x[0]).collect()
       
        # Generate Initial Clusters
        cluster_init = random.sample(indexes, k=k)
        centroids = rdd.filter(lambda x: x[0] in cluster_init).collect()
        
        print('KMeans for CS+RS')
        # Run K-Means Algorithm
        cluster_assignment, iteration = k_means(rs_points, centroids, k, indexes,
                                                rs_points.collectAsMap())
        print('k-means num of iteration: ', iteration)

        temp = cluster_assignment.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b)
        # Summarized CS set
        temp_summary = temp.filter(lambda x: len(x[1]) > 1).map(lambda x: summarize_cluster(x)).sortBy(lambda x: x[0])
        new_cs_summary = temp_summary.collect()
        new_cs_cent_std = temp_summary.map(lambda x: generate_centriod_std(x)).sortBy(lambda x: x[0]).collect()

        # Remaining Data to RS set
        rs_points = temp.filter(lambda x: len(x[1]) <= 1).flatMap(lambda x: x[1])

        ######################################### Merge Close CS Clusters ##############################################
        res = check_if_merge(new_cs_cent_std, cs_cent_std)

        cs_summary = combine_cluster(res, new_cs_summary, cs_summary)
        temp = sc.parallelize(cs_summary)

        cs_cent_std = temp.map(lambda x: generate_centriod_std(x)).sortBy(lambda x: x[0]).collect()

    # END LOOP
# Last Round

#################################### End Round & Record Summary ####################################################
# Store Intermediate Results
# DS
nof_cluster_discard.append(len(ds_summary))
num_points = 0
for j in ds_summary:
    num_points += j[1][0]
nof_point_discard.append(num_points)
# CS
nof_cluster_compression.append((len(cs_summary)))
num_points = 0
for j in cs_summary:
    num_points += j[1][0]
nof_point_compression.append((num_points))
# RS
nof_point_retained.append(rs_points.count())

################################## Output Intermediate Results  #######################################################
with open(intermediate_res, 'w') as file:
    file.write('round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained'+'\n')
    for i in range(len(nof_cluster_discard)):
        file.write(str(i+1) + ',' + str(nof_cluster_discard[i]) + ',' + str(nof_point_discard[i]) + ',' +
                   str(nof_cluster_compression[i]) + ',' + str(nof_point_compression[i])
                   + ',' + str(nof_point_retained[i]) + '\n')

################################## Merge Close CS Clusters to DS Else Outlier ##########################################
res = merge_final(cs_cent_std, ds_cent_std)

ds_summary = combine_cluster(res, cs_summary, ds_summary)

# Change into dict for output
temp = sc.parallelize(ds_summary).flatMapValues(lambda x: x[3])

ans_dict = temp.map(lambda x: (x[1],x[0]))
# Rs points labeled as outlier
rs_dict = rs_points.map(lambda x: (x[0],-1))
# Combine both dict
temp = ans_dict.union(rs_dict)
ans = temp.collectAsMap()

################################## Output Clustering Results  #######################################################
with open(cluster_res, 'w') as file:
    file.write(json.dumps(ans))

# Output Runtime
t1_end = time.time()
print('Duration: ', t1_end - t1)

sc.stop()
# End Program
