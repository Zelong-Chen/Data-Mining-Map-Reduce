from pyspark import SparkContext
import sys
import json
from collections import defaultdict
import os

review_file = sys.argv[1]
business_file = sys.argv[2]
output_file = sys.argv[3]
spark_var = str(sys.argv[4])
n = int(sys.argv[5])

#review_file = 'review.json'
#business_file = 'business.json'
#output_file = 'output2.txt'
#spark_var = 'no_spark'
#n = int(20)

# If using Spark
if spark_var == 'spark':

    sc = SparkContext('local[*]', 'task1')
    sc.setLogLevel('OFF')

    # Review.json
    textRDD = sc.textFile(review_file)
    review_rdd = textRDD.map(json.loads).map(lambda row: (row['business_id'], row['stars'])).persist()

    # Businesss.json
    textRDD2 = sc.textFile(business_file)
    business_rdd = textRDD2.map(json.loads).map(lambda row: (row['business_id'], row['categories'])).persist()

    # Join Review and Business and remove None values
    rdd = review_rdd.leftOuterJoin(business_rdd).filter(lambda x: x[1][1] is not None)

    # Split categories by ',' ; Strip leading and ending spaces; flatMap into (category, (star, count))
    cleaned = rdd.map(lambda x: (x[1][1].split(','), x[1][0])).flatMap(lambda x: [(v.strip(), (x[1], 1)) for v in x[0]])

    # Reduce by Key ; add star counts together ; add # of counts together
    task1 = cleaned.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))

    # Calculate avg by stars/total counts then sort by avg stars and alphabetical order
    task2 = task1.map(lambda x: (str(x[0]), x[1][0]/x[1][1])).sortByKey().sortBy(lambda x: x[1], ascending=False).take(n)
    
    # Convert from tuple to list of list
    pairs_list = []
    for i in task2:
        pairs_list.append([i[0],i[1]])
        
    # Output Dictionary
    ans_dict = {"result": pairs_list}
    
    print('spark:')
    print(ans_dict)
    # Output File
    with open(output_file, 'w') as file:
        file.write(json.dumps(ans_dict))
    
    sc.stop()
    
elif spark_var == 'no_spark':
    
    # Read in review and business
    review = [json.loads(line) for line in open(review_file, 'r')]
    business = [json.loads(line) for line in open(business_file, 'r')]

    # Generate list of categories and initalize counters for stars and count
    vocab = []
    for i in business:
        test = i['categories']
        if test == None:
            pass
        else:
            if ',' in test:
                categories = test.split(',')
            else:
                categories = [test]
            for j in categories:
                vocab.append(j.strip())
    vocab = list(set(vocab))
    star_arr = [0] * len(vocab)
    count_arr = [0] * len(vocab)
    
    # store values based on business_id as key 
    temp = defaultdict(list)
    for elem in business:
        temp[elem['business_id']].extend([elem['categories']])
    for elem in review:
        temp[elem['business_id']].extend([elem['stars']])

    # Function to update stars and count counters per business per category
    def avg_rating(arr):
        if len(arr) <= 1:
            return 0
        else:
            category = arr[0]
            sum_stars = sum(arr[1:])
            length = len(arr[1:])

            if category == None:
                return 0
            else:
                if ',' in category:
                    categories = category.split(',')
                else:
                    categories = [category]
                for i in categories:
                    # Strip leading and ending spaces
                    category = i.strip()
                    # Look for index of location of category
                    index = vocab.index(category)
                    # Add star counts and increase counter
                    star_arr[index] += sum_stars
                    count_arr[index] += length
                return 1

    # Call predefine function per dict item
    Output = [avg_rating(y) for x, y in temp.items()]

    # Calculate Average rating per category
    avg_arr = [(i[0] / i[1]) if i[1] != 0 else 0 for i in zip(star_arr, count_arr)]
    avg_arr

    # Find all indices with max average rating
    indices = [i for i, x in enumerate(avg_arr) if x == max(avg_arr)]

    # If indicies greater than # retrieved then pass
    if len(indices) >= n:
        pass
    # Else sort list of values and get top # retrieved indicies
    else:
        indicies = sorted(range(len(avg_arr)), key=lambda i: avg_arr[i])[-n:]

    # Append into one list of the category and its score
    ans = []
    for i in indices:
        ans.append([str(vocab[i]), avg_arr[i]])

    # Sort ans alphabetically
    ans.sort(key=lambda x: (x[1], x[0]))
    
    # Format into submission dict
    arr = []
    for i in range(0, n, 1):
        arr.append(ans[i])
    ans_dict = {"result": arr}

    print(ans_dict)
    # Output File
    with open(output_file, 'w') as file:
        file.write(json.dumps(ans_dict))
        