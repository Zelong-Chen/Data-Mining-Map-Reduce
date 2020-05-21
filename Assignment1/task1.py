from pyspark import SparkContext
import sys
import json
import os

input_file = sys.argv[1]
output_file = sys.argv[2]
stopwords_file = sys.argv[3]
y = str(sys.argv[4])
m = int(sys.argv[5])
n = int(sys.argv[6])

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('OFF')

input_file_path = input_file
textRDD = sc.textFile(input_file_path)
rdd = textRDD.map(json.loads).map(lambda row: (row['review_id'], row['date'][0:4], row['user_id'],
                                               row['text'])).persist()

# (A) Total number of reviews
counts = rdd.count()

# (B) Number of reviews in a given year, y
years = rdd.map(lambda row: (row[1],1)).filter(lambda x:x[0]==y).reduceByKey(lambda a, b: a+b).collect()


# (C) Number of distinct users who have written the reviews
users = rdd.map(lambda row: (row[2],1)).reduceByKey(lambda a,b: 1).count()

# (D) Top m users who have the largest number of reviews and its count
top_users = rdd.map(lambda row: (row[2],1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: (x[1],x[0]), ascending=False).take(m)
# Generate list of pairs [user,count]
top_users_list = []
for i in top_users:
    top_users_list.append([i[0],i[1]])

# (E) Top n frequent words in the review text in lower case with punctuation and stopwords excluded
# Function for lower case and remove punctuation
def lower_clean_str(x):
    punc = '([,.!?:;])'
    lowercased_str = x.lower()
    for ch in punc:
        lowercased_str = lowercased_str.replace(ch, '')
    return lowercased_str

# Read in stopwords
stop_word_path = stopwords_file
lines = sc.textFile(stop_word_path)
stop_words = lines.collect()

words = rdd.flatMap(lambda row: lower_clean_str(row[3]).split(' ')).filter(lambda x: x not in stop_words)\
    .map(lambda w: (w,1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: (x[1],x[0]), ascending=False).take(n)

words_list = []
for i in words:
    words_list.append(i[0])

# Output Dictionary
ans_dict = {"A": counts,
        "B": years[0][1],
        "C": users,
        "D": top_users_list,
        "E": words_list}
print(ans_dict)
# Output File
with open(output_file, 'w') as file:
    file.write(json.dumps(ans_dict))

sc.stop()