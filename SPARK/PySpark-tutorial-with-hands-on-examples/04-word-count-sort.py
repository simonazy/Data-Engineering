# map() vs flatMap()
# flatMap() can create many new elements from each one
import re
from pyspark import SparkConf, SparkContext 

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

def normalizaWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

lines = sc.textFile("Book")
# words = lines.flatMap(lambda x: x.split())
words = lines.flatMap(normalizaWords)

"under hood of countByValue() "
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
# wordCounts = words.countByValue()

results = wordCountsSorted.collect()

# for word, count in results.items():
#     cleanWord = word.encode("ascii", 'ignore')
#     if (cleanWord):
#         print(word, count)

for res in results:
    count = str(res[0])
    word = res[1].encode('ascii', 'ignore')
    if (word):
        print (word, ":\t\t", count)