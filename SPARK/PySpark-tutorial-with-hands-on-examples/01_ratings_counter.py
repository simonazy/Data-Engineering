from pyspark import SparkConf, SparkContext
import collections
"""
SPARKCONTEXT
- is responsible for making RDD's resilient and distributed
- created by your driver program
- creates RDD's (can be from JDBC, Cassandra, Hive, JSON, HBase, CSV)
- the spark shell created a "sc" object for you
"""
conf = SparkConf().setAppName("RatingHistogram").setMaster("local")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-100k/u.data")
"""RDD Methods: rdd.map(lambda x: x*x)
accept a function as a parameter: functional programming.
"""
rating = lines.map(lambda x: x.split()[2])
"""RDD Actions: collect, count, CountByValue, take, top, reduce"""
result = rating.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))

for key, value in sortedResults.items():
    print("%s %i" %(key, value))