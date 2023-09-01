# Broadcast objects to the executors, such that they'are always there 
# Just use sc.broadcast() to ship off whateve you want
# Then use .value() to get the object back 
# Use the broadcasted object however you want - map functions, UDF's, whatever
from pyspark.sql.types import StructType, StructField, LongType, IntegerType
from pyspark.sql.functions import col, min, round, desc, udf
from pyspark.sql import SparkSession 
import codecs 

def loadMovieNames():
    movieNames = {}
    with codecs.open("/home/simona/ml-100k/u.item","r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
nameDict = spark.sparkContext.broadcast(loadMovieNames()) #Available all across the cluster now

schema = StructType([StructField("userID", IntegerType(), True),
                     StructField("movieID", IntegerType(), True),
                     StructField("rating", IntegerType(), True),
                     StructField("timestamp", LongType(), True)])

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("/home/simona/ml-100k/u.data")
moviesCounts = moviesDF.groupBy("movieID").count().orderBy(desc("count"))

def lookupMovieName(movieId):
    return nameDict.value[movieId] 

lookupMovieNameUDF = udf(lookupMovieName)

movieWithNames = moviesCounts.withColumn("movieTitle", lookupMovieNameUDF(col("movieID")))

movieWithNames.show(10)

spark.stop()