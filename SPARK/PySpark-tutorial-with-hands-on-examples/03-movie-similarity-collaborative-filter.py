from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sqrt, sum, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

movieNamesSchema = StructType([StructField("movieID", IntegerType(), True),
                               StructField("movieTitle", StringType(), True)
                               ])

moviesSchema = StructType([StructField("userID", IntegerType(), True),
                           StructField("movieID", IntegerType(), True),
                           StructField("rating", IntegerType(), True),
                           StructField("timeStamp", LongType(), True)
                           ])

spark = SparkSession.builder.appName("MovieSimilarities").master("local").getOrCreate()

movieNames = spark.read.option("sep", "|") \
                       .option("charset", "ISO-8859-1") \
                       .schema(movieNamesSchema) \
                       .csv("/home/simona/ml-100k/u.item")

movies = spark.read.option("sep", "\t") \
                   .schema(moviesSchema) \
                   .csv("/home/simona/ml-100k/u.data")

def getMovieName(movieID):
    result = movieNames.filter(col("movieID")==movieID).select("movieTitle").collect()[0]
    return result[0]

ratings = movies.select("userId","movieId","rating")

# lump every moive rated by the same user together
moviePairs = ratings.alias("rating1") \
        .join(ratings.alias("rating2"), (col("rating1.userID")==col("rating2.userID")) & (col("rating1.movieID") < col("rating2.movieID"))) \
        .select(col("rating1.movieID").alias("movie1"), col("rating2.moviID").alias("movie2"),
                col("ratings1.rating").alias("rating1"),col("ratings2.rating").alias("rating2")) 

def computeCosineSimilarity(data):
    pairScores = data \
      .withColumn("xx", col("rating1") * col("rating1")) \
      .withColumn("yy", col("rating2") * col("rating2")) \
      .withColumn("xy", col("rating1") * col("rating2")) 
    
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( 
        sum(col("xy")).alias("numerator"), \
        sqrt(sum(col("xx")))*sqrt(sum(col("yy"))).alias("denominator"), \
        count(col("xy")).alias("numPairs")
        )
    
    result = calculateSimilarity.withColumn("score", when(col("denominator") != 0, col("numerator")/col("denominator")).otherwise(0)) \
                       .select("movie1","movie2","score","numPairs")
    
    return result 


moviePairSimilarities = computeCosineSimilarity(moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((col("movie1") == movieID) | (col("movie2") == movieID)) & \
          (col("score") > scoreThreshold) & (col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(col("score").desc()).take(10)
    
    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(getMovieName(movieNames, similarMovieID) + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))
        