from pyspark.sql import SparkSession
from pyspark.sql.functions import split, lower, explode, col, desc,count


spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

book = spark.read.text("book.txt")
# explode - similar to flatMap: take a field and explode it to multiple rows
words = book.select(explode(split(book.value, "\\W")).alias("word"))
# filter by multiple conditions: & ()
words = words.filter((words.word !="") & (words.word !=" "))

lowercaseWords = words.select(lower(col("word")).alias("word"))

wordCounts = lowercaseWords.groupBy("word").agg(count("word").alias("count_of_word"))

wordCountsSorted = wordCounts.sort(desc(col("count_of_word")))

wordCountsSorted.show()