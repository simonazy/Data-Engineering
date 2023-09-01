from pyspark.sql import SparkSession
from pyspark.sql.functions import round, avg

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header","true")\
                   .option("inferschema","true")\
                   .csv("fakefriends-header.csv")

print("Here is your inferred schema")
people.printSchema()

print("Let's display the name column: ")
people.select("name").show()

print("Filter out anyone ove 21: ")
people.filter(people.age<21).show()

print("Group by age")
people.groupby("age").count().show()

print("Make everyone 10 years older")
people.select("name", people.age+10).show()

#Part 2#
friendsByAge = people.select("age","friends")

friendsByAge.groupBy("age").avg("friends").show()

friendsByAge.groupby("age").agg(round(avg("friends"),2).alias("friends_avg")).sort("age").show()

spark.stop()