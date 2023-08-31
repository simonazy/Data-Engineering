from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, min, round
from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([StructField("stationID", StringType(), True),
                     StructField("date", IntegerType(), True),
                     StructField("measure_type", StringType(), True),
                     StructField("temperature", FloatType(), True)]) 

df = spark.read.schema(schema).csv("1800.csv")
df.printSchema()

minTemps = df.filter(df.measure_type == 'TMIN')

stationTemps = minTemps.select("stationID", "temperature")

minTempsByStation = stationTemps.groupBy("stationID").agg(min("temperature").alias("min_temperature"))
minTempsByStation.show()

minTempsByStationF=minTempsByStation.withColumn("temperature", round(col("min_temperature")*0.1*(9.0/5.0)+32,2)).select("stationId","temperature").sort("temperature")

results = minTempsByStationF.collect()

for res in results:
    print(res[0] +"\t{:.2f}F".format(res[1]))