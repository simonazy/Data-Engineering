from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame API").getOrCreate()
sc = spark.sparkContext

path = '/home/simona/ml-100k/somefile.txt'
#read
df = spark.read \
           .option("delimiter", ";") \
           .option("header", True) \
           .option("nullValue", "n/a") \
           .option("header", True) \
           .csv(path)
df.show()

# read & write 
import tempfile
with tempfile.TemporaryDirectory() as d:
    spark.createDataFrame([{"age": 100,"name":"Kwon"}]).write.mode("overwrite").format("json").save(d)

    spark.read.format('json').load(d).show()

# parquet files
peopleDF = spark.read.json("examples/src/main/resources/people.json")
peopleDF.write.parquet("people.parquet")

parquetFile = spark.read.parquet("people.parquet")
parquetFile.createOrReplaceTempView("parquetFile")
teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenagers.show()

from pyspark.sql import Row
squaresDF = spark.createDataFrame(sc.parallelize(range(1,6)) \
                      .map(lambda i: Row(single=i, double=i**3)))
squaresDF.write.parquet("data/test_table/key=1")
