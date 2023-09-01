from pyspark.sql.functions import col, min, round, desc, udf, explode, split
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("popuparHeros").getOrCreate()


connections = spark.read.text("Marvel+Graph")
heros  = connections.select(explode(split(connections.value, " ")).alias("heroID"))
herosCount = heros.groupBy("heroID").count().orderBy(desc("count"))
# herosCount.show()


# Load the lookup table
schema = StructType([StructField("Id", StringType(), True), 
                     StructField("Name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel+Names")

# Use broadcast variable 
lookupTableNames = spark.sparkContext.broadcast(names.collect())

def lookupName(id):
    matching_rows = [row for row in lookupTableNames.value if row.Id == id]
    if matching_rows:
        name = matching_rows[0].Name
        return name
    else:
        return None

lookupNameUDF = udf(lookupName)

herosCountWithName = herosCount.withColumn("Name", lookupNameUDF("heroID"))

herosCountWithName.show()

# Use join function
print("====Use Join Function====")
HerosHaveOneConn = herosCount.filter(col("count")==1)
# HerosHaveOneConn.show()
HerosHaveOneConn.join(names, HerosHaveOneConn.heroID==names.Id, "left").select("*").show()

# Use min 
print("=====Use agg min=====")
HerosHaveMin = herosCount.filter(col("count") == herosCount.agg(min(col("count"))).first()[0])
# HerosHaveMin.show()
HerosHaveMin.join(names, HerosHaveMin.heroId==names.Id, "left").select("*").show()

spark.stop()