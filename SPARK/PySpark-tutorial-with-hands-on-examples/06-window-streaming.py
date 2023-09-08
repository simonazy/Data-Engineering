from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, regexp_extract, window, current_timestamp

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
accessLines = spark.readStream.text("logs")

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logsDF2 = logsDF.withColumn("eventTime", current_timestamp())

endpointCounts = logsDF2.groupBy(window(col("eventTime"), "30 seconds", "10 seconds"), col("endpoint")).count()
sortedEndpointCounts = endpointCounts.orderBy(func.col("count").desc())
# Display the stream to the console
query = sortedEndpointCounts.writeStream.outputMode("complete").format("console") \
      .queryName("counts").start()

# Wait until we terminate the scripts
query.awaitTermination()

# Stop the session
spark.stop()