from pyspark import SparkConf, SparkContext 

conf = SparkConf().setMaster("local").setAppName("CustomerSpend")
sc = SparkContext(conf=conf)

lines = sc.textFile("customer-orders.csv")

def parseLine(line):
    fields = line.split(",")
    customerId = int(fields[0])
    money = round(float(fields[2]),2)
    return (customerId, money)

linesRdd = lines.map(parseLine)

customerSpend = linesRdd.reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0])).sortByKey()

results = customerSpend.collect()

for res in results:
    print(res[1], res[0])