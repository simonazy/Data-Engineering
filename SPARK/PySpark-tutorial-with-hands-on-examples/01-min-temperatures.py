from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinmalTemp")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(",")
    stationId = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])*0.1*(9.0/5.0) + 32.0
    return (stationId, entryType, temperature)

lines = sc.textFile("1800.csv")

parsedLines = lines.map(parseLine)

parsedLines.collect()

minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

stationTemp = minTemps.map(lambda x: (x[0], x[2]))

minTemp = stationTemp.reduceByKey(lambda x,y: min(x,y))

results = minTemp.collect()

for res in results:
    print(res[0] + "\t{:.2f}".format(res[1]))