from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Assignment1")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    custid  = int(fields[0])
    itemid = float(fields[1])
    amount = float(fields[2])
    return (custid, amount)

lines = sc.textFile("file:///Users/madhur/Desktop/SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
print rdd
totalsByCustomer = rdd.reduceByKey(lambda x, y: x+y )
results = totalsByCustomer.collect()
# averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# results = averagesByAge.collect()
for result in results:
    print(result)
