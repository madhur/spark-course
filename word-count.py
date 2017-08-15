from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Users/madhur/Desktop/SparkCourse/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()
print(type(wordCounts))
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
    	pass
        #print(cleanWord.decode() + " " + str(count))
