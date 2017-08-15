import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Users/madhur/Desktop/SparkCourse/book.txt")
words = input.flatMap(normalizeWords)
print(type(words))
wordCounts = words.countByValue()
print(type(wordCounts))
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
    	pass
        #print(cleanWord.decode() + " " + str(count))
