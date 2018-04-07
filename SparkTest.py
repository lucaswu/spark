from pyspark import SparkContext

sc = SparkContext()
logFile = "/Users/lucas/Documents/spark/spark-2.2.1-bin-hadoop2.7/README.md"
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda  line: 'spark' in line).count()
print('Line with spark :%s'%numAs)
print(logData.first())
