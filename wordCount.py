from pyspark import SparkContext
sc = SparkContext( 'local', 'test')
logFile = "/Users/lucas/Documents/spark/spark-2.2.1-bin-hadoop2.7/README.md"
textFile = sc.textFile(logFile)
wordCount = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a, b : a + b)
wordCount.foreach(print)