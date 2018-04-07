from pyspark import SparkContext
sc = SparkContext()

rdd = sc.parallelize([("spark",2),("hadoop",6),("hadoop",4),("spark",6)])
rdd2 = rdd.mapValues(lambda x:(x,1))
#rdd2.foreach(print)
rdd3 = rdd2.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))

rdd4 = rdd3.mapValues(lambda x:(x[0]/x[1]))
rdd4.foreach(print)
rdd4.collect()