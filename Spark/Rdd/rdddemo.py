from pyspark import SparkContext

sc = SparkContext(appName="rddOps", master='local')
myCollection  = "Spark unified Engine: Simple Processing sample data".split(" ")
words_rdd = sc.parallelize(myCollection, 2)

#print(words_rdd.collect())




rdd1 = sc.parallelize(range(0,25), 6)
print("parallelize : "+str(rdd1.getNumPartitions()))
rdd1.saveAsTextFile("C://Work//AWS_DE//pyspark//PySparkPratice-main//PySparkPratice-main//res")