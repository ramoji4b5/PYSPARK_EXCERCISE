from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("WordCount").setMaster("local[*]")
sc = SparkContext(conf=conf)

data = [1,2,3,4,5]
distdata = sc.parallelize(data)

res1 = distdata.map(lambda x : x+2)
print(res1.collect())
