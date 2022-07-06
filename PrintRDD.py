from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("Print RDD")
sc = SparkContext(conf=conf)
data = sc.textFile("in/data.txt")
data.foreach(lambda f : print(f))