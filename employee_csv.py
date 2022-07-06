from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext


def usingSparkSession():

    spark = SparkSession.builder.appName("Employee data").getOrCreate()
    #df1 = spark.read.csv("C:/Users/ramoj/OneDrive/Documents/excercise_1.csv")
    df1 = spark.read.option("header","true").option("inferSchema","true").csv("C:/Users/ramoj/OneDrive/Documents/excercise_1.csv")
    df2 = spark.read.option("header","true").option("inferSchema","true").csv("C:/Users/ramoj/OneDrive/Documents/excercise_2.csv")
    df1.printSchema()
    df2.printSchema()
    df1.createOrReplaceTempView("emp")
    df2.createOrReplaceTempView("dep")
    spark.sql(
        "select dep.Deapartment, AVG(emp.salary) from emp join dep on emp.employeeID==dep.employeeID group by Deapartment order by AVG(emp.salary) DESC").show(
        5)

    spark.sql(
        "select AVG(emp.salary),dep.Deapartment from emp join dep on emp.employeeID==dep.employeeID where emp.age >=40 group by Deapartment ").show()

    spark.sql(
        "select count(dep.level) from emp join dep on emp.employeeID==dep.employeeID where emp.salary >=4000 and dep.level='VP' and dep.Deapartment ='HR'").show()

    spark.stop()

def usingSparkContext():
    print("using Spark context")
    conf = SparkConf().setAppName("primeNumbers").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sqlcontext = SQLContext(sc)
    #rawData = sc.textFile("C:/Users/ramoj/OneDrive/Documents/excercise_1.csv")
    #data = sc.parallelize(rawData)
    #df1 = data.toDF()
    #df1.printSchema()
    df1 =sqlcontext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('C:/Users/ramoj/OneDrive/Documents/excercise_1.csv') # this is your csv file
    df1.printSchema()
    df2 = sqlcontext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(
        'C:/Users/ramoj/OneDrive/Documents/excercise_2.csv')  # this is your csv file
    df2.printSchema()

    df1.createOrReplaceTempView("emp")
    df2.createOrReplaceTempView("dep")
    sqlcontext.sql("select dep.Deapartment, AVG(emp.salary) from emp join dep on emp.employeeID==dep.employeeID group by Deapartment order by AVG(emp.salary) DESC").show(5)


if __name__ == '__main__':
    usingSparkContext()

