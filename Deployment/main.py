print("hello world")
from unittest import result
import numpy
import random
import findspark
findspark.init()

""" from pyspark import SparkContext

sc = SparkContext(appName="EstimatePi")
def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1
NUM_SAMPLES = 1000000
count = sc.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
sc.stop() """


from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, desc
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .getOrCreate() 
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) #formato al output de tablas

#1. Cargar los CSV en dataframes
consoles_df = spark.read.csv("../data/consoles.csv", header=True, inferSchema=True)
result_df = spark.read.csv("../data/result.csv", header=True, inferSchema=True)
consoles_df.printSchema()
result_df.printSchema()

""" out = result_df.where(result_df.metascore > 97)
print(result_df) """
""" out = result_df.agg({'userscore':'avg'})
print(out) """

#The top 10 best games for each console/company.
window = Window.partitionBy(result_df['console']).orderBy(desc("metascore"),desc("userscore"))
report1 = result_df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 10) 
print(report1)
#The worst 10 games for each console/company.
#The top 10 best games for all consoles.
report3 = result_df \
    .orderBy(["metascore","userscore"], ascending=[False,False]) \
    .limit(10)
print(report3)
#The worst 10 games for all consoles. 
report4 = result_df \
    .orderBy(["metascore","userscore"], ascending=[True,True]) \
    .limit(10)
print(report4)