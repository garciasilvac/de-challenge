print("hello world")
from gettext import NullTranslations
from unittest import result
import numpy
import random
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, desc, monotonically_increasing_id, trim, avg, when, to_json


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .getOrCreate() 
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) #formato al output de tablas

#####   1. Cargar los CSV en dataframes ##########
consoles_input = spark.read.csv("../data/consoles.csv", header=True, inferSchema=True)
result_input = spark.read.csv("../data/result.csv", header=True, inferSchema=True)


#### Limpieza de datos ##########
result_input = result_input.withColumn("console", trim(col("console")))
result_input = result_input.withColumn("userscore", when(result_input.userscore == "tbd",0).otherwise(result_input.userscore))

#########  normalizacion de la base ###########
companies_df = consoles_input.select(col("company").alias("company_name")).distinct() \
    .withColumn("id",monotonically_increasing_id())
consoles_df = consoles_input \
    .join(companies_df,companies_df["company_name"]==consoles_input["company"], "left") \
    .select(consoles_input["console"].alias("console_name"),companies_df["id"].alias("company_id")) \
    .withColumn("id",monotonically_increasing_id())
games_df = result_input.select(col("name").alias("game_name")).distinct() \
    .withColumn("id",monotonically_increasing_id())
result_df= result_input \
    .join(games_df, games_df["game_name"]==result_input["name"],"left") \
    .withColumn("id",monotonically_increasing_id()) \
    .select(col("id").alias("game_id"),"console","metascore","userscore","date") \
    .join(consoles_df, consoles_df["console_name"]==result_input["console"],"left") \
    .select("game_id",col("id").alias("console_id"),"metascore","userscore","date") \
    .withColumn("id",monotonically_increasing_id())


####### Procesasmiento de los reportes ############

### se calcula promedio de votaciones por juego/consola, para todas las fechas:
result1a_df = result_df.groupBy("console_id","game_id").agg(avg("metascore").alias("avg_meta"), avg("userscore").alias("avg_user"))

######    The top 10 best games for each console/company.
print("################   TOP 10  GAMES EACH CONSOLE:  ##############")
# a. for each console:
window1a = Window.partitionBy(result1a_df['console_id']).orderBy(desc("avg_meta"),desc("avg_user"))
report1a = result1a_df.select('*', rank().over(window1a).alias('rank')).filter(col('rank') <= 10)
print(report1a)
report1a.write.mode("overwrite").json("report1a.json")
#b. for each company
result1b_df = report1a.join(consoles_df, consoles_df["id"]==report1a["console_id"])
result1b_df= result1b_df.drop(col("rank")).drop(col("id"))
window1b= Window.partitionBy(result1b_df['company_id']).orderBy(desc("avg_meta"),desc("avg_user"))
report1b = result1b_df.select('*', rank().over(window1b).alias('rank')).filter(col('rank') <= 10)
print(report1b)
report1b.write.mode("overwrite").json("report1b.json")

#######    The worst 10 games for each console/company.
print("################   WORST 10  GAMES EACH CONSOLE:  ##############")
#a. for each console:
window2a = Window.partitionBy(result1a_df['console_id']).orderBy("avg_meta","avg_user")
report2a = result1a_df.select('*', rank().over(window2a).alias('rank')).filter(col('rank') <= 10)
print(report2a)
report2a.write.mode("overwrite").json("report2a.json")
#b. for each company
result2b_df = report2a.join(consoles_df, consoles_df["id"]==report1a["console_id"])
result2b_df= result2b_df.drop(col("rank")).drop(col("id"))
window2b= Window.partitionBy(result2b_df['company_id']).orderBy("avg_meta","avg_user")
report2b = result2b_df.select('*', rank().over(window2b).alias('rank')).filter(col('rank') <= 10)
print(report2b)
report2b.write.mode("overwrite").json("report2b.json")


########    The top 10 best games for all consoles.
print("################   TOP 10 GAMES ALL CONSOLES:  ##############")

result3_df = report1b.drop(col("rank")).orderBy(desc("avg_meta"),desc("avg_user"))
report3 = result3_df.limit(10)
print(report3)
report3.write.mode("overwrite").json("report3.json")
########    The worst 10 games for all consoles
print("################   WORST 10 GAMES ALL CONSOLES:  ##############")

result4_df = report2b.drop(col("rank")).orderBy("avg_meta","avg_user")
report4 = result4_df.limit(10)
print(report4)
report4.write.mode("overwrite").json("report4.json")