import numpy as np
import time
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, countDistinct, split, explode, desc, regexp_replace, trim, col, lower
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark import SparkContext, SparkConf
# spark = SparkSession.builder.master("local").getOrCreate()
#start_time = time.time()

spark = SparkSession.builder.appName("KmeansPlusPlus").getOrCreate()
sc = spark.sparkContext
a =[]
cores = ['1', '2', '3', '4']
tms = []
for core in cores:

  conf = SparkConf().setAll([('spark.executor.cores', core)])
  spark.sparkContext.stop()
  sc = SparkContext(conf=conf)
  print("\033[34m",sc.getConf().getAll(), "\033[0m")
  spark = SparkSession.builder.config(conf=conf).appName("WordCount").getOrCreate()

  shakes = spark.read.format("text").load("/content/Shakespeare.txt").withColumnRenamed("value", "line")
    # shakes.show()

  shakes = shakes.select(explode(split(col("line"), " ")).alias("word_per_line"))

  shakes = shakes.filter(col("word_per_line") != "")

    # punc='!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'



  start_time = time.time()
  shakes = shakes.select(lower(trim(regexp_replace(col("word_per_line"),"\\p{Punct}",''))).alias("punc_rmv"))

  # how many distict words are in this text
  #print("how many distict words are in this text:")
  shakes.distinct().count() #28481
  shakes_grp = shakes.groupBy(col("punc_rmv")).count().orderBy(col('count').desc())

  #Top 10 most repetitive words
  #print("Top 10 most repetitive words:")
  #shakes_grp.show(10)


  # how many words without repetition
  result = 0
  temp_list = shakes_grp.select('count').collect()
  ar = np.array(temp_list)
  for i in range(1,len(ar)):
    if  ar[i] == 1: result = result + 1  
  tms.append(time.time() - start_time)  
  #print("Total number of words without repetition is = ", result) #12336

fig = plt.figure()
    
plt.bar(range(1, len(cores)+1), tms)
plt.xlabel("cores")
plt.ylabel("time")
plt.title(f"plot for core timing Shakespeare.txt")
plt.show()
