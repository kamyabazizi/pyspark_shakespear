import numpy as np
import time
from pyspark.sql.functions import split, explode, col, expr, lit, regexp_replace, lower, trim, translate
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

# spark = SparkSession.builder.master("local").getOrCreate()
start_time = time.time()
spark = SparkSession \
    .builder \
    .appName("Word count") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
conf = spark.sparkContext._conf.setAll([('spark.executor.cores', '1')])
spark.sparkContext.stop()
spark = SparkSession.builder.config(conf=conf).getOrCreate()

shakes = spark.read.format("text").load("Shakespeare.txt").withColumnRenamed("value", "line")
# shakes.show()

shakes = shakes.select(explode(split(col("line"), " ")).alias("word_per_line"))

shakes = shakes.filter(col("word_per_line") != "")

# punc='!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'

shakes = shakes.select(lower(trim(regexp_replace(col("word_per_line"),"\\p{Punct}",''))).alias("punc_rmv"))

# how many distict words are in this text
print("how many distict words are in this text:")
print(shakes.distinct().count()) #28481


shakes_grp = shakes.groupBy(col("punc_rmv")).count().orderBy(col('count').desc())

#Top 10 most repetitive words
print("Top 10 most repetitive words:")
shakes_grp.show(10)


# how many words without repetition
result = 0
temp_list = shakes_grp.select('count').collect()
ar = np.array(temp_list)
for i in range(1,len(ar)):
  if  ar[i] == 1: result = result + 1    
print("Total number of words without repetition is = ", result) #12336

print("--- %s seconds ---" % (time.time() - start_time))
