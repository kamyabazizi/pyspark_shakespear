import numpy as np
from pyspark.sql.functions import split, explode, col, expr, lit, regexp_replace, lower, trim, translate
from pyspark.sql import SparkSession

# spark = SparkSession.builder.master("local").getOrCreate()

spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()

shakes = spark.read.format("text").load("/content/Shakespeare.txt").withColumnRenamed("value", "line")
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
