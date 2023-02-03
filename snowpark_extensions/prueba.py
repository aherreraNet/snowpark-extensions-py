import findspark
import re
findspark.find()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()

from pyspark.sql.functions import split
df = spark.createDataFrame([('From: mauricio@mobilize.net',)], ['s',])

res = df.select(split(df.s, '((From|To)|Subject): (\w+@\w+\.[a-z]+)', 1).alias('s')).show()


