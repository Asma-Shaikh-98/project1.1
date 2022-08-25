# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data = "F:\\bigdata\\datasets\\books.xml"
df = spark.read.format("xml").option("rowtag","book").option("path",data).load()
# df.show()

# Which book is having more than 10 price
# Data processing
# res = df.withColumnRenamed("_id","id").where(col("price")>10)
# res.show()
# OR
res = df.withColumnRenamed("_id","id").filter(df.price > 10)
res.show()
# store this data into csv format
# op="F:\\bigdata\\result\\xml2csv"
# res.write.mode("overwrite").format("csv").option("header","true").save(op)
#This will store in partition like part-00000 and i dont want to store like this
# I want to store like .csv etc
# Directly its not possible to save like this
# to save like this we have to use pandas
# first convert this dataframe into pandas
op="F:\\bigdata\\result\\xml2csv.csv"
res.toPandas().to_csv(op)


# While executing if you get error like -->Pandas >= 0.23.2 must be installed
# then go to settings -->project python... -->python interpretor -->search for pandas if not available -->
#                       click on +--> pandas -->install it and run your program

######################
# Difference between pandas and koalas
# The Koalas project makes data scientists more productive when interacting with big data, by implementing the pandas DataFrame API on top of Apache Spark.
# pandas is the de facto standard (single-node) DataFrame implementation in Python, while Spark is the de facto standard for big data processing.
# koalas run on the top of multinode clusters
# where as pandas run on single node cluster
# https://koalas.readthedocs.io/en/latest/getting_started/10min.html


