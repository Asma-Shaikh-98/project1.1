# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data = "F:\\bigdata\\datasets\\complexxmldata.xml"

# In this xml data there are many tags avaialable as row tag but the second tag we have to consider as rowtag
df = spark.read.format("xml").option("rowtag","book").option("path",data).load()
df.show()
df.printSchema()