# # For building anything related to spark these three lines are mandatory(for any pyspark code)
#
# from pyspark.sql import *
# from pyspark.sql.functions import *
#
# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#
# # Data cleasing for different date formats in the given data
#
# data="F:/bigdata/drivers/dateusecae.csv"
# df=spark.read.format("csv").option("header","true").option("inferschema","true").load(data)
#
# def dynamic_func(cols,frmts=("MM-dd-yyyy","yyyy-MM-dd","dd-MMM-yyyy","ddMMMMyyyy","MMM/yyyy/dd","yyyy-MMM-dd")):
#     return coalesce(*[to_date(cols,i) for i in frmts])
#
# #data Cleansing
# import re
# cols=[re.sub('[^a-zA-Z-0-9]','',c) for c in df.columns]
#
# ndf=df.toDF(*cols)
# res=ndf.withColumn("mydob1",dynamic_func(col("mydob")))
# res.show()
# res.printSchema()

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data = "F:\\bigdata\\datasets\\world_bank.json"
df = spark.read.format("json").load(data)
df.show()
df.printSchema()