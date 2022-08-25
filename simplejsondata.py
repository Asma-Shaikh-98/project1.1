# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="F:\\bigdata\\datasets\\zips.json"
df = spark.read.format("json").load(data)
# df.show(truncate=False)
# df.printSchema()

# Simple datatypes:int,string,double date etc
# Complex datatype means:Array,struct,Map
# Special charactters are not recommendable so thats why rename columns which is having special chars

# If any field having more than one value with comma seperated like here we have [-72.679921, 42.264319] in square bracket so this is called array datatype i.e.collection of same datatype


# ndf = df.withColumnRenamed("_id","id").withColumn("loc",explode(col("loc")))
# ndf.show()
# ndf.printSchema()

# explode(col) -->if you have one field which has several elements so explode function will create multiple column with respect to no of elements
# explode() simple explode/unnest data means remove array elements.

# now our requirement is to seperate this array into two columns and give them name

ndf = df.withColumnRenamed("_id","id").withColumn("longitude",col("loc")[0]).withColumn("latitude",col("loc")[1]).drop("loc")
ndf.show(5)
ndf.printSchema()