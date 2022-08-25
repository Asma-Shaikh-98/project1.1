# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
# working with json data and write data into oracle or mysql
data="F:\\bigdata\\datasets\\zips.json"
df = spark.read.format("json").load(data)
ndf1 = df.withColumnRenamed("_id","id").withColumn("longitude",col("loc")[0]).withColumn("latitude",col("loc")[1]).drop("loc")
# Now to process this data
ndf1.createOrReplaceTempView("tab")

ndf=spark.sql("select * from tab where state = 'CA'")
host="jdbc:mysql://mysqldb.cz8ogtvhogqa.ap-south-1.rds.amazonaws.com:3306/asma?useSSL=false"
uname="mysqldb"
pwd="mypassword"
ndf.write.mode("overwrite").format('jdbc').option('url',host)\
    .option('dbtable','jsontomysql')\
    .option('user',uname)\
    .option('password',pwd)\
    .option('driver','com.mysql.jdbc.Driver').save()
