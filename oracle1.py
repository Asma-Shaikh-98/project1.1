# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

host= "jdbc:mysql://mysqldb.cz8ogtvhogqa.ap-south-1.rds.amazonaws.com:3306/asma?useSSL=false"

uname="mysqldb"
pwd="mypassword"

df=spark.read.format("jdbc").option("url",host)\
    .option("dbtable","emp").option("user",uname).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").load()

res=df.na.fill(0,['comm']).withColumn("comm",col('comm').cast(IntegerType()))\
    .withColumn("hiredate",date_format(col("hiredate"),"yyyy-MM-dd"))
res.show()
res.printSchema()

# spark-submit --master local --deploy-mode client F:\bigdata\spark-3.1.2-bin-hadoop3.2\python\pyspark\pyspark_projects\project1\oracle1.py
