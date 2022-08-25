import sys
import os
import findspark
findspark.init()
os.environ["JAVA_HOME"]="/usr/lib/jvm/java"
os.environ["SPARK_HOME"]="/usr/lib/spark"
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").enableHiveSupport().appName("test").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("ERROR")

host="jdbc:mysql://mysqldb.cz8ogtvhogqa.ap-south-1.rds.amazonaws.com:3306/asma?useSSL=false"
uname="mysqldb"
pwd="mypassword"
tab="emp"
df=spark.read.format('jdbc').option('url',host).option('dbtable',tab).option('user',uname).option('password',pwd).option('driver','com.mysql.jdbc.Driver').load()
df.show()
# If you want to store data in hive in sparkSession you must have to mesion enableHiveSupport() to store in hive.Similarly df write with saveAsTable method.
df.write.format("hive").saveAsTable(tab)

#similarly hive tables directly displaying using shell script
spark.sql("show tables").show()
spark.sql("select * from emp").show()