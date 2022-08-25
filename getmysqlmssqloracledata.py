# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

host="jdbc:mysql://mysqldb.cz8ogtvhogqa.ap-south-1.rds.amazonaws.com:3306/asma?useSSL=false"
uname="mysqldb"
pwd="mypassword"
df=spark.read.format('jdbc')\
    .option('url',host)\
    .option('dbtable',"emp")\
    .option('user',uname)\
    .option('password',pwd)\
    .option('driver','com.mysql.jdbc.Driver').load()
# Data Processing
res=df.na.fill(0,['comm','mgr']).withColumn("comm",col("comm").cast(IntegerType()))\
    .withColumn("hiredate",date_format(col("hiredate"),"yyyy/MMM/dd"))

#To store result temporarily
res.write.format('jdbc').option('url',host)\
    .option('dbtable','empclean')\
    .option('user',uname)\
    .option('password',pwd)\
    .option('driver','com.mysql.jdbc.Driver').save()

# IMP
#Note that please mension table name different otherwise it will give deadlock
#Deadlock means if you read and writing from or into same table at the same time it unable to process because each holds a lock that the other needs.
#Because both transaction are waiting for a resource to become available,neither ever release the lock it holds.
#Because of this we get empty table
res.show()
res.printSchema()