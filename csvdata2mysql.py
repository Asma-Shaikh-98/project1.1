# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data='F:\\bigdata\\drivers\\10000Records.csv'
df=spark.read.format('csv').option('header','true').option('sep',',').option('inferschema','true').load(data)
import re
cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
ndf = df.toDF(*cols)
ndf.show()
host="jdbc:mysql://mysqldb.cz8ogtvhogqa.ap-south-1.rds.amazonaws.com:3306/asma?useSSL=false"
uname="mysqldb"
pwd="mypassword"
ndf.write.mode("overwrite").format('jdbc').option('url',host)\
    .option('dbtable','records1')\
    .option('user',uname)\
    .option('password',pwd)\
    .option('driver','com.mysql.jdbc.Driver').save()