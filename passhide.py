# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *
import configparser
from configparser import ConfigParser
conf=ConfigParser()

conf.read(r'F:\\Asma\\Spark_by_venu_Sir\\datasets\\config.txt')
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","passd")
data=conf.get("input","data")
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
# data='F:\\bigdata\\drivers\\10000Records.csv'
df=spark.read.format('csv').option('header','true').option('sep',',').option('inferschema','true').load(data)
import re
cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
ndf = df.toDF(*cols)
ndf.show()

ndf.write.mode("overwrite").format('jdbc').option('url',host)\
    .option('dbtable','testrecords1')\
    .option('user',user)\
    .option('password',pwd)\
    .option('driver','com.mysql.jdbc.Driver').save()
