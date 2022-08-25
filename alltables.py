#import all tables from mysql
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
import configparser
from configparser import ConfigParser
conf=ConfigParser()

conf.read(r'F:\\Asma\\Spark_by_venu_Sir\\datasets\\config.txt')
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","passd")
data=conf.get("input","data")
qry="(select table_name from information_schema.TABLEs where table_schema='asma')t"
df1=spark.read.format('jdbc').option('url',host).option('dbtable',qry).option('user',user)\
    .option('password',pwd)\
    .option('driver','com.mysql.jdbc.Driver').load()

tabs=[x[0] for x in df1.collect() if df1.count()>0]
# all the tables is converting to list by using list comprehension

for i in tabs:
    df=spark.read.format('jdbc').option('url',host)\
        .option('dbtable',i).option('user',user)\
        .option('password',pwd)\
        .option('driver','com.mysql.jdbc.Driver').load()
    df.show()