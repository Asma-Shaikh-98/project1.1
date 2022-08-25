# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
# Program to remove special characters from schema and convert to lowercase
data='F:\\bigdata\\drivers\\10000Records.csv'
df=spark.read.format('csv').option('header','true').option('sep',',').option('inferschema','true').load(data)
import re
cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
ndf = df.toDF(*cols)
# to Know how many males and females data you have
res=ndf.groupBy(col('gender')).agg(count(col('*')).alias('cnt'))
res.show(5,truncate=False)
res.printSchema()
