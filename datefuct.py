# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").config('spark.sql.session.timezone','EST').appName("test").getOrCreate()
data='F:\\bigdata\\drivers\\donations.csv'
df=spark.read.format('csv').option('header','true').option('inferschema','true').load(data)
res=df.withColumn('dt',to_date(df.dt,'d-M-yyyy')).withColumn('today',current_date()).withColumn('ts',current_timestamp()).withColumn('dtdiff',datediff(col('today'),col('dt')))
# res.show(5)
def func1(days):
    year=days//365
    mn=(days-year*365)//30
    day= (days - mn*30 - year * 365)
    full="{}-years {}-months {}-days".format(year,mn,day)
    return full

uf=udf(func1)

res=df.withColumn('dt',to_date(df.dt,'d-M-yyyy'))\
    .withColumn('today',current_date())\
    .withColumn('ts',current_timestamp())\
    .withColumn('dtdiff',datediff(col('today'),col('dt')))\
    .withColumn('dtadd',date_add(col('dt'),100))\
    .withColumn('dtsub',date_sub(col('dt'),10))\
    .withColumn('lstdt',last_day(col('dt')))\
    .withColumn('nxtday',next_day(col('dt'),'fri'))\
    .withColumn('dateformat1',date_format(col('dt'),'dd-MM-yy'))\
    .withColumn('dtdiff1',uf(col('dtdiff')))
res.show(truncate=False)
res.printSchema()