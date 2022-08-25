# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").config("spark.sql.session.timezone","EST").appName("test").getOrCreate()

data='F:\\bigdata\\drivers\\donations.csv'

df=spark.read.format('csv').option('header','true').option('inferschema','true').load(data)
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
    .withColumn('dateadd',date_add(col('dt'),-100))\
    .withColumn('dtsub',date_sub(col('dt'),100))\
    .withColumn('lstdt',last_day(col('dt')))\
    .withColumn('nxtday',next_day(col('dt'),'friday'))\
    .withColumn('dateform1',date_format(col('dt'),"dd-MM-yy"))\
    .withColumn('dateform2',date_format(col('dt'),"dd/MM/yy"))\
    .withColumn('dateform3',date_format(col('dt'),"dd/MMMM/yy/EEEE/z"))\
    .withColumn('dateform4',date_format(col('dt'),"dd/MMMM/yy/EEEE/O"))\
    .withColumn('lstdt1',date_format(last_day(col('dt')),'yyyy-MM-dd-EEE'))\
    .withColumn('monlstfri',next_day(date_add(last_day(col('today')),-7),"Fri"))\
    .withColumn('dayodweek',dayofweek(col('dt')))\
    .withColumn('quartr',quarter(col('dt')))\
    .withColumn('dayofmon',dayofmonth(col('dt')))\
    .withColumn('dayofyr',dayofyear(col('dt')))\
    .withColumn('yr',year(col('dt')))\
    .withColumn('mon',month(col('dt')))\
    .withColumn('hr',hour(col('ts'))) \
    .withColumn('min', minute(col('ts'))) \
    .withColumn('monbet',months_between(current_date(),col('dt')))\
    .withColumn('floor',floor(col('monbet')))\
    .withColumn('ceil',ceil(col('monbet')))\
    .withColumn('round',round(col('monbet')).cast(IntegerType()))\
    .withColumn('dttrunc',date_trunc('year',col('dt')))\
    .withColumn('dttrunc1',date_trunc('year',col('dt')).cast(DateType()))\
    .withColumn('dttrunc4',date_trunc('month',col('dt')))\
    .withColumn('dttrunc2',date_trunc('day',col('dt')))\
    .withColumn('dttrunc3',date_trunc('day',col('dt')).cast(DateType()))\
    .withColumn("weekofyr",weekofyear(col('dt')))\
    .withColumn('dtdiff1',uf(col('dtdiff')))\
    .withColumn('15th day',date_format(date_add(last_day(add_months(col('dt'), -1)),15),'EEEE'))

res.printSchema()
res.show(truncate=False)
