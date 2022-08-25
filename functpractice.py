# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()


# ndf = df.withColumn('state',when(col('state')=='NY','Newyork').when(df.state=='CA','California').otherwise(col('state')))
# ndf.printSchema()
# ndf=df.withColumn('address',when(col('address').contains('#'),'*****').otherwise(df.address))
# ndf=df.withColumn('address1',when(df.address.contains('#'),'*****').otherwise(df.address))
# ndf=df.withColumn('address1',when(df.address.contains('#'),'*****').otherwise(df.address))\
#     .withColumn('address2',regexp_replace(df.address,'#','-'))
# ndf = df.withColumn('state',when(df.state=='NY','Newyork').when(df.state=='CA','California').otherwise(df.state))
# ndf = df.withColumn('emails2',substring_index(df.email,'@',-1))
# ndf1 = ndf.groupBy(ndf.emails2).count().orderBy(col('count').desc())
# ndf1.show(5,truncate=False)
# data='F:\\bigdata\\drivers\\us-500.csv'
# df=spark.read.format('csv').option('header','true').option('inferstructure','true').load(data)
# df.createOrReplaceTempView('tab')
# qry="""with tmp as (select *, concat_ws('_', first_name, last_name) fullname, substring_index(email,'@',-1) mail from tab)
# select mail, count(*) cnt from tmp group by mail order by cnt desc
# """
# ndf=spark.sql(qry)
# ndf.show(5,truncate=False)

# ndf=spark.sql("select email,concat_ws(' ',first_name,last_name)fullname,substring_index(email,'@',-1) as username from tab ")
# ndf= df.withColumn('@pos',instr(df.email,'@'))

# ndf2=ndf.groupBy(ndf.emails2).count()
# ndf2.show(truncate=False)
#to convert newly added columns data type to its appropriate datatype we have to import pyspark.sql.types package
# from pyspark.sql.types import *
# drop (columns) ... delete unnecessary columns.
# ndf= df.withColumn('phone3',regexp_replace(df.phone1,'-','').cast(LongType())).drop('email','city','country').withColumnRenamed('first_name','fname').withColumnRenamed('last_name','lname')

#ndf=df.groupBy(df.state).agg(count('*').alias('cnt')).orderBy(col('cnt').desc())
# ndf=df.withColumn('fullname',concat_ws("-",df.first_name,df.last_name,df.state))
#cast will convert into data type here we are converting into Long datatype


data='F:\\bigdata\\drivers\\us-500.csv'
df=spark.read.format('csv').option('header','true').option('inferstructure','true').load(data)
df.createOrReplaceTempView('tab')
def func(st):
    if (st=='NY'):
        return '30% off'
    elif(st=='CA'):
        return  "40% off"
    elif (st == 'OH'):
        return "50% off"
    else:
        return "500/_ off"
uf=udf(func)

spark.udf.register('offer',uf)
ndf=spark.sql('select *,offer(state) todays_offer from tab')
# ndf=df.withColumn('offer',uf(col('state')))
ndf.show(5,truncate=False)