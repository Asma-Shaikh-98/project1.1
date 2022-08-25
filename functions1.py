# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data='F:\\bigdata\\drivers\\us-500.csv'
df=spark.read.format('csv').option('header','True').option('inferSchema','true').load(data)
# ndf=df.withColumn('fullname',concat_ws("_",df.first_name,df.last_name,df.state))
# ndf=df.withColumn('fullname',concat_ws("_",df.first_name,df.last_name,df.state)).withColumn('phone1',regexp_replace(col('phone1'),"-",""))
# ndf=df.withColumn('fullname',concat_ws("_",df.first_name,df.last_name,df.state)).withColumn('phone1',regexp_replace(df.phone1,"-",""))
# ndf=df.withColumn('fullname',concat_ws("_",df.first_name,df.last_name,df.state)).withColumn('phone1',regexp_replace(col('phone1'),"-","").cast(LongType()))

# ndf=df.withColumn('state',when(col('state')=='NY','New-York')/
#     .otherwise(col('state')))
# ndf.show()

# ndf=df.withColumn('state',when(df.state=='NY','New York').otherwise(df.state))
# ndf=df.withColumn('state',when(df.state=='NY','New York').when(df.state=='CA','California').otherwise(df.state))

# ndf=df.withColumn('address1',when(col('address').contains('#'),'*****').otherwise(col('address')))\
#     .withColumn('address2',regexp_replace(col('address'),'#','-'))
# ndf=df.withColumn('address1',when(df.address.contains('#'),'*****').otherwise(df.address))\
#     .withColumn('address2',regexp_replace(df.address,'#','-'))


# ndf=df.withColumn('address1',when(col('address').contains('#'),'*****').otherwise(col('address')))\
#     .withColumn('address2',regexp_replace(col('address'),'#','-'))
# ndf1=df.withColumn('substr',substring(col('email'),0,5)).withColumn("email1",substring_index(col('email'),'@',-1)).withColumn("username",substring_index(col('email'),'@',1))
# ndf=ndf1.groupBy(ndf1.email1).count().orderBy(col('count').desc())


df.createOrReplaceTempView('tab')
ndf=spark.sql("select *,concat_ws('_',first_name,last_name)")
ndf.show(5)
# ndf.printSchema()