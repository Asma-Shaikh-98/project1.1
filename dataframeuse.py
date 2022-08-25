# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
# data='F:\\bigdata\\drivers\\donations.csv'
data='F:\\bigdata\\drivers\\bank-full.csv'
df=spark.read.format('csv').option('header','True').option('sep',';').option('inferschema','true').load(data)

# rdd=spark.sparkContext.textFile(data)
# skip=rdd.first()
# odata= rdd.filter(lambda x:x!=skip)

# df.printSchema()
#printing columns and its datatype in nice tree format.
# res=df.where(col('age')>90)
# res=df.where((col('age')>90) & (col('marital') !='married'))
# res =df.groupBy(col('marital')).agg(sum('balance')).alias('smb')).orderBy(col('smb').desc())
res = df.groupBy(col('marital')).agg(count('*').alias("cnt"),sum(col('balance')).alias('smb'))
# res=df.where(col('balance')>avg(col('balance')))
# res = df.groupBy(col("marital")).agg(count("*").alias("cnt"),sum(col("balance")).alias("smb"))

# df.createOrReplaceTempView('tab')
# res=spark.sql('select * from tab where age>50 and balance > 50000')

# res=spark.sql('select marital,sum(balance) from tab group by marital')


res.show()
# res.printSchema()