# For building anything related to spark these three lines are mandatory(for any pyspark code)
from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
# data = 'F:\\bigdata\\drivers\\donations1.csv'
# rdd=spark.sparkContext.textFile(data)
# skip=rdd.first()
# odata=rdd.filter(lambda x:x!=skip)
# df=spark.read.csv(odata,header=True,inferSchema=True)
# df.printSchema()
# df.show(5)

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
# data='F:\\bigdata\\drivers\\donations.csv'
data='F:\\bigdata\\drivers\\bank-full.csv'
df=spark.read.format('csv').option('header','True').option('sep',';').option('inferschema','true').load(data)
# res=df.groupBy(df.marital).count().alias('sum')

# res=df.groupBy(col('marital')).agg(count('*').alias('cnt'),sum(col('balance')).alias('smb')).where(col('balance')>=avg(col('balance')))


# res=df.groupBy(col('marital')).agg(count('balance')/
#         .alias('cnt'),avg(col('balance')).alias('avg'))/
# .where(col('balance')>(col('avg')))

# res=df.select(avg(col('balance))).where(col('balance')>avg('balance')).groupBy(col('balance'))

# res1 = df.select(avg('balance').alias('avg'))
#
# res = df.select(col('marital'),col('balance'),col('age'),col('job')).where(col('balance')>
#
# res.show()


# res=df.groupBy(col('marital')).agg(sum(col('balance')).alias('smb')).orderBy(col('smb').desc())
# res=df.select(col('age'),col('marital'),col("balance")).where((col('age')>60) & (col('marital')!='married'))
# res=df.select(col('age'),col('marital'),col("balance")).where((col('age')>60) | (col('marital')=='married') & (col('balance')>=40000))

# res=df.groupBy(col("marital")).agg(sum(col("balance")).alias("smb")).orderBy(col("smb").desc())
# df.createOrReplaceTempView('tab')
# res=spark.sql('select marital,count(balance) from tab group by marital')
# res.printSchema()

# res1=spark.sql('select marital,sum(balance) from tab group by marital')
# res.show()

df.createOrReplaceTempView('tab')
res=spark.sql('select * from tab where balance > (select avg(balance) from tab)')
res.show()
# df.printSchema()

