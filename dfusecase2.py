# For building anything related to spark these three lines are mandatory(for any pyspark code)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data='F:\\Asma\\Spark_by_venu_Sir\\datasets\\10000Records.csv'
df=spark.read.format('csv').option('header','true').option('sep',',').option('inferschema','true').load(data)

# num = int(df.count())
# df.show(num,truncate=False)
# df.printSchema()


# ---------------------
import re
num = int(df.count())
cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
# cols=[re.sub(' ',"",c) for c in df.columns]
ndf = df.toDF(*cols)
ndf.show()
# Data Processing using programming friendly

res=ndf.withColumn("today",current_date()).withColumn("dateofbirth",to_date(col("dateofbirth"),'M/d/yyyy')).withColumn("diff1",datediff(col("today"),col("dateofbirth"))).where(col("gender")=='F')
# res = ndf.groupBy(col('gender')).agg(count("*").alias("cnt"))
# res1 = ndf.groupBy(col('gender')).agg(count(col("*")).alias("cnt"))

res.show()
# res.printSchema()
# res1.show()
# res1.printSchema()

# spark-submit --master local --deploy-mode client F:/bigdata/spark-3.1.2-bin-hadoop3.2/python/pyspark/pyspark_projects/project1/dfusecase2.py