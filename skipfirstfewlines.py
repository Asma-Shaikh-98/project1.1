from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
data="F:\\bigdata\\drivers\\asl1.csv"
drdd=sc.textFile(data)
res = drdd.zipWithIndex().filter(lambda x:x[1]>=5).map(lambda x:x[0])
df=spark.read.csv(res,header=True)
df.show()
# for i in res.collect():
#    print(i)
