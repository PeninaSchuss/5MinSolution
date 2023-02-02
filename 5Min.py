from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
APP = 'BeeHive'

spark = SparkSession.builder.appName(APP).getOrCreate()

df = spark.read.options(inferSchema='True').csv("BeeHiveTestData.csv")
df = df.withColumnRenamed("_c5", "Father SIZE")
df = df.withColumnRenamed("_c3", "DaughtersEfficiencyScore")
base_schema = df.schema

window = Window.partitionBy('Father SIZE').orderBy((col("DaughtersEfficiencyScore")).asc())

n_min = (
    df.withColumn("row_num", row_number().over(window))
    .filter(col("row_num") <= 5)
    .select("Father SIZE", col("row_num").alias("rank of min"), col("DaughtersEfficiencyScore").alias("lowest n (n = 5) per father size")))

n_min.show(55)
