from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("job-1-spark") \
    .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
    .config("fs.s3a.endpoint", "http://172.27.63.8:9000") \
    .config("fs.s3a.access.key", "admin")\
    .config("fs.s3a.secret.key", "password")\
    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.path.style.access", "True")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("csv")\
    .option("header", "True")\
    .option("inferSchema","True")\
    .csv("s3a://landing/*.csv")

print ("\nImprime os dados lidos da lading:")
print (df.show())

print ("\nImprime o schema do dataframe lido da raw:")
print (df.printSchema())

print ("\nEscrevendo os dados lidos da raw para parquet na processing zone...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("s3a://processing/df-parquet-file.parquet")

df_parquet = spark.read.format("parquet")\
 .load("s3a://processing/df-parquet-file.parquet")

print ("\nDados lidos em parquet da processing zone")
print (df_parquet.show())

df_parquet.createOrReplaceTempView("view_df_parquet")

df_result = spark.sql("SELECT BNF_CODE as Bnf_code \
                       ,SUM(ACT_COST) as Soma_Act_cost \
                       ,SUM(QUANTITY) as Soma_Quantity \
                       ,SUM(ITEMS) as Soma_items \
                       ,AVG(ACT_COST) as Media_Act_cost \
                      FROM view_df_parquet \
                      GROUP BY bnf_code")

print ("\n ========= Imprime o resultado do dataframe processado =========\n")
print (df_result.show())

print ("\nEscrevendo os dados processados na Curated Zone...")

df_result.write.format("parquet")\
         .mode("overwrite")\
         .save("s3a://curated/df-result-file.parquet")

spark.stop()