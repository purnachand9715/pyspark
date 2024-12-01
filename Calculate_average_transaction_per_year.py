#This is to calculate average transaction amount per yer

from pyspark.sql import SparkSession
from pyspark.sql.functions import year

spark = SparkSession.builder.appName("Calculate average transaction amount year ").getOrCreate()

transData = [
    (1, 269, '2018-08-15', 500),
    (2, 478, '2018-11-25', 400),
    (3, 269, '2019-01-05', 1000),
    (4, 123, '2020-10-20', 600),
    (5, 478, '2021-07-05', 700),
    (6, 123, '2022-03-05', 900)
]

columns = ['transaction_id', 'user_id', 'transaction_date','transaction_amount']

df_tnx = spark.createDataFrame(data=transData, schema=columns)

df_tnx.printSchema()

df_tnx.show()

df_year = df_tnx.withColumn('Year', year(df_tnx['transaction_date']))

df_year.show()

df_final = df_year.groupBy("Year","user_id").avg("transaction_amount").alias("avg_transaction_amount")

df_final.show()
