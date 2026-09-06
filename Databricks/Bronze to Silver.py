# Databricks notebook source
### Bronze Logic 
from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType 

table_names =[]

for i in dbutils.fs.ls("abfss://bronze@sanstrg.dfs.core.windows.net/SalesLT/"):
    table_names.append(i.name.split("/")[0])

table_names

for i in table_names:
    path =f"abfss://bronze@sanstrg.dfs.core.windows.net/SalesLT/{i}/SalesLT.{i}.parquet"
    path_dest = f"abfss://silver@sanstrg.dfs.core.windows.net/SalesLT/{i}/"
    df = spark.read.format("parquet")\
        .option("inferSchema", True)\
        .load(path)
    column_list = df.columns
    
    for col in column_list:
        if "Date" in col or "date" in col:
            df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
    
    df.write.mode("overwrite").format("delta").save(path_dest)
    print (f"Table {i} has been processed")

table_names = []
for i in dbutils.fs.ls("abfss://bronze@sanstrg.dfs.core.windows.net/dbo/"):
    table_names.append(i.name.split("/")[0])

for i in table_names:
    path =f"abfss://bronze@sanstrg.dfs.core.windows.net/dbo/{i}/dbo.{i}.parquet"
    path_dest = f"abfss://silver@sanstrg.dfs.core.windows.net/dbo/{i}/"
    df = spark.read.format("parquet")\
        .option("inferSchema", True)\
        .load(path)
    df.write.mode("overwrite").format("delta").save(path_dest)
    print (f"Table {i} has been processed")