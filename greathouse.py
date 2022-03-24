# Databricks notebook source
containerSourceBronze = "wasbs://bronze@storagegreathouse.blob.core.windows.net/"
containerMountBronze = "/mnt/greathouse_bronze"

# COMMAND ----------

list_mounted = list(map(lambda x: x.mountPoint, dbutils.fs.mounts()))
list_mounted

# COMMAND ----------

if (containerMountBronze not in list_mounted):
    dbutils.fs.mount(
      source = containerSourceBronze,
      mount_point = containerMountBronze,
      extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
    )
else:
    print("Already mounted")

# COMMAND ----------

# MAGIC %scala
# MAGIC val list_files = dbutils.fs.ls("/mnt/greathouse_raw")

# COMMAND ----------

# MAGIC %scala
# MAGIC val list_csv = dbutils.fs.ls("/mnt/greathouse_raw").map(_.path)
# MAGIC list_csv.foreach(println)

# COMMAND ----------

# MAGIC %scala
# MAGIC println(list_csv(0))
# MAGIC val greathouseDistrictDF = spark.read.options(Map("delimiter"->";")).option("header",true).csv(list_csv(0))
# MAGIC greathouseDistrictDF.show(2)

# COMMAND ----------

# MAGIC %scala
# MAGIC println(list_csv(1))
# MAGIC val realEstateDF = spark.read.options(Map("delimiter"->",")).option("header",true).csv(list_csv(1))
# MAGIC realEstateDF.show(2)

# COMMAND ----------

# MAGIC %scala
# MAGIC println(list_csv(2))
# MAGIC val rogerBrotherDF = spark.read.options(Map("delimiter"->";")).option("header",true).csv(list_csv(2))
# MAGIC rogerBrotherDF.show(2)

# COMMAND ----------

# MAGIC %scala
# MAGIC println(list_csv(3))
# MAGIC val stockGHHoldingsDF = spark.read.options(Map("delimiter"->";")).option("header",true).csv(list_csv(3))
# MAGIC stockGHHoldingsDF.show(2)

# COMMAND ----------

# MAGIC %scala
# MAGIC println(list_csv(4))
# MAGIC val stockRBholdingDF = spark.read.options(Map("delimiter"->";")).option("header",true).csv(list_csv(4))
# MAGIC stockRBholdingDF.show(2)

# COMMAND ----------

# MAGIC %scala
# MAGIC rogerBrotherDF.count()

# COMMAND ----------

# MAGIC %scala
# MAGIC greathouseDistrictDF.count()

# COMMAND ----------

# MAGIC %fs ls "/mnt/greathouse_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC # PYTHON

# COMMAND ----------

import pandas as pd
import os
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading coordinates greathouse holdings

# COMMAND ----------

df1 = spark.read \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .option("header", True) \
    .csv("/mnt/greathouse_bronze/GreatHouse Holdings District.csv")
print(df1.count())
print(df1.dropDuplicates().count())
df1.show(3)

# COMMAND ----------

df1.describe(['longitude', 'latitude']).show()
df1.distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading coordinates Roger&Brothers

# COMMAND ----------

df3 = spark.read \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .option("header", True) \
    .csv("/mnt/greathouse_raw/Roger&Brothers District.csv")
print(df3.count())
print(df3.dropDuplicates().count())
df3.show(3)

# COMMAND ----------

df3.describe(['longitude', 'latitude']).show()
df3.distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenate coordinate dataframes

# COMMAND ----------

district_locations_both = df1.union(df3)
print(district_locations_both.count())
print(df3.count() + df1.count())
print(district_locations_both.dropDuplicates().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Conclusion: coordinate contained in one dataframe are also contained in the other

# COMMAND ----------

# MAGIC %md
# MAGIC ## loading real estate ad csv

# COMMAND ----------

df2 = spark.read \
    .option("delimiter", ",") \
    .option("inferSchema", True) \
    .option("header", True) \
    .csv("/mnt/greathouse_raw/Real Estate Ad.csv")
print(df2.count())
print(df2.dropDuplicates().count())
df2.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter real estate ad csv on coordinates provided in districts dataframes

# COMMAND ----------

df2.join(district_locations_both, [df2.longitude == district_locations_both.longitude, df2.latitude == district_locations_both.latitude], "inner").dropDuplicates().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Conclusion: 0 rows are removed from the Real Estate Ad dataframe after filtering with coordinates provided in districts dataframe
# MAGIC ##### Conclusion: coordinates dataframes will not be considered for further analysis are they are useless

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

print(df2.dropDuplicates().count())
print(df2.dropna().count())

# COMMAND ----------

real_estate_final = df2.dropna()
real_estate_final.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading stocks greathouse holding

# COMMAND ----------

df4 = spark.read \
    .option("delimiter", ";") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_raw/Stock GreatHouse Holdings.csv")
print(df4.count())
print(df4.dropDuplicates().count())
df4.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading stocks ROGER & brothers

# COMMAND ----------

df5 = spark.read \
    .option("delimiter", ";") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_raw/Stock Roger&Brothers.csv")
print(df5.count())
print(df5.dropDuplicates().count())
df5.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatenation of the two stocks dataframes

# COMMAND ----------

stock_houses_to_sell = df4.union(df5)
print(stock_houses_to_sell.count())
print(df4.count() + df5.count())
print(stock_houses_to_sell.dropDuplicates().count())
print(stock_houses_to_sell.dropna().count())

# COMMAND ----------

stock_houses_to_sell.describe().show()
stock_houses_to_sell.distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Here we have a bad formatting for prices, not recognized as float. We convert the format

# COMMAND ----------

# cleaning stocks df + type conversion
stock_houses_to_sell_c = stock_houses_to_sell.withColumn("price",regexp_replace("price", ",", ".")).withColumn("price",col("price").cast("double"))
stock_houses_to_sell_c.describe().show()
stock_houses_to_sell_c.distinct().count()

# COMMAND ----------

stocks_final = stock_houses_to_sell_c

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting silver

# COMMAND ----------

containerSourceSilver = "wasbs://silver@storagegreathouse.blob.core.windows.net/"
containerMountSilver = "/mnt/greathouse_silver"

# COMMAND ----------

if (containerMountSilver not in list_mounted):
    dbutils.fs.mount(
      source = containerSourceSilver,
      mount_point = containerMountSilver,
      extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
    )
else:
    print("Already mounted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving to silver

# COMMAND ----------

stocks_final.write.mode("overwrite").format("csv").save("/mnt/greathouse_silver/stocks_final.csv", header = 'true')
real_estate_final.write.mode("overwrite").format("csv").save("/mnt/greathouse_silver/real_estate_final.csv", header = 'true')

# COMMAND ----------

test = spark.read \
    .option("delimiter", ",") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_silver/stocks_final.csv").toPandas()
print(test.shape)
test.tail()
