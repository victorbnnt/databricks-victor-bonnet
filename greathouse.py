# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://bronze@storagegreathouse.blob.core.windows.net/",
  mount_point = "/mnt/greathouse_raw",
  extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
)

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
# MAGIC PYTHON

# COMMAND ----------

import pandas as pd
import os

# COMMAND ----------

# MAGIC %md
# MAGIC <b style="color:orange">Loading coordinates greathouse holdings</b>

# COMMAND ----------

df1 = spark.read \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .option("header", True) \
    .csv("/mnt/greathouse_raw/GreatHouse Holdings District.csv").toPandas()
print(df1.shape)
print(df1.drop_duplicates().shape)
df1.head()

# COMMAND ----------

# MAGIC %md
# MAGIC <b style="color:orange">Loading coordinates Roger&Brothers</b>

# COMMAND ----------

df3 = spark.read \
    .option("delimiter", ";") \
    .option("inferSchema", True) \
    .option("header", True) \
    .csv("/mnt/greathouse_raw/Roger&Brothers District.csv").toPandas()
print(df3.shape)
print(df3.drop_duplicates().shape)
df3.head()

# COMMAND ----------

# MAGIC %md
# MAGIC <b style="color:orange">Concatenate coordinate dataframes</b>

# COMMAND ----------

district_locations_both = pd.concat([df1, df3], ignore_index=True).drop_duplicates()
print(district_locations_both.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC <b style="color:orange">loading real estate ad csv</b>

# COMMAND ----------

df2 = spark.read \
    .option("delimiter", ",") \
    .option("inferSchema", True) \
    .option("header", True) \
    .csv("/mnt/greathouse_raw/Real Estate Ad.csv").toPandas()
print(df2.shape)
df2.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC <b style="color:orange">filter real estate ad csv ==> does nothing</b>

# COMMAND ----------

df2_filtered = pd.merge(district_locations_both, df2,  how='left', left_on=['longitude','latitude'], right_on = ['longitude','latitude'])
df2_filtered.shape

# COMMAND ----------

df2_filtered.head(3)

# COMMAND ----------

df2.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC <b style="color:orange">Create a column that is a concatenation of longitude and latitude columns to be used with groupby</b>

# COMMAND ----------

df2_grouped = df2.copy()
df2_grouped["coordConcat"] = df2_grouped["longitude"].apply(str) + df2_grouped["latitude"].apply(str)
df2_grouped

# COMMAND ----------

df2_grouped["coordConcat"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC <b style="color:orange">Groupby and mean on numerical values / most frequent on categorical values</b>

# COMMAND ----------

# groupby sur les coordinates identiques 
#       => mean sur les valeurs numériques
#       => most frequent sur les valeurs catégoriques
df2_cleaned = df2_grouped.groupby("coordConcat").agg({'longitude': 'mean',
                                'latitude': 'mean',
                                'housing_median_age': 'mean',
                                'total_rooms': 'mean',
                                'total_bedrooms': 'mean',
                                'population': 'mean',
                                'households': 'mean',
                                'median_income': 'mean',
                                'median_house_value': 'mean',
                                'ocean_proximity': lambda x:x.value_counts().index[0]
                               }).reset_index().drop(columns="coordConcat")
df2_cleaned.head()

# COMMAND ----------

df2_cleaned.ocean_proximity.value_counts()

# COMMAND ----------

# dataframe contenant les informations sur les districts (en partant sur la base que une couple de coordinate identique sont un distric)
districts_informations = df2_cleaned.copy()
districts_informations.head(4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## loading stocks greathouse holding

# COMMAND ----------

df4 = spark.read \
    .option("delimiter", ";") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_raw/Stock GreatHouse Holdings.csv").toPandas()
print(df4.shape)
df4.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## loading stocks ROGER & brothers

# COMMAND ----------

df5 = spark.read \
    .option("delimiter", ";") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_raw/Stock Roger&Brothers.csv").toPandas()
print(df5.shape)
df5.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## concat

# COMMAND ----------

# La concatenation des dataframes stocks house est le deuxième dataframe que l'on veut
stock_houses_to_sell = pd.concat([df4, df5], ignore_index=True)
stock_houses_to_sell.head(3)

# COMMAND ----------

stock_houses_to_sell.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ## mounting silver

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://silver@storagegreathouse.blob.core.windows.net/",
  mount_point = "/mnt/greathouse_silver",
  extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## saving to silver

# COMMAND ----------

stocks_final = spark.createDataFrame(stock_houses_to_sell)
district_final = spark.createDataFrame(df2)

# COMMAND ----------

stocks_final.write.mode("overwrite").format("csv").save("/mnt/greathouse_silver/stocks_final.csv", header = 'true')
district_final.write.mode("overwrite").format("csv").save("/mnt/greathouse_silver/district_final.csv", header = 'true')

# COMMAND ----------

test = spark.read \
    .option("delimiter", ",") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_silver/stocks_final.csv").toPandas()
print(test.shape)
test.tail()

# COMMAND ----------


