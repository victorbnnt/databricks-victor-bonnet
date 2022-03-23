# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting silver

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://silver@storagegreathouse.blob.core.windows.net/",
  mount_point = "/mnt/greathouse_silver",
  extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
)

# COMMAND ----------

# MAGIC %md
# MAGIC # loading the dataframes

# COMMAND ----------

stocksDF = spark.read \
    .option("delimiter", ",") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_silver/stocks_final.csv").toPandas()
print(stocksDF.shape)
stocksDF.head()

# COMMAND ----------

districtsDF = spark.read \
    .option("delimiter", ",") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_silver/district_final.csv").toPandas()
print(districtsDF.shape)
districtsDF.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # Average price per number of rooms on stocks dataframe

# COMMAND ----------

stocksDF_sp = spark.read \
    .option("delimiter", ",") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_silver/stocks_final.csv")
stocksDF_sp.show()

# COMMAND ----------

# cleaning stocks df + type conversion
stocksDF_sp_c = stocksDF_sp.withColumn("price",regexp_replace("price", ",", ".")).withColumn("price",col("price").cast("double"))

# COMMAND ----------

stocksDF_sp_c.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

stocksDF_sp_mean_price_per_rooms = stocksDF_sp_c.groupBy("Rooms").mean("price").withColumnRenamed("avg(price)", "mean_price").orderBy(col("Rooms").asc())
stocksDF_sp_mean_price_per_rooms.show()

# COMMAND ----------

plt.scatter(stocksDF_sp_mean_price_per_rooms.toPandas()["mean_price"], stocksDF_sp_mean_price_per_rooms.toPandas()["Rooms"]);

# COMMAND ----------

stocksDF_sp_mean_price_per_bedrooms = stocksDF_sp_c.groupBy("Bedrooms").mean("price").withColumnRenamed("avg(price)", "mean_price").orderBy(col("Bedrooms").asc())
stocksDF_sp_mean_price_per_bedrooms.show()

# COMMAND ----------

plt.scatter(stocksDF_sp_mean_price_per_bedrooms.toPandas()["mean_price"], stocksDF_sp_mean_price_per_bedrooms.toPandas()["Bedrooms"]);

# COMMAND ----------

# MAGIC %md
# MAGIC # Average price per number of rooms on district dataframe

# COMMAND ----------

districtDF_sp = spark.read \
    .option("delimiter", ",") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_silver/district_final.csv")
districtDF_sp.show()

# COMMAND ----------

districtDF_sp_mean_price_per_rooms = districtDF_sp.groupBy("total_rooms").mean("median_house_value").withColumnRenamed("avg(median_house_value)", "mean_price").orderBy(col("total_rooms").asc())
districtDF_sp_mean_price_per_rooms.show()

# COMMAND ----------

plt.scatter(districtDF_sp_mean_price_per_rooms.toPandas()["mean_price"], districtDF_sp_mean_price_per_rooms.toPandas()["total_rooms"]);

# COMMAND ----------

districtDF_sp_mean_price_per_bedrooms = districtDF_sp.groupBy("total_bedrooms").mean("median_house_value").withColumnRenamed("avg(median_house_value)", "mean_price").orderBy(col("total_bedrooms").asc())
districtDF_sp_mean_price_per_bedrooms.show()

# COMMAND ----------

plt.scatter(districtDF_sp_mean_price_per_bedrooms.toPandas()["mean_price"], districtDF_sp_mean_price_per_bedrooms.toPandas()["total_bedrooms"]);

# COMMAND ----------


