// Databricks notebook source
// MAGIC %md
// MAGIC # Streaming

// COMMAND ----------

// MAGIC %md
// MAGIC # 1. Mount containers

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.1 Bronze

// COMMAND ----------

// MAGIC %python
// MAGIC containerSourceBronze = "wasbs://bronze@storagegreathouse.blob.core.windows.net/"
// MAGIC containerMountBronze = "/mnt/greathouse_bronze"
// MAGIC 
// MAGIC list_mounted = list(map(lambda x: x.mountPoint, dbutils.fs.mounts()))
// MAGIC 
// MAGIC if (containerMountBronze not in list_mounted):
// MAGIC     dbutils.fs.mount(
// MAGIC       source = containerSourceBronze,
// MAGIC       mount_point = containerMountBronze,
// MAGIC       extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
// MAGIC     )
// MAGIC else:
// MAGIC     print("Already mounted")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.2 Streaming

// COMMAND ----------

// MAGIC %python
// MAGIC containerSourceStream = "wasbs://streaming@storagegreathouse.blob.core.windows.net/"
// MAGIC containerMountStream = "/mnt/greathouse_streaming"
// MAGIC 
// MAGIC list_mounted = list(map(lambda x: x.mountPoint, dbutils.fs.mounts()))
// MAGIC 
// MAGIC if (containerMountStream not in list_mounted):
// MAGIC     dbutils.fs.mount(
// MAGIC       source = containerSourceStream,
// MAGIC       mount_point = containerMountStream,
// MAGIC       extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
// MAGIC     )
// MAGIC else:
// MAGIC     print("Already mounted")

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

dbutils.fs.ls("/mnt/greathouse_streaming/source")

// COMMAND ----------

val dataPath = "/mnt/greathouse_streaming/source"

// COMMAND ----------

val getSchema = spark.read.options(Map("delimiter"->",")).option("header",true).option("inferSchema", true).csv("/mnt/greathouse_streaming/source/Real Estate Ad.csv")
getSchema.printSchema()

// COMMAND ----------

val df = spark.readStream.schema(getSchema.schema).option("maxFilesPerTrigger", 1).options(Map("delimiter"->",")).option("header",true).csv("/mnt/greathouse_streaming/source/Real Estate Ad.csv")
println("Streaming DataFrame : " + df.isStreaming)

// COMMAND ----------

val df2 = df.withColumnRenamed("longitude", "longitudeX")
df2.printSchema()

// COMMAND ----------

val basePath = "/mnt/greathouse_streaming/workingdir"
dbutils.fs.mkdirs(basePath)
val outputPathDir = "/mnt/greathouse_streaming/output"
dbutils.fs.mkdirs(outputPathDir)
val checkpointPath = basePath + "/checkpoint"
dbutils.fs.mkdirs(checkpointPath)

// COMMAND ----------

df2.writeStream 
      .queryName("stream_1p")  
      //.trigger(processingTime="3 seconds")
      .format("parquet")
      .option("checkpointLocation", checkpointPath)
      .outputMode("append")
      .start(outputPathDir)
      .awaitTermination()

// COMMAND ----------


