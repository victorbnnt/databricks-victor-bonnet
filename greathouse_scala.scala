// Databricks notebook source
// MAGIC %md
// MAGIC # --- Bronze to silver ---

// COMMAND ----------

// MAGIC %md
// MAGIC # 1. Mounting containers

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
// MAGIC ## 1.2 Silver

// COMMAND ----------

// MAGIC %python
// MAGIC containerSourceSilver = "wasbs://silver@storagegreathouse.blob.core.windows.net/"
// MAGIC containerMountSilver = "/mnt/greathouse_silver"
// MAGIC 
// MAGIC list_mounted = list(map(lambda x: x.mountPoint, dbutils.fs.mounts()))
// MAGIC 
// MAGIC if (containerMountSilver not in list_mounted):
// MAGIC     dbutils.fs.mount(
// MAGIC       source = containerSourceSilver,
// MAGIC       mount_point = containerMountSilver,
// MAGIC       extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
// MAGIC     )
// MAGIC else:
// MAGIC     print("Already mounted")

// COMMAND ----------

// MAGIC %md
// MAGIC # 2. Load files

// COMMAND ----------

val list_files = dbutils.fs.ls("/mnt/greathouse_raw")
val list_csv = dbutils.fs.ls("/mnt/greathouse_raw").map(_.path)
list_csv.foreach(println)

// COMMAND ----------

println(list_csv(0))
val greathouseDistrictDF = spark.read.options(Map("delimiter"->";")).option("header",true).option("inferSchema", true).csv(list_csv(0))
greathouseDistrictDF.show(2)
println(list_csv(1))
val realEstateDF = spark.read.options(Map("delimiter"->",")).option("header",true).option("inferSchema", true).csv(list_csv(1))
realEstateDF.show(2)
println(list_csv(2))
val rogerBrotherDF = spark.read.options(Map("delimiter"->";")).option("header",true).option("inferSchema", true).csv(list_csv(2))
rogerBrotherDF.show(2)
println(list_csv(3))
val stockGHHoldingsDF = spark.read.options(Map("delimiter"->";")).option("header",true).option("inferSchema", true).csv(list_csv(3))
stockGHHoldingsDF.show(2)
println(list_csv(4))
val stockRBholdingDF = spark.read.options(Map("delimiter"->";")).option("header",true).option("inferSchema", true).csv(list_csv(4))
stockRBholdingDF.show(2)

// COMMAND ----------

// MAGIC %fs ls "/mnt/greathouse_raw"

// COMMAND ----------

// MAGIC %md
// MAGIC # 3. Imports

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC # 4. Cleaning stock dataframes

// COMMAND ----------

val stockRBholdingDF_clean = stockRBholdingDF.toDF().withColumn("price", regexp_replace(col("price"), lit(","), lit(".")).cast("Float"))

// COMMAND ----------

stockRBholdingDF_clean.show(3)

// COMMAND ----------

stockRBholdingDF_clean.createOrReplaceTempView("stockRBholdingDF")
spark.sql("SELECT max(price) FROM stockRBholdingDF").show()

// COMMAND ----------

stockRBholdingDF_clean.describe().show()

// COMMAND ----------

val stockGHHoldingsDF_clean = stockGHHoldingsDF.toDF().withColumn("price", regexp_replace(col("price"), lit(","), lit(".")).cast("Float"))

// COMMAND ----------

stockGHHoldingsDF_clean.show(3)

// COMMAND ----------

stockGHHoldingsDF_clean.createOrReplaceTempView("stockGHHoldingsDF")
spark.sql("SELECT max(price) FROM stockGHHoldingsDF").show()

// COMMAND ----------

stockGHHoldingsDF_clean.describe().show()

// COMMAND ----------

println(stockGHHoldingsDF_clean.dropDuplicates().count())
println(stockGHHoldingsDF_clean.count())
println(stockGHHoldingsDF_clean.na.drop().count())
println("----")
println(stockRBholdingDF_clean.dropDuplicates().count())
println(stockRBholdingDF_clean.count())
println(stockRBholdingDF_clean.na.drop().count())

// COMMAND ----------

// MAGIC %md
// MAGIC # 5. Concatenate stock dataframes

// COMMAND ----------

val stock_houses_final = stockGHHoldingsDF_clean.union(stockRBholdingDF_clean)

// COMMAND ----------

// MAGIC %md
// MAGIC # 6. Saving stock dataframes

// COMMAND ----------

stock_houses_final.toDF().write.mode("overwrite").format("csv").option("header",true).option("delimiter", ";").save("/mnt/greathouse_silver/stock_houses_final_raw.csv")

// COMMAND ----------

//println(list_csv(0))
//val test = spark.read.options(Map("delimiter"->";")).option("header",true).option("inferSchema", true).csv("/mnt/greathouse_silver/stock_houses_final_raw.csv")
//test.show(2)

// COMMAND ----------

// MAGIC %md
// MAGIC # 7. Real estate dataframe cleaning

// COMMAND ----------

realEstateDF.printSchema()

// COMMAND ----------

println(realEstateDF.count())
println(realEstateDF.na.drop().count())
println(realEstateDF.dropDuplicates.count())

// COMMAND ----------

val realEstateDF_final = realEstateDF.na.drop()
realEstateDF_final.show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC # 8. Saving Real estate dataframe

// COMMAND ----------

realEstateDF_final.write.mode("overwrite").format("csv").option("header",true).option("delimiter", ";").save("/mnt/greathouse_silver/real_estate_final_raw.csv")

// COMMAND ----------

//val test = spark.read.options(Map("delimiter"->";")).option("header",true).option("inferSchema", true).csv("/mnt/greathouse_silver/real_estate_final_raw.csv")
//test.count()

// COMMAND ----------


