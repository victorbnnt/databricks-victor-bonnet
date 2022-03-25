// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC # 1. Mounting containers

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.1 Silver

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
// MAGIC ## 1.2. Gold

// COMMAND ----------

// MAGIC %python
// MAGIC containerSourceGold = "wasbs://gold@storagegreathouse.blob.core.windows.net/"
// MAGIC containerMountGold = "/mnt/greathouse_gold"
// MAGIC 
// MAGIC list_mounted = list(map(lambda x: x.mountPoint, dbutils.fs.mounts()))
// MAGIC 
// MAGIC if (containerMountGold not in list_mounted):
// MAGIC     dbutils.fs.mount(
// MAGIC       source = containerSourceGold,
// MAGIC       mount_point = containerMountGold,
// MAGIC       extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
// MAGIC     )
// MAGIC else:
// MAGIC     print("Already mounted")

// COMMAND ----------

// MAGIC %md
// MAGIC # 2. Load files

// COMMAND ----------

val stocksDF = spark.read 
    .option("delimiter", ",") 
    .option("header", true) 
    .option("inferSchema", true) 
    .csv("/mnt/greathouse_silver/stocks_final.csv")
println(stocksDF.count())
println(stocksDF.dropDuplicates().count())
stocksDF.show(3)

// COMMAND ----------

val realEstateDF = spark.read 
    .option("delimiter", ",") 
    .option("header", true) 
    .option("inferSchema", true) 
    .csv("/mnt/greathouse_silver/real_estate_final.csv")
println(realEstateDF.count())
println(realEstateDF.dropDuplicates().count())
realEstateDF.show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC # 3. Outliers

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3.1. Price on stock dataframe

// COMMAND ----------

display(stocksDF)

// COMMAND ----------

val price_array = stocksDF.select("price").map(f=>f.getDouble(0)).collect().toArray
val meanprice = price_array.sum/price_array.length
val stdDev = Math.sqrt((price_array.map( _ - meanprice).map(t => t*t).sum)/price_array.length)

// COMMAND ----------

val stocksDF_new = stocksDF.filter($"price" < (meanprice + 3*stdDev)).filter($"price" > (meanprice - 3*stdDev))
stocksDF_new.count()

// COMMAND ----------

display(stocksDF_new)

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3.2. Rooms on stock dataframe

// COMMAND ----------

stocksDF_new.select("Rooms").map(f=>f.getInt(0).toLong).collect().max

// COMMAND ----------

stocksDF_new.select("Rooms").describe().show()

// COMMAND ----------

val rooms_array = stocksDF_new.select("Rooms").map(f=>f.getInt(0).toLong).collect().toArray
val meanRooms = rooms_array.sum/rooms_array.length.toLong
val stdDevRooms = Math.sqrt((rooms_array.map( _ - meanRooms)).map(t => t*t).sum)/rooms_array.length

// COMMAND ----------

val stocksDF_new_v2 = stocksDF_new.filter($"Rooms" < (meanRooms + 3*stdDevRooms))
stocksDF_new_v2.count()

// COMMAND ----------

stocksDF_new_v2.select("Rooms").describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC # 4. Questions
// MAGIC ## Average price per number of rooms on stocks dataframe

// COMMAND ----------

stocksDF_new_v2.printSchema()

// COMMAND ----------

stocksDF_new_v2.head()

// COMMAND ----------

val stocksDF_mean_price_per_rooms = stocksDF_new_v2.groupBy("Rooms").mean("price").withColumnRenamed("avg(price)", "mean_price").orderBy($"Rooms".asc)
stocksDF_mean_price_per_rooms.show()

// COMMAND ----------

display(stocksDF_mean_price_per_rooms)

// COMMAND ----------

val stocksDF_mean_price_per_bedrooms = stocksDF_new_v2.groupBy("Bedrooms").mean("price").withColumnRenamed("avg(price)", "mean_price").orderBy($"Bedrooms".asc)
stocksDF_mean_price_per_bedrooms.show()

// COMMAND ----------

display(stocksDF_mean_price_per_bedrooms)

// COMMAND ----------

// MAGIC %md
// MAGIC # Analyse on district dataframe

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating districts by coordinate ranges

// COMMAND ----------

val minLat = 

// COMMAND ----------

val minLat = stocksDF_new_v2.select("latitude").map(f => math.floor(f.getDouble(0))).collect().min
val maxLat = stocksDF_new_v2.select("latitude").map(f => math.ceil(f.getDouble(0))).collect().max
val minLon = stocksDF_new_v2.select("longitude").map(f => math.floor(f.getDouble(0))).collect().min
val maxLon = stocksDF_new_v2.select("longitude").map(f => math.ceil(f.getDouble(0))).collect().max

val Dif = 30


// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC # TODO

// COMMAND ----------

// MAGIC %python
// MAGIC stocksDF.toPandas()[stocksDF.toPandas()["price"] < (stocksDF.toPandas()["price"].mean() + 3*stocksDF.toPandas()["price"].std())].shape
// MAGIC stocksDF.toPandas()[stocksDF.toPandas()["price"] < (stocksDF.toPandas()["price"].mean() + 3*stocksDF.toPandas()["price"].std())][stocksDF.toPandas()["price"] > (stocksDF.toPandas()["price"].mean() - 3*stocksDF.toPandas()["price"].std())].boxplot(column="price")

// COMMAND ----------

// MAGIC %python
// MAGIC stocksDF_new = stocksDF.toPandas()[stocksDF.toPandas()["price"] < (stocksDF.toPandas()["price"].mean() + 3*stocksDF.toPandas()["price"].std())][stocksDF.toPandas()["price"] > (stocksDF.toPandas()["price"].mean() - 3*stocksDF.toPandas()["price"].std())]
// MAGIC stocksDF_new.shape

// COMMAND ----------

// MAGIC %python
// MAGIC stocksDF_new[stocksDF_new["Rooms"] < (stocksDF_new["Rooms"].mean() + 3*stocksDF_new["Rooms"].std())][stocksDF_new["Rooms"] > (stocksDF_new["Rooms"].mean() - 3*stocksDF_new["Rooms"].std())].boxplot(column="Rooms")
// MAGIC stocksDF_new[stocksDF_new["Rooms"] < (stocksDF_new["Rooms"].mean() + 3*stocksDF_new["Rooms"].std())][stocksDF_new["Rooms"] > (stocksDF_new["Rooms"].mean() - 3*stocksDF_new["Rooms"].std())].shape

// COMMAND ----------

// MAGIC %python
// MAGIC stocksDF_new_v2 = stocksDF_new[stocksDF_new["Rooms"] < (stocksDF_new["Rooms"].mean() + 3*stocksDF_new["Rooms"].std())][stocksDF_new["Rooms"] > (stocksDF_new["Rooms"].mean() - 3*stocksDF_new["Rooms"].std())]

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF = spark.read \
// MAGIC     .option("delimiter", ",") \
// MAGIC     .option("header", True) \
// MAGIC     .option("inferSchema", True) \
// MAGIC     .csv("/mnt/greathouse_silver/real_estate_final.csv")
// MAGIC print(realEstateDF.count())
// MAGIC print(realEstateDF.dropDuplicates().count())
// MAGIC realEstateDF.show(3)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC import matplotlib.pyplot as plt
// MAGIC import pandas as pd
// MAGIC import numpy as np
// MAGIC import math

// COMMAND ----------

// MAGIC %md
// MAGIC # Question 1
// MAGIC ## Average price per number of rooms on stocks dataframe

// COMMAND ----------

// MAGIC %python
// MAGIC # cleaning stocks df + type conversion
// MAGIC stocksSpark = spark.createDataFrame(stocksDF_new_v2)
// MAGIC stocksSpark.printSchema()

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.head()

// COMMAND ----------

// MAGIC %python
// MAGIC stocksDF_mean_price_per_rooms = stocksSpark.groupBy("Rooms").mean("price").withColumnRenamed("avg(price)", "mean_price").orderBy(col("Rooms").asc())
// MAGIC stocksDF_mean_price_per_rooms.show()

// COMMAND ----------

// MAGIC %python
// MAGIC plt.scatter(stocksDF_mean_price_per_rooms.toPandas()["mean_price"], stocksDF_mean_price_per_rooms.toPandas()["Rooms"]);

// COMMAND ----------

// MAGIC %python
// MAGIC stocksDF_mean_price_per_bedrooms = stocksSpark.groupBy("Bedrooms").mean("price").withColumnRenamed("avg(price)", "mean_price").orderBy(col("Bedrooms").asc())
// MAGIC stocksDF_mean_price_per_bedrooms.show()

// COMMAND ----------

// MAGIC %python
// MAGIC plt.scatter(stocksDF_mean_price_per_bedrooms.toPandas()["mean_price"], stocksDF_mean_price_per_bedrooms.toPandas()["Bedrooms"]);

// COMMAND ----------

// MAGIC %md
// MAGIC # Analyse on district dataframe

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating districts by coordinate ranges

// COMMAND ----------

// MAGIC %python
// MAGIC minLat = math.floor(realEstateDF.toPandas()["latitude"].min())
// MAGIC maxLat = math.ceil(realEstateDF.toPandas()["latitude"].max())
// MAGIC minLon = math.floor(realEstateDF.toPandas()["longitude"].min())
// MAGIC maxLon = math.ceil(realEstateDF.toPandas()["longitude"].max())
// MAGIC #
// MAGIC Dif = 30
// MAGIC #
// MAGIC rangeLat = list(np.linspace(minLat, maxLat+1, Dif))  
// MAGIC #print(rangeLat)
// MAGIC latitudeRanges = []
// MAGIC #
// MAGIC #
// MAGIC for lat in range(0, len(rangeLat)):
// MAGIC     try:
// MAGIC         latitudeRanges.append([rangeLat[lat], rangeLat[lat+1]])
// MAGIC     except:
// MAGIC         pass
// MAGIC #
// MAGIC #print(latitudeRanges)
// MAGIC #
// MAGIC rangeLon = list(np.linspace(minLon-1, maxLon, Dif))
// MAGIC longitudeRanges = []
// MAGIC #
// MAGIC for lon in range(0, len(rangeLon), 1):
// MAGIC     try:
// MAGIC         longitudeRanges.append([rangeLon[lon], rangeLon[lon+1]])
// MAGIC     except:
// MAGIC         pass
// MAGIC print(longitudeRanges)

// COMMAND ----------

// MAGIC %python
// MAGIC districS = dict()
// MAGIC districtcounter = 0
// MAGIC for latbloc in latitudeRanges:
// MAGIC     for lonbloc in longitudeRanges:
// MAGIC         dis = {"lat": latbloc, "lon": lonbloc, "latplt": (latbloc[1]+latbloc[0])/2, "lonplt": (lonbloc[1]+lonbloc[0])/2}
// MAGIC         districS[str(districtcounter+1)] = dis
// MAGIC         districtcounter+=1
// MAGIC #districS

// COMMAND ----------

// MAGIC %python
// MAGIC def getDistrict(lon, lat, dictref):
// MAGIC     for k, v in dictref.items():
// MAGIC         if (lon > v["lon"][0] and lon <= v["lon"][1]) and (lat > v["lat"][0] and lat <= v["lat"][1]):
// MAGIC             return k

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.show(3)

// COMMAND ----------

// MAGIC %python
// MAGIC district_number = []
// MAGIC district_plt_lat = []
// MAGIC district_plt_lon = []
// MAGIC price_per_district = stocksSpark.toPandas()
// MAGIC for k in range(0, dfTemp.shape[0]):
// MAGIC     district_number.append(getDistrict(price_per_district["longitude"][k], price_per_district["latitude"][k], districS))
// MAGIC price_per_district["district_number"] = pd.Series(district_number)
// MAGIC price_per_district["district_plt_lat"] = price_per_district["district_number"].apply(lambda x: districS[x]["latplt"])
// MAGIC price_per_district["district_plt_lon"] = price_per_district["district_number"].apply(lambda x: districS[x]["lonplt"])
// MAGIC price_per_district.head()

// COMMAND ----------

// MAGIC %python
// MAGIC #price_per_district["district_number"].value_counts

// COMMAND ----------

// MAGIC %python
// MAGIC mean_price_per_district = price_per_district.groupby("district_number").agg({"price": "mean", "longitude": "mean", "latitude": "mean", "district_plt_lat": "mean", "district_plt_lon": "mean"})
// MAGIC mean_price_per_district.plot(kind="scatter", x="district_plt_lon", y="district_plt_lat",
// MAGIC     s=mean_price_per_district['price']/200, label="price",
// MAGIC     c="price", cmap=plt.get_cmap("jet"),
// MAGIC     colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC )
// MAGIC plt.legend()
// MAGIC plt.title("Mean price per district")
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %python
// MAGIC max_price_per_district = price_per_district.groupby("district_number").agg({"price": "max", "longitude": "mean", "latitude": "mean", "district_plt_lat": "mean", "district_plt_lon": "mean"})
// MAGIC max_price_per_district.plot(kind="scatter", x="district_plt_lon", y="district_plt_lat",
// MAGIC     s=max_price_per_district['price']/1000, label="price",
// MAGIC     c="price", cmap=plt.get_cmap("jet"),
// MAGIC     colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC )
// MAGIC plt.legend()
// MAGIC plt.title("Max price per district")
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %python
// MAGIC min_price_per_district = price_per_district.groupby("district_number").agg({"price": "min", "longitude": "mean", "latitude": "mean", "district_plt_lat": "mean", "district_plt_lon": "mean"})
// MAGIC min_price_per_district.plot(kind="scatter", x="district_plt_lon", y="district_plt_lat",
// MAGIC     s=min_price_per_district['price']/1000, label="price",
// MAGIC     c="price", cmap=plt.get_cmap("jet"),
// MAGIC     colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC )
// MAGIC plt.legend()
// MAGIC plt.title("Min price per district")
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Kind of houses most important in the market

// COMMAND ----------

// MAGIC %md
// MAGIC ##### ROOMS

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.groupBy("Rooms").count().orderBy(col("Rooms").desc()).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### BEDROOMS

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.groupBy("Bedrooms").count().orderBy(col("Bedrooms").desc()).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Rooms on different price ranges

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.toPandas().head(3)

// COMMAND ----------

// MAGIC %python
// MAGIC with_price_range = stocksSpark.toPandas().copy().sort_values(by="price")
// MAGIC with_price_range["priceRanges"] = pd.cut(stocksSpark.toPandas()["Rooms"], bins=15, labels=False)
// MAGIC with_price_range.head()

// COMMAND ----------

// MAGIC %python
// MAGIC price_per_number_of_rooms = with_price_range.groupby("priceRanges").agg({"Rooms": "mean", "price": "mean"}).sort_values(by='price', ascending=False).reset_index().drop(columns="priceRanges")
// MAGIC price_per_number_of_rooms

// COMMAND ----------

// MAGIC %python
// MAGIC price_per_number_of_rooms.plot.scatter(x="Rooms", y="price")

// COMMAND ----------

// MAGIC %md
// MAGIC ### BedRooms on different price ranges

// COMMAND ----------

// MAGIC %python
// MAGIC with_price_range_bedrooms = stocksSpark.toPandas().copy().sort_values(by="price")
// MAGIC with_price_range_bedrooms["priceRanges"] = pd.cut(stocksSpark.toPandas()["Bedrooms"], bins=15, labels=False)
// MAGIC with_price_range_bedrooms.head()

// COMMAND ----------

// MAGIC %python
// MAGIC price_per_number_of_bedrooms = with_price_range_bedrooms.groupby("priceRanges").agg({"Bedrooms": "mean", "price": "mean"}).sort_values(by='price', ascending=False).reset_index().drop(columns="priceRanges")
// MAGIC price_per_number_of_bedrooms

// COMMAND ----------

// MAGIC %python
// MAGIC price_per_number_of_bedrooms.plot.scatter(x="Bedrooms", y="price")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Price map by adress

// COMMAND ----------

// MAGIC %python
// MAGIC fig = plt.figure(figsize=(30,8))
// MAGIC gs = fig.add_gridspec(1,2)
// MAGIC ax1 = fig.add_subplot(gs[0, 0])
// MAGIC plt.plot(realEstateDF.toPandas()["longitude"]);
// MAGIC ax2 = fig.add_subplot(gs[0, 1])
// MAGIC plt.plot(realEstateDF.toPandas()["latitude"]);

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF.toPandas().plot(x="longitude", y="latitude", kind="scatter", c="red", colormap="YlOrRd")

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF.show(3)

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF.toPandas().plot(kind="scatter", x="longitude", y="latitude",
// MAGIC     s=realEstateDF.toPandas()['population']/100, label="population",
// MAGIC     c="median_house_value", cmap=plt.get_cmap("jet"),
// MAGIC     colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC )
// MAGIC plt.legend()
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF.toPandas().plot(kind="scatter", x="longitude", y="latitude",
// MAGIC     s=realEstateDF.toPandas()['total_rooms']/100, label="total_rooms",
// MAGIC     c="total_rooms", cmap=plt.get_cmap("jet"),
// MAGIC     colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC )
// MAGIC plt.legend()
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF.toPandas().plot(kind="scatter", x="longitude", y="latitude",
// MAGIC     s=realEstateDF.toPandas()['total_bedrooms']/50, label="total bedrooms",
// MAGIC     c="total_bedrooms", cmap=plt.get_cmap("jet"),
// MAGIC     colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC )
// MAGIC plt.legend()
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF.toPandas().plot(kind="scatter", x="longitude", y="latitude",
// MAGIC     s=realEstateDF.toPandas()['median_income'], label="median_income",
// MAGIC     c="median_income", cmap=plt.get_cmap("jet"),
// MAGIC     colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC )
// MAGIC plt.legend()
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Price map

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.show(1)

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.toPandas().plot(kind="scatter", x="longitude", y="latitude",
// MAGIC     s=stocksSpark.toPandas()['price']/10000, label="price",
// MAGIC     c="price", cmap=plt.get_cmap("jet"),
// MAGIC     colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC )
// MAGIC plt.legend()
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Median household function of location

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF.groupBy("ocean_proximity").mean("median_house_value").withColumnRenamed("avg(median_house_value)", "median_house_value").orderBy(col("median_house_value").desc()).show()

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF.groupBy("ocean_proximity").mean("median_house_value").withColumnRenamed("avg(median_house_value)", "median_house_value").orderBy(col("median_house_value").desc()).toPandas().plot.bar()

// COMMAND ----------

// MAGIC %python
// MAGIC price_per_district.head(3)

// COMMAND ----------

// MAGIC %python
// MAGIC district_number_real_estate = []
// MAGIC district_plt_lat_real_estate = []
// MAGIC district_plt_lon_real_estate = []
// MAGIC price_per_district_real_estate = realEstateDF.toPandas().dropna()
// MAGIC for k in range(0, price_per_district_real_estate.shape[0]):
// MAGIC     district_number_real_estate.append(getDistrict(price_per_district_real_estate["longitude"][k], price_per_district_real_estate["latitude"][k], districS))
// MAGIC price_per_district_real_estate["district_number"] = pd.Series(district_number_real_estate)
// MAGIC price_per_district_real_estate.dropna(subset=["district_number"], inplace=True)
// MAGIC price_per_district_real_estate["district_plt_lat"] = price_per_district_real_estate["district_number"].apply(lambda x: districS[x]["latplt"])
// MAGIC price_per_district_real_estate["district_plt_lon"] = price_per_district_real_estate["district_number"].apply(lambda x: districS[x]["lonplt"])
// MAGIC price_per_district_real_estate.head()

// COMMAND ----------

// MAGIC %python
// MAGIC testdff = pd.merge(price_per_district_real_estate, price_per_district, how='inner', left_on=['district_number'], right_on = ['district_number'])
// MAGIC testdff.head()

// COMMAND ----------

// MAGIC %python
// MAGIC testdff.dropna(inplace=True)
// MAGIC testdff.groupby("ocean_proximity").agg({"price": "mean"})#, "longitude": "mean", "latitude": "mean", "district_plt_lat": "mean", "district_plt_lon": "mean"})
// MAGIC #testdff2.plot(kind="scatter", x="district_plt_lon", y="district_plt_lat",
// MAGIC #    s=mean_price_per_district['price']/200, label="price",
// MAGIC #    c="price", cmap=plt.get_cmap("jet"),
// MAGIC #    colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC #)
// MAGIC #plt.legend()
// MAGIC #plt.title("Mean price per district")
// MAGIC #plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Mounting GOLD

// COMMAND ----------

// MAGIC %python
// MAGIC containerSourceGold = "wasbs://gold@storagegreathouse.blob.core.windows.net/"
// MAGIC containerMountGold = "/mnt/greathouse_gold"

// COMMAND ----------

// MAGIC %python
// MAGIC if (containerMountGold not in list_mounted):
// MAGIC     dbutils.fs.mount(
// MAGIC       source = containerSourceGold,
// MAGIC       mount_point = containerMountGold,
// MAGIC       extra_configs = {"fs.azure.account.key.storagegreathouse.blob.core.windows.net":dbutils.secrets.get(scope = "scope-databricks", key="key1")}
// MAGIC     )
// MAGIC else:
// MAGIC     print("Already mounted")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Saving in  GOld

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Question 1

// COMMAND ----------

// MAGIC %python
// MAGIC stocksDF_mean_price_per_rooms.write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/stocksDF_mean_price_per_rooms.csv", header = 'true')
// MAGIC stocksDF_mean_price_per_rooms.show()

// COMMAND ----------

// MAGIC %python
// MAGIC stocksDF_mean_price_per_bedrooms.write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/stocksDF_mean_price_per_bedrooms.csv", header = 'true')
// MAGIC stocksDF_mean_price_per_bedrooms.show()

// COMMAND ----------

// MAGIC %python
// MAGIC stocksDF_mean_price_per_bedrooms.coalesce(1).write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/address", header = 'true')

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Question 2

// COMMAND ----------

// MAGIC %python
// MAGIC spark.createDataFrame(mean_price_per_district).write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/mean_price_per_district.csv", header = 'true')
// MAGIC spark.createDataFrame(mean_price_per_district).show(3)

// COMMAND ----------

// MAGIC %python
// MAGIC spark.createDataFrame(max_price_per_district).write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/max_price_per_district.csv", header = 'true')
// MAGIC spark.createDataFrame(max_price_per_district).show(3)

// COMMAND ----------

// MAGIC %python
// MAGIC spark.createDataFrame(min_price_per_district).write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/min_price_per_district.csv", header = 'true')
// MAGIC spark.createDataFrame(min_price_per_district).show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Question 3

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.groupBy("Rooms").count().orderBy(col("Rooms").desc()).write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/most_house_per_rooms.csv", header = 'true')

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.groupBy("Bedrooms").count().orderBy(col("Bedrooms").desc()).write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/most_house_per_bedrooms.csv", header = 'true')

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Question 4

// COMMAND ----------

// MAGIC %python
// MAGIC spark.createDataFrame(price_per_number_of_rooms).write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/rooms_per_price_range.csv", header = 'true')
// MAGIC spark.createDataFrame(price_per_number_of_bedrooms).write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/bedrooms_per_price_range.csv", header = 'true')

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Question 5

// COMMAND ----------

// MAGIC %python
// MAGIC #price map from the real estate dataframe ?
// MAGIC stocksSpark.write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/stocks_clean_initial_df.csv", header = 'true')

// COMMAND ----------

// MAGIC %python
// MAGIC stocksSpark.toPandas().plot(kind="scatter", x="longitude", y="latitude",
// MAGIC     s=stocksSpark.toPandas()['price']/10000, label="price",
// MAGIC     c="price", cmap=plt.get_cmap("jet"),
// MAGIC     colorbar=True, alpha=0.4, figsize=(10,7),
// MAGIC )
// MAGIC plt.legend()
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Mean house hold per location

// COMMAND ----------

// MAGIC %python
// MAGIC realEstateDF.groupBy("ocean_proximity").mean("median_house_value").withColumnRenamed("avg(median_house_value)", "median_house_value").orderBy(col("median_house_value").desc()).write.mode("overwrite").format("csv").save("/mnt/greathouse_gold/median_house_value_per_location.csv", header = 'true')

// COMMAND ----------


