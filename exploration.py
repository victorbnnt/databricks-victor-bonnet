# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting silver

# COMMAND ----------

containerSourceSilver = "wasbs://silver@storagegreathouse.blob.core.windows.net/"
containerMountSilver = "/mnt/greathouse_silver"
list_mounted = list(map(lambda x: x.mountPoint, dbutils.fs.mounts()))

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
# MAGIC # Loading the dataframes

# COMMAND ----------

stocksDF = spark.read \
    .option("delimiter", ",") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_silver/stocks_final.csv")
print(stocksDF.count())
print(stocksDF.dropDuplicates().count())
stocksDF.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## outliers

# COMMAND ----------

stocksDF.select("price").describe().show()

# COMMAND ----------

stocksDF.toPandas().shape
stocksDF.toPandas().boxplot(column="price")

# COMMAND ----------

stocksDF.toPandas()[stocksDF.toPandas()["price"] < (stocksDF.toPandas()["price"].mean() + 3*stocksDF.toPandas()["price"].std())].shape
stocksDF.toPandas()[stocksDF.toPandas()["price"] < (stocksDF.toPandas()["price"].mean() + 3*stocksDF.toPandas()["price"].std())][stocksDF.toPandas()["price"] > (stocksDF.toPandas()["price"].mean() - 3*stocksDF.toPandas()["price"].std())].boxplot(column="price")

# COMMAND ----------

stocksDF_new = stocksDF.toPandas()[stocksDF.toPandas()["price"] < (stocksDF.toPandas()["price"].mean() + 3*stocksDF.toPandas()["price"].std())][stocksDF.toPandas()["price"] > (stocksDF.toPandas()["price"].mean() - 3*stocksDF.toPandas()["price"].std())]
stocksDF_new.shape

# COMMAND ----------

stocksDF_new[stocksDF_new["Rooms"] < (stocksDF_new["Rooms"].mean() + 3*stocksDF_new["Rooms"].std())][stocksDF_new["Rooms"] > (stocksDF_new["Rooms"].mean() - 3*stocksDF_new["Rooms"].std())].boxplot(column="Rooms")
stocksDF_new[stocksDF_new["Rooms"] < (stocksDF_new["Rooms"].mean() + 3*stocksDF_new["Rooms"].std())][stocksDF_new["Rooms"] > (stocksDF_new["Rooms"].mean() - 3*stocksDF_new["Rooms"].std())].shape

# COMMAND ----------

stocksDF_new_v2 = stocksDF_new[stocksDF_new["Rooms"] < (stocksDF_new["Rooms"].mean() + 3*stocksDF_new["Rooms"].std())][stocksDF_new["Rooms"] > (stocksDF_new["Rooms"].mean() - 3*stocksDF_new["Rooms"].std())]

# COMMAND ----------

realEstateDF = spark.read \
    .option("delimiter", ",") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/mnt/greathouse_silver/real_estate_final.csv")
print(realEstateDF.count())
print(realEstateDF.dropDuplicates().count())
realEstateDF.show(3)

# COMMAND ----------

from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import math

# COMMAND ----------

# MAGIC %md
# MAGIC # Question 1
# MAGIC ## Average price per number of rooms on stocks dataframe

# COMMAND ----------

# cleaning stocks df + type conversion
stocksSpark = spark.createDataFrame(stocksDF_new_v2)
stocksSpark.printSchema()

# COMMAND ----------

stocksSpark.head()

# COMMAND ----------

stocksDF_mean_price_per_rooms = stocksSpark.groupBy("Rooms").mean("price").withColumnRenamed("avg(price)", "mean_price").orderBy(col("Rooms").asc())
stocksDF_mean_price_per_rooms.show()

# COMMAND ----------

plt.scatter(stocksDF_mean_price_per_rooms.toPandas()["mean_price"], stocksDF_mean_price_per_rooms.toPandas()["Rooms"]);

# COMMAND ----------

stocksDF_mean_price_per_bedrooms = stocksSpark.groupBy("Bedrooms").mean("price").withColumnRenamed("avg(price)", "mean_price").orderBy(col("Bedrooms").asc())
stocksDF_mean_price_per_bedrooms.show()

# COMMAND ----------

plt.scatter(stocksDF_mean_price_per_bedrooms.toPandas()["mean_price"], stocksDF_mean_price_per_bedrooms.toPandas()["Bedrooms"]);

# COMMAND ----------

# MAGIC %md
# MAGIC # Analyse on district dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating districts by coordinate ranges

# COMMAND ----------

minLat = math.floor(realEstateDF.toPandas()["latitude"].min())
maxLat = math.ceil(realEstateDF.toPandas()["latitude"].max())
minLon = math.floor(realEstateDF.toPandas()["longitude"].min())
maxLon = math.ceil(realEstateDF.toPandas()["longitude"].max())
#
Dif = 30
#
rangeLat = list(np.linspace(minLat, maxLat+1, Dif))  
#print(rangeLat)
latitudeRanges = []
#
#
for lat in range(0, len(rangeLat)):
    try:
        latitudeRanges.append([rangeLat[lat], rangeLat[lat+1]])
    except:
        pass
#
#print(latitudeRanges)
#
rangeLon = list(np.linspace(minLon-1, maxLon, Dif))
longitudeRanges = []
#
for lon in range(0, len(rangeLon), 1):
    try:
        longitudeRanges.append([rangeLon[lon], rangeLon[lon+1]])
    except:
        pass
print(longitudeRanges)

# COMMAND ----------

districS = dict()
districtcounter = 0
for latbloc in latitudeRanges:
    for lonbloc in longitudeRanges:
        dis = {"lat": latbloc, "lon": lonbloc, "latplt": (latbloc[1]+latbloc[0])/2, "lonplt": (lonbloc[1]+lonbloc[0])/2}
        districS[str(districtcounter+1)] = dis
        districtcounter+=1
#districS

# COMMAND ----------

def getDistrict(lon, lat, dictref):
    for k, v in dictref.items():
        if (lon > v["lon"][0] and lon <= v["lon"][1]) and (lat > v["lat"][0] and lat <= v["lat"][1]):
            return k

# COMMAND ----------

stocksSpark.show(3)

# COMMAND ----------

district_number = []
district_plt_lat = []
district_plt_lon = []
dfTemp = stocksSpark.toPandas()
for k in range(0, dfTemp.shape[0]):
    district_number.append(getDistrict(dfTemp["longitude"][k], dfTemp["latitude"][k], districS))
dfTemp["district_number"] = pd.Series(district_number)
dfTemp["district_plt_lat"] = dfTemp["district_number"].apply(lambda x: districS[x]["latplt"])
dfTemp["district_plt_lon"] = dfTemp["district_number"].apply(lambda x: districS[x]["lonplt"])
dfTemp.head()

# COMMAND ----------

testDFR = dfTemp.groupby("district_number").agg({"price": "mean", "longitude": "mean", "latitude": "mean", "district_plt_lat": "mean", "district_plt_lon": "mean"})
testDFR.plot(kind="scatter", x="district_plt_lon", y="district_plt_lat",
    s=testDFR['price']/200, label="price",
    c="price", cmap=plt.get_cmap("jet"),
    colorbar=True, alpha=0.4, figsize=(10,7),
)
plt.legend()
plt.title("Mean price per district")
plt.show()

# COMMAND ----------

testDFR = dfTemp.groupby("district_number").agg({"price": "max", "longitude": "mean", "latitude": "mean", "district_plt_lat": "mean", "district_plt_lon": "mean"})
testDFR.plot(kind="scatter", x="district_plt_lon", y="district_plt_lat",
    s=testDFR['price']/1000, label="price",
    c="price", cmap=plt.get_cmap("jet"),
    colorbar=True, alpha=0.4, figsize=(10,7),
)
plt.legend()
plt.title("Max price per district")
plt.show()

# COMMAND ----------

testDFR = dfTemp.groupby("district_number").agg({"price": "min", "longitude": "mean", "latitude": "mean", "district_plt_lat": "mean", "district_plt_lon": "mean"})
testDFR.plot(kind="scatter", x="district_plt_lon", y="district_plt_lat",
    s=testDFR['price']/1000, label="price",
    c="price", cmap=plt.get_cmap("jet"),
    colorbar=True, alpha=0.4, figsize=(10,7),
)
plt.legend()
plt.title("Min price per district")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Kind of houses most important in the market

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ROOMS

# COMMAND ----------

stocksSpark.groupBy("Rooms").count().orderBy(col("Rooms").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### BEDROOMS

# COMMAND ----------

stocksSpark.groupBy("Bedrooms").count().orderBy(col("Bedrooms").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rooms on different price ranges

# COMMAND ----------

stocksSpark.toPandas().head(3)

# COMMAND ----------

with_price_range = stocksSpark.toPandas().copy()
with_price_range["priceRanges"] = pd.cut(stocksSpark.toPandas()["Rooms"], bins=15, labels=False)
with_price_range.head()

# COMMAND ----------

price_per_number_of_rooms = with_price_range.groupby("priceRanges").agg({"Rooms": "mean", "price": "mean"}).sort_values(by='price', ascending=False).reset_index().drop(columns="priceRanges")
price_per_number_of_rooms

# COMMAND ----------

price_per_number_of_rooms.plot.scatter(x="Rooms", y="price")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BedRooms on different price ranges

# COMMAND ----------

with_price_range_bedrooms = stocksSpark.toPandas().copy()
with_price_range_bedrooms["priceRanges"] = pd.cut(stocksSpark.toPandas()["Bedrooms"], bins=15, labels=False)
with_price_range_bedrooms.head()

# COMMAND ----------

price_per_number_of_bedrooms = with_price_range_bedrooms.groupby("priceRanges").agg({"Bedrooms": "mean", "price": "mean"}).sort_values(by='price', ascending=False).reset_index().drop(columns="priceRanges")
price_per_number_of_bedrooms

# COMMAND ----------

price_per_number_of_bedrooms.plot.scatter(x="Bedrooms", y="price")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Price map by adress

# COMMAND ----------

fig = plt.figure(figsize=(30,8))
gs = fig.add_gridspec(1,2)
ax1 = fig.add_subplot(gs[0, 0])
plt.plot(realEstateDF.toPandas()["longitude"]);
ax2 = fig.add_subplot(gs[0, 1])
plt.plot(realEstateDF.toPandas()["latitude"]);

# COMMAND ----------

realEstateDF.toPandas().plot(x="longitude", y="latitude", kind="scatter", c="red", colormap="YlOrRd")

# COMMAND ----------

realEstateDF.show(3)

# COMMAND ----------

realEstateDF.toPandas().plot(kind="scatter", x="longitude", y="latitude",
    s=realEstateDF.toPandas()['population']/100, label="population",
    c="median_house_value", cmap=plt.get_cmap("jet"),
    colorbar=True, alpha=0.4, figsize=(10,7),
)
plt.legend()
plt.show()

# COMMAND ----------

realEstateDF.toPandas().plot(kind="scatter", x="longitude", y="latitude",
    s=realEstateDF.toPandas()['total_rooms']/100, label="total_rooms",
    c="total_rooms", cmap=plt.get_cmap("jet"),
    colorbar=True, alpha=0.4, figsize=(10,7),
)
plt.legend()
plt.show()

# COMMAND ----------

realEstateDF.toPandas().plot(kind="scatter", x="longitude", y="latitude",
    s=realEstateDF.toPandas()['total_bedrooms']/50, label="total bedrooms",
    c="total_bedrooms", cmap=plt.get_cmap("jet"),
    colorbar=True, alpha=0.4, figsize=(10,7),
)
plt.legend()
plt.show()

# COMMAND ----------

realEstateDF.toPandas().plot(kind="scatter", x="longitude", y="latitude",
    s=realEstateDF.toPandas()['median_income'], label="median_income",
    c="median_income", cmap=plt.get_cmap("jet"),
    colorbar=True, alpha=0.4, figsize=(10,7),
)
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Median household function of location

# COMMAND ----------

realEstateDF.groupBy("ocean_proximity").mean("median_house_value").withColumnRenamed("avg(median_house_value)", "median_house_value").orderBy(col("median_house_value").desc()).show()

# COMMAND ----------

realEstateDF.groupBy("ocean_proximity").mean("median_house_value").withColumnRenamed("avg(median_house_value)", "median_house_value").orderBy(col("median_house_value").desc()).toPandas().plot.bar()

# COMMAND ----------


