# Python and pyspark modules required

start_pyspark_shell [-e 2] [-c 1] [-w 1] [-m 1] 

import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import * 

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M

# Load and create schema

schema_daily = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", DoubleType(), True),
    StructField("MEASUREMENT_FLAG", StringType(), True),
    StructField("QUALITY_FLAG", StringType(), True),
    StructField("SOURCE_FLAG", StringType(), True),
    StructField("OBSERVATION_FLAG", StringType(), True),
])
daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2020.csv.gz")
    .limit(1000)
    .repartition(partitions)
)


schema_stations = StructType([
    StructField("ID", StringType()),
    StructField("LATITUDE", DoubleType()),
    StructField("LONGITUDE", DoubleType()),
    StructField("ELEVATION", DoubleType()),
    StructField("STATE", StringType()),
    StructField("NAME", StringType()),
    StructField("GSN_FLAG", StringType()),
    StructField("HCN/CRN_FLAG", StringType()),
    StructField("WMO_ID", StringType()),
])

#stations = stations.withColumnRenamed("WMO ID","WMO_ID")

stations_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/stations")
)

stations = stations_text_only.select(
    F.substring(F.col("VALUE"), 1,  (11 - 1) + 1).alias("ID").cast(schema_stations["ID"].dataType),
    F.substring(F.col("VALUE"), 13, (20 - 13) + 1).alias("LATITUDE").cast(schema_stations["LATITUDE"].dataType),
    F.substring(F.col("VALUE"), 22, (30 - 22) + 1).alias("LONGITUDE").cast(schema_stations["LONGITUDE"].dataType),
    F.substring(F.col("VALUE"), 32, (37 - 32) + 1).alias("ELEVATION").cast(schema_stations["ELEVATION"].dataType),
    F.substring(F.col("VALUE"), 39, (40 - 39) + 1).alias("STATE").cast(schema_stations["STATE"].dataType),
    F.substring(F.col("VALUE"), 42, (71 - 42) + 1).alias("NAME").cast(schema_stations["NAME"].dataType),
    F.substring(F.col("VALUE"), 73, (75 - 73) + 1).alias("GSN_FLAG").cast(schema_stations["GSN_FLAG"].dataType),
    F.substring(F.col("VALUE"), 77, (79 - 77) + 1).alias("HCN/CRN_FLAG").cast(schema_stations["HCN/CRN_FLAG"].dataType),
    F.substring(F.col("VALUE"), 81, (85 - 81) + 1).alias("WMO_ID").cast(schema_stations["WMO_ID"].dataType)
)


schema_states = StructType([
  StructField("CODE", StringType(), True),
  StructField("NAME", StringType(), True),
])
states_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/states")
)
states = states_text_only.select(
    F.substring(F.col("VALUE"), 1,  (2 - 1) + 1).alias("CODE").cast(schema_states["CODE"].dataType),
    F.substring(F.col("VALUE"), 4, (50 - 4) + 1).alias("NAME").cast(schema_states["NAME"].dataType),
)


schema_countries = StructType([
  StructField("CODE", StringType(), True),
  StructField("NAME", StringType(), True),
])
countries_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/countries")
)
countries = countries_text_only.select(
    F.substring(F.col("VALUE"), 1,  (2 - 1) + 1).alias("CODE").cast(schema_countries["CODE"].dataType),
    F.substring(F.col("VALUE"), 4, (50 - 4) + 1).alias("NAME").cast(schema_countries["NAME"].dataType),
)


schema_inventory = StructType([
  StructField("ID", StringType(), True),
  StructField("LATITUDE", DoubleType(), True),
  StructField("LONGITUDE", DoubleType(), True),
  StructField("ELEMENT", StringType(), True),
  StructField("FIRST_YEAR", StringType(), True),
  StructField("LAST_YEAR", StringType(), True),
])
inventory_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/inventory")
)
inventory = inventory_text_only.select(
    F.substring(F.col("VALUE"), 1,  (11 - 1) + 1).alias("ID").cast(schema_inventory["ID"].dataType),
    F.substring(F.col("VALUE"), 13, (20 - 13) + 1).alias("LATITUDE").cast(schema_inventory["LATITUDE"].dataType),
    F.substring(F.col("VALUE"), 22,  (30 - 22) + 1).alias("LONGITUDE").cast(schema_inventory["LONGITUDE"].dataType),
    F.substring(F.col("VALUE"), 32, (35 - 32) + 1).alias("ELEMENT").cast(schema_inventory["ELEMENT"].dataType),
    F.substring(F.col("VALUE"), 37, (40 - 37) + 1).alias("FIRST_YEAR").cast(schema_inventory["FIRST_YEAR"].dataType),
    F.substring(F.col("VALUE"), 42, (45 - 42) + 1).alias("LAST_YEAR").cast(schema_inventory["LAST_YEAR"].dataType),
)


# SQL

#Question 2c:
stations.count()
states.count()
countries.count()
inventory.count()
stations.filter(stations.WMO_ID =="     ").count()

#To create an ouput directory
hdfs dfs -mkdir hdfs:///user/sgu67/outputs/ghcnd


#Question 3: (a)-
stations = stations.withColumn("CountryCode",(stations.ID.substr(1,2)))
station.show(5,False)
#(b)-
stations_join_countries = stations.join(countries.withColumnRenamed("CODE","CountryCode"), on="CountryCode",how='left')
stations_join_countries.show(5,False)
#(c)-
stations_join_states = stations.where(stations.CountryCode == "US").join(states.withColumnRenamed("CODE","STATE"),on='STATE',how='left')
stations_join_states.show(5,False)
#(d)-
elements_inventory= (
            inventory.select(["ID", "LATITUDE","LONGITUDE","ELEMENT","FIRST_YEAR","LAST_YEAR"])
            .groupBy("ID")
            .agg(
              F.collect_set("ELEMENT").alias('Element_Stations'),
              F.min(F.col("FIRST_YEAR")).alias("Oldest_Year"),
              F.max(F.col("LAST_YEAR")).alias("Latest_Year"),
              F.count(F.col("ELEMENT")).alias("COUNT")
             )
)
elements_inventory.show(5,False)

core_elements = inventory[inventory.ELEMENT.isin(["PRCP","SNOW","SNWD","TMAX","TMIN"])].groupBy("ID").agg(F.count("ELEMENT").alias("CoreElements"))
core_elements.show(5,False)

other_elements =inventory[inventory.ELEMENT.isin(["PRCP","SNOW","SNWD","TMAX","TMIN"])== "False"].groupBy("ID").agg(F.count("ELEMENT").alias("OtherElements"))
other_elements.show(5,False)

core_elements.select("ID","CoreElements").where("CoreElements == 5").show(5,False)

temperature = inventory[inventory.ELEMENT.isin(["TMAX","TMIN"])].groupBy("ID").agg(F.count("ELEMENT").alias("TemperatureElements"))
temperature.show(5,False)

#(E)
stations_join_elements_inventory = stations.join(elements_inventory, on='ID',how='left')
stations_join_elements_inventory.show(5,False)

stations_join_elements_inventory.write.format('parquet').save("hdfs:///user/sgu67/outputs/ghcnd/stations_join_elements_inventory")
#Writing the data into hdfs
countries.write.format('csv').save("hdfs:///user/dpp29/outputs/ghcnd/countries")

#(F)
daily_join_stationselementsinventory = daily.join(stations_join_elements_inventory, on='ID',how='left')
daily_join_stationselementsinventory.show(5,False)




#Challenges
daily.QUALITY_FLAG.isin(["D",G",I",K",L",M",N",O",R",S",TW",,X,Z"])