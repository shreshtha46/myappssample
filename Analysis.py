# Python and pyspark modules required

start_pyspark_shell [-e 4] [-c 8] [-w 4] [-m 4] [-n sgu67]

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

stations = stations.withColumn("CountryCode",(stations.ID.substr(1,2)))

#Question 1(a): Total number of unique stations in 2020
stations.select(["ID"]).distinct().count()
daily.select("ID").distinct().count()

#GSN, HCN or CRN

stations.select("ID","GSN_FLAG","HCN/CRN_FLAG").where(F.col("GSN_FLAG") == "GSN").count()
stations.select("ID","GSN_FLAG","HCN/CRN_FLAG").where(F.col("HCN/CRN_FLAG") == "HCN").count()
stations.select("ID","GSN_FLAG","HCN/CRN_FLAG").where(F.col("HCN/CRN_FLAG") == "CRN").count()

#(b):
stations_ineach_country = (
            stations.select(["ID","CountryCode"])
            .groupBy("CountryCode")
            .agg(
                F.count(F.col("ID")).alias("Count")
            )
)

countries = (
            countries
            .join(
                stations_ineach_country.withColumnRenamed("CountryCode", "CODE"),
                on="CODE",
                how="left"
            )
           
)
countries.show(5,False)


stations_ineach_states = (
            stations.select(["ID","STATE"])
            .groupBy("STATE")
            .agg(
                F.count(F.col("STATE")).alias("Count")
            )
)

states = (
            states
            .join(
                stations_ineach_states.withColumnRenamed("STATE", "CODE"),
                on="CODE",
                how="left"
            )
           
)
states.show(5,False)

countries.write.format('csv').save("hdfs:///user/sgu67/outputs/ghcnd/countries")
states.write.format('csv').save("hdfs:///user/sgu67/outputs/ghcnd/states")

#Question2(a)
import math

def geographic_distance(latitude1,longitude1,latitude2,longitude2):
    R = 6373.0
    
    latitude1 = math.radians(latitude1)
    longitude1 = math.radians(longitude1)
    latitude2 = math.radians(latitude2)
    longitude2 = math.radians(longitude2)
    
    dlongitude = longitude2 - longitude1

    dlatitude = latitude2 - latitude1
    
    
    #Haversine formula to calculate distance using longitude latitude
    a = math.sin(dlatitude / 2)**2 + math.cos(latitude1) * math.cos(latitude2) * math.sin(dlongitude / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    
    return distance    
 
stations_crossjoin = (stations.withColumnRenamed('LATITUDE','latitude1').withColumnRenamed('LONGITUDE','longitude1').withColumnRenamed('NAME','Station1')).limit(5).crossJoin((stations.withColumnRenamed('LATITUDE','latitude2').withColumnRenamed('LONGITUDE','longitude2').withColumnRenamed('NAME','Station2')).limit(5))
stations_crossjoin.show(5,False)
                

geographic_distance_udf = F.udf(geographic_distance, DoubleType())                

(
stations_crossjoin.select(
    F.col("Station1"),
    F.col("Station2"),
    geographic_distance_udf(F.col("latitude1"),F.col("longitude1"),F.col("latitude2"),F.col("longitude2")).alias("Distance")
  )
  .where(F.col("Distance") > 0)
  .show(5,False)
)

#(b)

NZ_stations_crossjoin = (stations.withColumnRenamed('LATITUDE','latitude1').withColumnRenamed('LONGITUDE','longitude1').withColumnRenamed('NAME','Station1')).where(F.col("CountryCode") == "NZ").crossJoin((stations.withColumnRenamed('LATITUDE','latitude2').withColumnRenamed('LONGITUDE','longitude2').withColumnRenamed('NAME','Station2')).where(F.col("CountryCode") == "NZ"))
NZ_stations_crossjoin.show(5,False)
                
NZ_stations_distances = (
        NZ_stations_crossjoin.select(
        F.col("Station1"),
        F.col("Station2"),
        geographic_distance_udf(F.col("latitude1"),F.col("longitude1"),F.col("latitude2"),F.col("longitude2")).alias("Distance")
    )
    .sort(F.col("Distance").desc())
    .show()
)

NZ_stations_distances.write.format('csv').save("hdfs:///user/sgu67/outputs/ghcnd/NZ_stations_distances")


#Question 3:(a)
hdfs fsck hdfs:///data/ghcnd/daily/2020.csv.gz
hdfs fsck hdfs:///data/ghcnd/daily/2010.csv.gz

#(b)
start_pyspark_shell [-e 4] [-c 8] [-w 4] [-m 4] [-n sgu67]

# Python and pyspark modules required

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import * 


spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
conf = sc.getConf()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M


schema_daily = StructType([
    StructField('ID', StringType()),
    StructField('DATE', StringType()),
    StructField('ELEMENT', StringType()),
    StructField('VALUE', IntegerType()),
    StructField('MEASUREMENT FLAG', StringType()),
    StructField('QUALITY FLAG', StringType()),
    StructField('SOURCE FLAG', StringType()),
    StructField('OBSERVATION TIME', StringType()),
])

# Load 2015 year daily data (csv format)

daily_2015 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2015.csv.gz")
)
daily_2015.cache()          # cache daily to avoid the time cost of loading it with each job (see job annotations below)
daily_2015.count()  # force daily to be loaded # 34899014



# Load 2020 year daily data (csv format)

daily_2020 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2020.csv.gz")
)
daily_2020.cache()          # cache daily to avoid the time cost of loading it with each job (see job annotations below)
daily_2020.count()  # force daily to be loaded # 5215365


# Load dat from 2015 to 2020

daily_2015_2020 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/20{15,16,17,18,19,20}.csv.gz")
)
daily_2015_2020.cache()
daily_2015_2020.count()   # count - 178918901


#Question 4(a):

start_pyspark_shell [-e 4] [-c 8] [-w 4] [-m 4] [-n sgu67]

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
    StructField("MEASUREMENT FLAG", StringType(), True),
    StructField("QUALITY FLAG", StringType(), True),
    StructField("SOURCE FLAG", StringType(), True),
    StructField("OBSERVATION TIME", StringType(), True),
])


daily_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/*.csv.gz")

)

#4(a): TO count the number of rows in daily
daily_all.count() #2928664523


#(b):
daily_core_elements= daily_all[daily_all.ELEMENT.isin(["PRCP","SNOW","SNWD","TMAX","TMIN"])].groupBy("ELEMENT").agg(F.count("ELEMENT").alias("count))
daily_core_elements.show()


#(c):
daily_TmaxTmin = daily_all.where((F.col('ELEMENT') == 'TMAX') |(F.col('ELEMENT') == 'TMIN'))
cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
Count_data=daily_TmaxTmin.groupBy("ID","DATE").agg(cnt_cond(F.col("ELEMENT")=='TMAX').alias('TMAX_COUNT'),cnt_cond(F.col("ELEMENT")=='TMIN').alias('TMIN_COUNT'))
datawithoutTmax = Count_data.where(F.col('TMIN_COUNT') > F.col('TMAX_COUNT'))
datawithoutTmax.count()

#How many different stations contributed to these observations?
datawithoutTmax.select("ID").distinct().count()

#(d):Filter daily to obtain all observations of TMIN and TMAX for all stations in New Zealand, and save the result to your output directory.
expr = "NZ[A-Z][0-9]"
#daily_TmaxTmin.filter(daily_TmaxTmin["ID"].rlike(expr)).show()
output = daily_TmaxTmin.filter(daily_TmaxTmin["ID"].rlike(expr))
output1=output.withColumnRenamed("MEASUREMENT FLAG", "MEASUREMENT_FLAG").withColumnRenamed("QUALITY FLAG", "QUALITY_FLAG").withColumnRenamed("SOURCE FLAG", "SOURCE_FLAG").withColumnRenamed("OBSERVATION TIME", "OBSERVATION_TIME")
output1.write.csv("NZ_dailyTminTmaxData.csv")

#How many observations are there, and how many years are covered by the observations?
output.count()

output2 = output.withColumn("YEAR",substring('DATE', 1, 4))
output2.show()
min_year = output2.agg({"YEAR": "min"}).collect()[0]
max_year = output2.agg({"YEAR": "max"}).collect()[0]
min_year
max_year
output2.select("YEAR").distinct().count()
output2.select("YEAR").distinct().sort(F.col("YEAR").asc()).show()


#write to ouput directory
output2.write.format('csv').save("hdfs:///user/sgu67/outputs/ghcnd/temperature")
#TO copy output to local
hdfs dfs –copyToLocal hdfs:///user/sgu67/outputs/ghcnd/temperature /home/sgu67




