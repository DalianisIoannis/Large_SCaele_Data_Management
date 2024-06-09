from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# initialize Spark session
spark = SparkSession.builder.appName("Q1_DF_csv").getOrCreate()

# data file paths
PATHS = [
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2010_to_2019.csv",
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2020_to_Present.csv",
]

# read csv files
df_old = spark.read.options(header=True, inferSchema=True).csv(PATHS[0])
df_new = spark.read.options(header=True, inferSchema=True).csv(PATHS[1])

# concatenate the two dataframes
df = df_old.union(df_new)

# select relevant columns (duplicates are not removed)
df = df.select("DATE OCC")

# extract date from timestamp
date = F.split(df["DATE OCC"], " ").getItem(0)

# split date on /
date = F.split(date, "/")

# add month column
df = df.withColumn("month", date.getItem(0).cast("int"))

# add year column
df = df.withColumn("year", date.getItem(2).cast("int"))

# drop unnecessary column
df = df.drop("DATE OCC")

# group by pairs of year and month
df = df.groupBy(["year", "month"])

# count records for each pair
df = df.count().withColumnRenamed("count", "crime_total")

# sort by year (ascending) and crime_total (descending)
df = df.sort(["year", "crime_total"], ascending=[True, False])

# for each year, rank months based on crime_total
w = Window.partitionBy("year").orderBy(df["crime_total"].desc())

# keep only top 3 months for each year
df = df.withColumn("rank", F.row_number().over(w)).filter("rank <= 3")

# display results
df.show(df.count(), truncate=False)

# stop Spark session
spark.stop()

