from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import geopy.distance

# initialize Spark session
spark = SparkSession.builder.appName("Q4-DF").getOrCreate()


# function to calculate distance between two coordinates
def get_distance(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km


# register the user defined function
get_distance_udf = F.udf(get_distance, "FLOAT")

# relevant paths
PATHS = [
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2010_to_2019.csv",
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2020_to_Present.csv",
    "hdfs://master:9000/home/user/Assignment/Data/csv/LA_police_stations.csv",
]

# read the csv files
df_old = spark.read.options(header=True, inferSchema=True).csv(PATHS[0])
df_new = spark.read.options(header=True, inferSchema=True).csv(PATHS[1])
df_stations = spark.read.options(header=True, inferSchema=True).csv(PATHS[2])

df_crimes = df_old.union(df_new)

# filter out crimes without guns and Null Island
df_crimes = df_crimes.filter(
    (df_crimes["Weapon Used Cd"].rlike("^1\\d\\d$"))
    & ((df_crimes["LAT"] != 0.0) | (df_crimes["LON"] != 0.0))
)

# join the two dataframes
df = df_crimes.join(df_stations, df_crimes["AREA "] == df_stations["PREC"])

# calculate the distance between the station and the crime
df = df.withColumn("Distance", get_distance_udf(df["LAT"], df["LON"], df["Y"], df["X"]))

# group by division and calculate the average distance and total number of incidents
df = df.groupBy("DIVISION").agg(
    F.avg("Distance").alias("Average Distance"),
    F.count("Distance").alias("Crime Total"),
)

# sort the number of incidents in descending order
df = df.orderBy(df["Crime Total"].desc())

# show the results
df.show(df.count(), False)

# stop Spark session
spark.stop()

