from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# initialize Spark session
spark = SparkSession.builder.appName("Q2_DF").getOrCreate()

# relevant paths
PATHS = [
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2010_to_2019.csv",
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2020_to_Present.csv",
]

# read csv files
df1 = spark.read.options(header=True, inferSchema=True).csv(PATHS[0])
df2 = spark.read.options(header=True, inferSchema=True).csv(PATHS[1])

# concatenate the two dataframes
df = df1.union(df2)

# select relevant columns (duplicates are not removed)
df = df.select(["TIME OCC", "Premis Desc"])

# filter out crimes that didn't happen on the street
df = df.filter(df["Premis Desc"] == "STREET")


# function to parse occurrence time into sections
def section(time):
    hour = time // 100
    if 5 <= hour < 12:
        return "Πρωί"
    elif 12 <= hour < 17:
        return "Απόγευμα"
    elif 17 <= hour < 21:
        return "Βράδυ"
    else:
        return "Νύχτα"


# register the UDF
section_udf = F.udf(section, "STRING")

# create a new column for time sections
df = df.withColumn("sections", section_udf(df["TIME OCC"]))

# group by the time sections
df = df.groupBy("sections")

# count the crimes in each section
df = df.count().withColumnRenamed("count", "crime_total")

# sort the results based on the total number of crimes
df = df.sort("crime_total", ascending=False)

# show the results
df.show(df.count(), False)

# stop Spark session
spark.stop()

