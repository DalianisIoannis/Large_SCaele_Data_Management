from pyspark.sql import SparkSession
import re
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, regexp_replace
import pyspark.sql.functions as F

# initialize Spark session
spark = SparkSession.builder.appName("Q3_DF_csv").getOrCreate()

# relevant paths
PATHS = [
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2010_to_2019.csv",
    "hdfs://master:9000/home/user/Assignment/Data/csv/LA_income_2015.csv",
    "hdfs://master:9000/home/user/Assignment/Data/csv/revgecoding.csv"
]

#########################################################
# read the Crime Data. Keep the Victims with value. Keep only year 2015

df = spark.read.options(header=True, inferSchema=True).csv(PATHS[0])  # read csv files into dataframes

# Filter the DataFrame to remove rows where "Vict Descent" is null or "X" = unknown
# df = df.filter((col("Vict Descent").isNotNull()) & (col("Vict Descent") != "X"))
# Filter the DataFrame to remove rows where "Vict Descent" is null
df = df.filter((col("Vict Descent").isNotNull()))

date = F.split(df["DATE OCC"], " ").getItem(0)  # extract date from timestamp
date = F.split(date, "/")  # split date and add year column
df = df.withColumn("year", date.getItem(2).cast("int"))
df = df.drop("DATE OCC")  # DATE OCC column is no longer needed
df = df.filter(df["year"] == 2015)  # keep only records with year 2015

# print(f"Total number of rows for crimes in 2015: {df.count()}")

# unecessary columns
columns_to_drop = ["Crm Cd 3", "Crm Cd 4", "Crm Cd 1", "Crm Cd 2", "Status", "Weapon Used Cd",
                    "Part 1-2", "Date Rptd", "Mocodes", "Vict Age", "Premis Cd", "Vict Sex",
                    "Premis Desc", "Crm Cd Desc", "Crm Cd", "Weapon Desc", "Status Desc",
                    "AREA NAME", "AREA ", "LOCATION", "TIME OCC", "Cross Street", "DR_NO"]
df = df.drop(*columns_to_drop)

#########################################################
# find zip codes with highest and lowest incomes for LA communities

df_income = spark.read.options(header=True, inferSchema=True).csv(PATHS[1])

# Clean the "Estimated Median Income" column by removing the "$" and "," and converting to float
df_income = df_income.withColumn("Estimated Median Income",
                                    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("float"))

# Filter the DataFrame to only keep the values in "Community" that start with "Los Angeles"
df_income = df_income.filter(col("Community").startswith("Los Angeles"))

# df_income.select("Community").distinct().show(df.count(), False)

# Get the 3 rows with lowest and 3 with highest "Estimated Median Income"
df_lowest = df_income.orderBy(col("Estimated Median Income").asc()).limit(3)  # 4
df_highest = df_income.orderBy(col("Estimated Median Income").desc()).limit(3)  # 8

# df_lowest.show(truncate=False)
# df_highest.show(truncate=False)

#########################################################
# read revgecoding data and keep only one Zip Code for every coordinate
# join df_geo with low and high income to keep only the data that correspond to
# the communities we are interested

df_geo = spark.read.options(header=True, inferSchema=True).csv(PATHS[2])
df_geo = df_geo.withColumnRenamed("ZIPcode", "Zip Code")  # Standardize column name
df_geo = df_geo.dropDuplicates(["LAT", "LON"])  # Remove duplicates based on LAT and LON

# UDF to extract first Zip Code, for example "90013; 90015:90015" > 90013
def extract_first_zip(zip_code):
    if zip_code:
        match = re.search(r'\d+', zip_code)
        if match:
            return int(match.group(0))
    return None

extract_first_zip_udf = F.udf(extract_first_zip, IntegerType())
df_geo = df_geo.withColumn("Zip Code", extract_first_zip_udf(df_geo["Zip Code"]))

# Default is inner join. An inner join returns only the rows where there
# is a match in both DataFrames based on the specified columns
df_geo_low = df_geo.join(df_lowest, on=['Zip Code'])
df_geo_high = df_geo.join(df_highest, on=['Zip Code'])

##################################################################################################################
# merge crime data with low and high income coordinates
# hints

# # catalyst optimizer choice
# merged_low = df.join(df_geo_low, on=['LAT', 'LON'])
# merged_high = df.join(df_geo_high, on=['LAT', 'LON'])

# # If one of your DataFrames is small enough, use a broadcast join to send
# # it to all nodes. This avoids shuffling large DataFrames across the network
# merged_low = df.join(df_geo_low.hint("broadcast"), on=['LAT', 'LON'])
# merged_high = df.join(df_geo_high.hint("broadcast"), on=['LAT', 'LON'])

# # suggests a sort-merge join. This requires both DataFrames to be sorted
# # on the join key, which can be efficient for large datasets with sorted data
# merged_low = df.join(df_geo_low.hint("merge"), on=['LAT', 'LON'])
# merged_high = df.join(df_geo_high.hint("merge"), on=['LAT', 'LON'])

# # suggests a shuffle hash join, where both DataFrames are shuffled and a hash join is performed
# merged_low = df.join(df_geo_low.hint("shuffle_hash"), on=['LAT', 'LON'])
# merged_high = df.join(df_geo_high.hint("shuffle_hash"), on=['LAT', 'LON'])

# suggests a shuffle-and-replicate nested loop join, where one DataFrame is broadcasted to each
# node and a nested loop join is performed
merged_low = df.join(df_geo_low.hint("shuffle_replicate_nl"), on=['LAT', 'LON'])
merged_high = df.join(df_geo_high.hint("shuffle_replicate_nl"), on=['LAT', 'LON'])

print(merged_low.explain())
print(merged_high.explain())

##################################################################################################################
# group descents, and prepare for display

# Group by 'Vict Descent' and count the total victims by group
crimes_in_low_income = merged_low.groupBy("Vict Descent").count().orderBy(F.desc("count"))
crimes_in_high_income = merged_high.groupBy("Vict Descent").count().orderBy(F.desc("count"))

# Define a UDF to map the descent codes to descriptions
def map_descent(code):
    # map descent codes with categories
    descent_dict = {
        "A": "Other Asian",
        "B": "Black",
        "C": "Chinese",
        "D": "Cambodian",
        "F": "Filipino",
        "G": "Guamanian",
        "H": "Hispanic/Latin/Mexican",
        "I": "American Indian/Alaskan Native",
        "J": "Japanese",
        "K": "Korean",
        "L": "Laotian",
        "O": "Other",
        "P": "Pacific Islander",
        "S": "Samoan",
        "U": "Hawaiian",
        "V": "Vietnamese",
        "W": "White",
        "X": "Unknown",
        "Z": "Asian Indian"
    }
    return descent_dict.get(code, "Other")

map_descent_udf = F.udf(map_descent, StringType())

# Apply the UDF to create a new column with descriptive values
crimes_in_low_income = crimes_in_low_income.withColumn("victim descent", map_descent_udf(df["Vict Descent"]))
crimes_in_high_income = crimes_in_high_income.withColumn("victim descent", map_descent_udf(df["Vict Descent"]))

crimes_in_low_income = crimes_in_low_income.drop("Vict Descent")  # drop Vict Descent
crimes_in_high_income = crimes_in_high_income.drop("Vict Descent")

# rename count column to total victims
crimes_in_low_income = crimes_in_low_income.withColumnRenamed("count", "total victims")
crimes_in_high_income = crimes_in_high_income.withColumnRenamed("count", "total victims")

# Reorder the columns to ensure 'victim descent' appears first
columns_reordered = ["victim descent"] + [col for col in crimes_in_low_income.columns if col != "victim descent"]
crimes_in_low_income = crimes_in_low_income.select(columns_reordered)
crimes_in_high_income = crimes_in_high_income.select(columns_reordered)

crimes_in_low_income.show(truncate=False)
crimes_in_high_income.show(truncate=False)

#########################################################

spark.stop()  # stop Spark session