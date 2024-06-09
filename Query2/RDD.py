from pyspark.sql import SparkSession
import csv

# initialize Spark session
spark = SparkSession.builder.appName("Q2_RDD").getOrCreate()
sc = spark.sparkContext

# relevant paths
PATHS = [
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2010_to_2019.csv",
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2020_to_Present.csv",
]

# read csv files
rdd1 = sc.textFile(PATHS[0])
rdd2 = sc.textFile(PATHS[1])

# remove headers
header1 = rdd1.first()
header2 = rdd2.first()
rdd1 = rdd1.filter(lambda row: row != header1)
rdd2 = rdd2.filter(lambda row: row != header2)

# concatenate the two RDDs
rdd = rdd1.union(rdd2)


# function to parse csv file lines
def parse_row(row):
    return next(csv.reader([row]))


# function to parse occurrence time into sections
def section(time):
    hour = int(time) // 100

    if hour >= 5 and hour < 12:
        return "Πρωί"
    elif hour >= 12 and hour < 17:
        return "Απόγευμα"
    elif hour >= 17 and hour < 21:
        return "Βράδυ"
    else:
        return "Νύχτα"


rdd = (
    # parse rows
    rdd.map(lambda row: parse_row(row))
    # keep only the crimes that happened on the street
    .filter(lambda row: row[15] == "STREET")
    # map each row to a (section, 1) tuple
    .map(lambda row: (section(row[3]), 1))
    # add up the occurrences of each section
    .reduceByKey(lambda a, b: a + b)
    # sort by crime count
    .sortBy(lambda x: x[1], ascending=False)
)

# print the results
results = rdd.collect()
print(results)

# Stop the Spark session
spark.stop()

