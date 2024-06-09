from pyspark.sql import SparkSession
import csv
import re
from collections import defaultdict
import geopy.distance

# initialize Spark session
spark = SparkSession.builder.appName("Q4-RDD").getOrCreate()
sc = spark.sparkContext


# function to calculate distance between two coordinates
def get_distance(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km


# relevant paths
PATHS = [
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2010_to_2019.csv",
    "hdfs://master:9000/home/user/Assignment/Data/csv/Crime_Data_from_2020_to_Present.csv",
    "hdfs://master:9000/home/user/Assignment/Data/csv/LA_police_stations.csv",
]

# read csv files and remove header
rdd_old = sc.textFile(PATHS[0])
header = rdd_old.first()
rdd_old = rdd_old.filter(lambda row: row != header)

rdd_new = sc.textFile(PATHS[1])
header = rdd_new.first()
rdd_new = rdd_new.filter(lambda row: row != header)

rdd_stations = sc.textFile(PATHS[2])
header = rdd_stations.first()
rdd_stations = rdd_stations.filter(lambda row: row != header)

rdd_crimes = rdd_old.union(rdd_new)
rdd_crimes = (
    # parse rows
    rdd_crimes.map(lambda row: [x.strip() for x in next(csv.reader([row]))])
    # filter out cases without guns
    .filter(
        lambda row: re.match("^1\\d\\d$", row[16]) is not None
    )  # .filter(lambda row: len(row[16]) == 3 & row[16][0] == "1")
    # map to (AREA CODE, (LAT, LONG))
    .map(lambda row: (int(row[4]), (float(row[-2]), float(row[-1]))))
    # filter out Null Island
    .filter(lambda row: (row[1][0] != 0.0) or (row[1][1] != 0.0))
)

rdd_stations = (
    # parse rows
    rdd_stations.map(lambda row: [x.strip() for x in next(csv.reader([row]))])
    # map to (PREC, (X, Y, DIVISION))
    .map(lambda row: (int(row[-1]), (float(row[0]), float(row[1]), row[3])))
)


# function to switch between join methods
def switch(method):
    if method == "default":
        return rdd_crimes.join(rdd_stations)
    elif method == "broadcast":
        # broadcast the small RDD (rdd_stations)
        stations_dict = dict(rdd_stations.collect())
        dict_broadcast = sc.broadcast(stations_dict)

        def join_function(pair):
            key, crimes_val = pair
            stations_val = dict_broadcast.value[key]

            # check if small RDD contains the key
            if stations_val:
                return (key, (crimes_val, stations_val))
            else:
                return None

        return rdd_crimes.map(join_function).filter(lambda row: row is not None)
    elif method == "repartition":
        # should be adjusted based on resources
        num_partitions = 4

        # partition the RDDs
        partitioned_crimes = rdd_crimes.partitionBy(num_partitions)
        partitioned_stations = rdd_stations.partitionBy(num_partitions)

        def join_function(partition):
            # dictionaries to hold the partitioned data
            dict_crimes = defaultdict(list)
            dict_stations = defaultdict(list)

            for key, val in partition:
                if isinstance(val, tuple) and len(val) == 3:  # data from rdd_stations
                    dict_stations[key].append(val)
                else:  # data from rdd_crimes
                    dict_crimes[key].append(val)

            # perform the join within the partition
            for key in dict_crimes:
                if key in dict_stations:
                    for v1 in dict_crimes[key]:
                        for v2 in dict_stations[key]:
                            yield (key, (v1, v2))

        return partitioned_crimes.union(partitioned_stations).mapPartitions(
            join_function
        )


# perform the join
# CHANGE THE JOIN METHOD HERE!
rdd_join = switch("broadcast")

rdd_join = (
    # map to (DIVISION, (DISTANCE, 1))
    rdd_join.map(
        lambda row: (
            row[1][1][2],
            (get_distance(row[1][0][0], row[1][0][1], row[1][1][1], row[1][1][0]), 1),
        )
    )
    # for each division sum the distances and count the number of crimes
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    # calculate the average distance
    .mapValues(
        lambda x: (x[0] / x[1], x[1])
    )  # .map(lambda row: (row[0], (row[1][0] / row[1][1], row[1][1]))
    # sort the results based on number of crimes
    .sortBy(lambda row: row[1][1], ascending=False)
)

# show the results
for location, stats in rdd_join.collect():
    print(f"{location:15} \t {stats[0]:15} \t {stats[1]:15}")

# stop the Spark session
spark.stop()

