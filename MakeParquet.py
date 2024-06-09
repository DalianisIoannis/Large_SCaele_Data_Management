from pyspark.sql import SparkSession

# initialize Spark session
spark = SparkSession.builder.appName("MakeParquet").getOrCreate()

# data directory
DATA_DIR = "hdfs://master:9000/home/user/Assignment/Data/"

# file names
f_names = [
    "Crime_Data_from_2010_to_2019.csv",
    "Crime_Data_from_2020_to_Present.csv",
    "LA_income_2015.csv",
    "LA_police_stations.csv",
    "revgecoding.csv",
]

for f in f_names:
    # file paths
    input_path = DATA_DIR + "csv/" + f
    output_path = DATA_DIR + "parquet/" + f.split(".")[0] + ".parquet"

    # read csv file
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # write parquet file
    df.write.parquet(output_path)

# stop Spark session
spark.stop()

