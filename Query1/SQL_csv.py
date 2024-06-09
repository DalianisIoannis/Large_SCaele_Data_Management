from pyspark.sql import SparkSession

# initialize Spark session
spark = SparkSession.builder.appName("Q1_SQL_csv").getOrCreate()

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

# extract year and month from date
df = spark.sql(
    """
    SELECT
        CAST(SPLIT(SPLIT(`DATE OCC`, ' ')[0], '/')[0] AS INT) AS month,
        CAST(SPLIT(SPLIT(`DATE OCC`, ' ')[0], '/')[2] AS INT) AS year
    FROM {dataset}
    """,
    dataset=df,
)

# group by year and month and count the number of crimes
df = spark.sql(
    """
    SELECT year, month, COUNT(*) AS crime_total
    FROM {dataset}
    GROUP BY year, month
    ORDER BY year ASC, crime_total DESC
    """,
    dataset=df,
)

# rank the months in each year by the number of crimes
df = spark.sql(
    """
    SELECT
        year,
        month,
        crime_total,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
    FROM {dataset}
    """,
    dataset=df,
)

# filter the top 3 months
df = spark.sql(
    """
    SELECT *
    FROM {dataset}
    WHERE ranking <= 3
    """,
    dataset=df,
)

# display results
df.show(df.count(), False)

# stop Spark session
spark.stop()

