from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg, count

spark = SparkSession.builder \
    .appName("taxi-stream-console") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2") \
    .getOrCreate()

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("pickup_datetime", StringType(), True), 
    StructField("pickup_lat", DoubleType(), True),
    StructField("pickup_long", DoubleType(), True),
    StructField("dropoff_lat", DoubleType(), True),
    StructField("dropoff_long", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("pickup_borough", StringType(), True)
])


raw_df = (
    spark.readStream
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("subscribe", "taxi_trips") \
    .option("startingOffsets", "latest") \
    .load()
)
json_strings = raw_df.selectExpr("CAST(value AS STRING) as json_str")

parsed = json_strings.select(from_json(col("json_str"), schema).alias("data")).select("data.*") \
            .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))

agg = parsed \
    .withWatermark("pickup_datetime", "2 minutes") \
    .groupBy(window("pickup_datetime", "1 minute"), col("pickup_borough")) \
    .agg(count("*").alias("trips_count"), avg("fare_amount").alias("avg_fare")) \
    .selectExpr("window.start as window_start", "window.end as window_end", "pickup_borough", "trips_count", "avg_fare")

query = agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()