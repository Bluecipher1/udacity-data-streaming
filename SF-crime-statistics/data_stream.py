import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.udacity.crimestatistics") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger",200) \
        .option("minRatePerPartition", 1000) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # extract the correct column from the kafka input resources
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"),
                psf.col('original_crime_type_name'), 
                psf.col('disposition'))
    
    # count the number of original crime type    
    agg_df = distinct_table\
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(
            psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),
            psf.col('original_crime_type_name')
        )\
        .count()\
        .sort('count', ascending=False)
    
    query = agg_df \
        .writeStream \
        .outputMode('Complete') \
        .format('console') \
        .option("truncate", "false") \
        .trigger(processingTime='10 seconds') \
        .start()


    query.awaitTermination()

    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join on disposition column
    join_query = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition)


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .config("spark.sql.shuffle.partitions", 1) \
        .config("spark.default.parallelism", 1) \
        .config("spark.driver.memory","2g") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
