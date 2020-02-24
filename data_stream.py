import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext

from pyspark.sql.functions import col
from pyspark.sql.functions import udf

import datetime

# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", DateType(), True),
    StructField("call_date", DateType(), True),
    StructField("offense_date", DateType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

#https://stackoverflow.com/questions/48305443/typeerror-column-object-is-not-callable-using-withcolumn/50805490
#@udf
def to_yyyymmddhh(timestamp):
    #print(f"TIMESTAMP:{type(timestamp)}")
    #converted_time = datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    return timestamp.strftime("%Y/%m/%d %H")
    #return timestamp

to_yyyymmddhh_udf = udf(to_yyyymmddhh)#, TimestampType())

def run_spark_job(spark):

    # TODO Create Spark Configuration
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "police-department-calls-for-service") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 10) \
        .option("stopGracefullyOnShutdown", "true") \
        .option("failOnDataLoss", "false") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*").alias("service")
    

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table\
                 .select("original_crime_type_name", "disposition", "call_date_time")

    # count the number of original crime type
    #
    #agg_df = service_table\
    agg_df = distinct_table\
        .select("original_crime_type_name", "disposition", "call_date_time")\
        .withColumn('call_date_hour', to_yyyymmddhh_udf(service_table.call_date_time))\
        .withWatermark("call_date_time", '60 minutes')\
        .groupBy("original_crime_type_name", "disposition", 'call_date_hour')\
        .count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    #query = agg_df \
    #query = service_table \
    """
    query = distinct_table \
        .writeStream \
        .format("console") \
        .queryName("Micro Batch") \
        .trigger(processingTime="20 seconds") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()
    query.awaitTermination()
    
    query = agg_df \
        .writeStream \
        .trigger(processingTime="30 seconds") \
        .outputMode('Update') \
        .format('console') \
        .option("truncate", "false") \
        .start()
    """
    # https://knowledge.udacity.com/questions/72289
    #.outputMode('Update')
    """"
    query = agg_df \
        .writeStream \
        .format("console") \
        .queryName("Micro Batch") \
        .trigger(processingTime="60 seconds") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .outputMode('Update') \
        .start()
    """
    # TODO attach a ProgressReporter
    #query.awaitTermination()

    

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True).alias("radio_code")
    radio_code_df.printSchema()


    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    #join_query = distinct_table\
    #    .join(radio_code_df, distinct_table.disposition == distinct_table.disposition, 'inner')\
    # Confirmed to work
    """
    join_query = distinct_table\
        .join(radio_code_df, "disposition", 'left_outer')\
        .writeStream \
        .format("console") \
        .queryName("Join Query") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .outputMode('append') \
        .start()
    """
    # Confirmed to work
    

    #join_distinct_query = distinct_table\
    #    .join(radio_code_df, "disposition", 'left_outer')

    join_query = distinct_table\
        .join(radio_code_df, "disposition", 'left_outer')\
        .select("original_crime_type_name", "disposition","description", "call_date_time")\
        .withColumn('call_date_hour', to_yyyymmddhh_udf(service_table.call_date_time))\
        .withWatermark("call_date_time", '60 minutes')\
        .groupBy("original_crime_type_name", "disposition","description",'call_date_hour')\
        .count()\
        .writeStream \
        .format("console") \
        .queryName("Join Query") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .outputMode('update') \
        .start()

    join_query.awaitTermination()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    """
    sc = SparkContext(appName="KafkaSparkStructuredStreaming")
    ssc = StreamingContext(sc, 1)
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config(conf=ssc)\
        .getOrCreate()
    """
    logger.info("Spark started")
    spark.sparkContext.setLogLevel("WARN")
    logging.getLogger("log4j.logger.org.apache.spark.sql.execution.streaming.MicroBatchExecution")\
        .setLevel(logging.INFO)

    run_spark_job(spark)
    #run_spark_job(ssc)

    spark.stop()