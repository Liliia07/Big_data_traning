#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import desc, row_number, col, count, sum
import pandas as pd

spark = SparkSession \
            .builder \
            .enableHiveSupport() \
            .appName('Homework_3_1')\
            .getOrCreate()

schema = StructType() \
.add("YEAR",StringType(),True) \
.add("MONTH",IntegerType(),True) \
.add("DAY",StringType(),True) \
.add("DAY_OF_WEEK",IntegerType(),True) \
.add("AIRLINE",StringType(),True) \
.add("FLIGHT_NUMBER",IntegerType(),True) \
.add("TAIL_NUMBER",StringType(),True) \
.add("ORIGIN_AIRPORT",StringType(),True) \
.add("DESTINATION_AIRPORT",StringType(),True) \
.add("SCHEDULED_DEPARTURE",IntegerType(),True) \
.add("DEPARTURE_TIME",IntegerType(),True) \
.add("DEPARTURE_DELAY",IntegerType(),True) \
.add("TAXI_OUT",IntegerType(),True) \
.add("WHEELS_OFF",IntegerType(),True) \
.add("SCHEDULED_TIME",IntegerType(),True) \
.add("ELAPSED_TIME",IntegerType(),True) \
.add("AIR_TIME",IntegerType(),True) \
.add("DISTANCE",IntegerType(),True) \
.add("WHEELS_ON",IntegerType(),True) \
.add("TAXI_IN",IntegerType(),True) \
.add("SCHEDULED_ARRIVAL",IntegerType(),True) \
.add("ARRIVAL_TIME",IntegerType(),True) \
.add("ARRIVAL_DELAY",IntegerType(),True) \
.add("DIVERTED",BooleanType(),True) \
.add("CANCELLED",BooleanType(),True) \
.add("CANCELLATION_REASON",StringType(),True) \
.add("AIR_SYSTEM_DELAY",IntegerType(),True) \
.add("SECURITY_DELAY",IntegerType(),True) \
.add("AIRLINE_DELAY",IntegerType(),True) \
.add("LATE_AIRCRAFT_DELAY",IntegerType(),True) \
.add("WEATHER_DELAY",IntegerType(),True)


df = spark.read.load("gs://bucket-for-hive-bigdata-procamp-b1a78672/2015_Flight_Delays_and_Cancellations/flights.csv",
                     format="csv", schema=schema, header="true")

# WITH CTE AS (
# select MONTH, DESTINATION_AIRPORT, count(*) as NUMBER_OF_VISITS from prod
# GROUP BY MONTH, DESTINATION_AIRPORT
# ),
# window_func
# (select DESTINATION_AIRPORT, NUMBER_OF_VISITS, ROW_NUMBER() OVER(PARTITION BY MONTH ORDER BY NUMBER_OF_VISITS DESC) rn from CTE)
# SELECT DESTINATION_AIRPORT, NUMBER_OF_VISITS FROM window_func
# WHERE rn = 1


count_all_DESTINATION_AIRPORT_per_MONTH = df.groupBy('MONTH','DESTINATION_AIRPORT').agg(count('*').alias('NUMBER_OF_VISITS'))

prerequisite_for_window_function = Window().partitionBy('MONTH').orderBy(desc('NUMBER_OF_VISITS'))

apply_window_function = count_all_DESTINATION_AIRPORT_per_MONTH.withColumn('rn',row_number().over(prerequisite_for_window_function))

most_popular_destination_airport_per_month = apply_window_function.select('MONTH','DESTINATION_AIRPORT','NUMBER_OF_VISITS').filter(col('rn') == 1).orderBy('MONTH')

most_popular_destination_airport_per_month.write.option("delimiter", "\t").mode("overwrite").option("header", "true").format("csv").save("gs://bucket-for-hive-bigdata-procamp-b1a78672/Homework_3_1_Result/")
