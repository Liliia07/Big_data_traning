#!/usr/bin/env python
# coding: utf-8
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import desc, row_number, col, count, sum
from pyspark.sql.functions import *

spark = SparkSession \
            .builder \
            .enableHiveSupport() \
            .appName('Homework_3_2')\
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
.add("CANCELLED",IntegerType(),True) \
.add("CANCELLATION_REASON",StringType(),True) \
.add("AIR_SYSTEM_DELAY",IntegerType(),True) \
.add("SECURITY_DELAY",IntegerType(),True) \
.add("AIRLINE_DELAY",IntegerType(),True) \
.add("LATE_AIRCRAFT_DELAY",IntegerType(),True) \
.add("WEATHER_DELAY",IntegerType(),True)


df_flights = spark.read.load("gs://bucket-for-hive-bigdata-procamp-b1a78672/2015_Flight_Delays_and_Cancellations/flights.csv",
                     format="csv", schema=schema, header="true")

df_airports = spark.read.load("gs://bucket-for-hive-bigdata-procamp-b1a78672/2015_Flight_Delays_and_Cancellations/airports.csv",
                     format="csv", header="true")


# Calculate percentage of canceled flights per origin airport per airline. Save the result to the GCS bucket in json format sorted by airline name and percentage for all airports but 'Waco Regional Airport' which should be stored in CSV format. Record should contain airline name, origin airport name, percentage, number of canceled flights, number of processed flights. Gather total number of flights per airline for debugging
# 
# SELECT ORIGIN_AIRPORT, AIRLINE, SUM(CANCELLED)/COUNT(*)*100 AS PERCENTAGE_OF_CANCELED_FLIGHTS from prod
#  GROUP BY ORIGIN_AIRPORT, AIRLINE
#  LIMIT 5

df_percentage_of_cancelled_flights = df_flights.groupBy('ORIGIN_AIRPORT','AIRLINE').agg((100*sum('CANCELLED')/count('CANCELLED')).alias('PERCENTAGE_OF_CANCELED_FLIGHTS'))

df_join = df_percentage_of_cancelled_flights.join(df_airports, df_percentage_of_cancelled_flights["ORIGIN_AIRPORT"] == df_airports["IATA_CODE"], "inner").select(df_percentage_of_cancelled_flights['*'],df_airports['AIRPORT'])

statistics_for_Waco_Regional_Airport = df_join.filter(col('AIRPORT')=='Waco Regional Airport').orderBy('AIRLINE')

statistics_for_other_Airports = df_join.filter(col('AIRPORT') != 'Waco Regional Airport').orderBy('AIRLINE')

statistics_for_Waco_Regional_Airport.write.mode("overwrite").option("header", "true").format("csv").save("gs://bucket-for-hive-bigdata-procamp-b1a78672/Homework_3_2_Result/Rusult_in_csv/")

statistics_for_other_Airports.write.mode("overwrite").option("header", "true").format("json").save("gs://bucket-for-hive-bigdata-procamp-b1a78672/Homework_3_2_Result/Rusult_in_json/")
