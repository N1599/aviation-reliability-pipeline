import logging

logging.basicConfig(
    level = logging.INFO
)

log = logging.getLogger(__name__)

from pyspark.sql import SparkSession

def create_spark_session():

    log.info("Creating Spark Session")
    
    spark = (
        SparkSession
        .builder
        .appName("Aviation Pipeline")
        .getOrCreate()
    )

    return spark


def fetch_data(spark):

    log.info("Reading airline dataset")
    
    _schema = "year integer, month integer, carrier string, carrier_name string, airport string, airport_name string, arr_flights double, arr_del15 double, carrier_ct double, weather_ct double, nas_ct double, security_ct double, late_aircraft_ct double, arr_cancelled double, arr_diverted double, arr_delay double, carrier_delay double, weather_delay double, nas_delay double, security_delay double, late_aircraft_delay double"

    df1 = spark.read.format("csv").option("header", True).schema(_schema).load("gs://nikhil-aviation-pipeline/data/Airline_Delay_Cause.csv")
    df1 = df1.select("year" , "carrier_name" , "arr_flights" , "arr_del15","carrier_delay" , "carrier_ct")

    return df1

import sys

def quality_check(data , spark):
    row_count = data.count()

    if row_count == 0:
        log.error("Empty DataFrame. Aborting.")
        spark.stop()
        sys.exit(1)
    log.info(f"Row count: {row_count}. Quality check passed.")
    return data

from pyspark.sql.functions import sum

def agg_data(data):

    log.info("Performing yearly airline aggregations")

    df2 = (
        data.groupBy("year","carrier_name").agg(
            sum("arr_flights").alias("total_arrival_flights"),
            sum("arr_del15").alias("total_arrival_delays"),
            sum("carrier_delay").alias("arrival_delay_minutes"),
            sum("carrier_ct").alias("carrier_delay_count")
        ))
    

    return df2

from pyspark.sql.functions import round,col,when

def metrics(data):

    log.info("Calculating airline KPIs")

    df3 = (
        data.withColumn("carrier_reliability_rate_pct" , when(col("total_arrival_flights") == 0 , 0)
                                                    .otherwise(round(((data.total_arrival_flights - data.carrier_delay_count) / data.total_arrival_flights) * 100 , 2)))
            .withColumn("carrier_caused_delay_pct" , when(col("total_arrival_delays") == 0 , 0)
                                                    .otherwise(round((col("carrier_delay_count") / col("total_arrival_delays")) * 100 , 2)))
            .withColumn("avg_delay_per_delayed_flight_minutes" ,  when(col("total_arrival_delays") == 0 , 0)
                                                    .otherwise(round(col("arrival_delay_minutes") / col("carrier_delay_count") , 2)))
        )

    df3 = df3.select("year" , "carrier_name" , "carrier_reliability_rate_pct" , "carrier_caused_delay_pct" , "avg_delay_per_delayed_flight_minutes")


    return df3

from pyspark.sql.window import Window
from pyspark.sql.functions import min, max

def composite_score(data):

    log.info("Calculating composite score")

    year_window = Window.partitionBy("year")

    df4 = (
            data.withColumn("min_delay" , min("avg_delay_per_delayed_flight_minutes").over(year_window)) 
              .withColumn("max_delay" , max("avg_delay_per_delayed_flight_minutes").over(year_window))  
              .withColumn("normalized_delay" , round(((col("avg_delay_per_delayed_flight_minutes") - col("min_delay")) / (col("max_delay") - col("min_delay"))) * 100 , 2))
              .withColumn("composite_score" , 
                          round(
                              col("carrier_reliability_rate_pct") * 0.5 + 
                              (100 - col("carrier_caused_delay_pct")) * 0.3 + 
                              (100 - col("normalized_delay")) * 0.2 , 2))
              .drop("min_delay","max_delay","normalized_delay")
    
        )
    
    return df4

from pyspark.sql.window import Window
from pyspark.sql.functions import  dense_rank,desc,col,asc

def ranking(data):

    log.info("Ranking top airlines by year")

    df5 = data.withColumn("Rank" , dense_rank().over(
        Window.partitionBy("year")
        .orderBy(col("composite_score").desc() , 
                 col("carrier_reliability_rate_pct").desc(),
                 col("carrier_caused_delay_pct").asc(),
                 col("avg_delay_per_delayed_flight_minutes").asc())))

    df5 = (
        df5.where(df5.Rank <= 3)
        .orderBy(desc("year") , asc("Rank"))
        )

    return df5

def write_to_gcs(data):
    
    log.info("Writing metrics parquet data to GCS")
    
    (
        data.write
        .mode("overwrite")
        .partitionBy("year")
        .parquet("gs://YOUR_BUCKET_NAME/parquet/airline_metrics_parquet")
    )

    log.info("Parquet write completed")

def write_to_bigquery(data):

   log.info("Writing ranked airlines to BigQuery")

   (
        data.write
            .format("bigquery")
            .option("table", "YOUR_PROJECT_ID.aviation_dataset.best_airlines")
            .option("temporaryGcsBucket", "YOUR_BUCKET_NAME")
            .option("writeMethod", "direct")
            .mode("overwrite")
            .save()

   )

   log.info("BigQuery write completed")

if __name__ == "__main__":

    spark = create_spark_session()
    
    raw_data = fetch_data(spark)

    quality_check(raw_data , spark)

    raw_data = raw_data.dropna()

    transformed_data = agg_data(raw_data)

    metrics_data = metrics(transformed_data)
    
    composite_data = composite_score(metrics_data)

    ranked_data = ranking(composite_data)

    write_to_gcs(composite_data)

    write_to_bigquery(ranked_data)

    spark.stop()




