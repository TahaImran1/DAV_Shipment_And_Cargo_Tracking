# Databricks notebook source
import os
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql.functions import (
    col,
    lit,
    coalesce,
    lag,
    when,
    count as _count,
    sum as _sum,
    avg as _avg,
    round as _round,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    least,
    abs as _abs,
    broadcast,
    row_number,
    to_date,
    unix_timestamp,
)
from pyspark.sql.window import Window
from audit_logger import PipelineLogger

PORT_RADIUS_KM = 15
MAX_GAP_HOURS = 2          # ignore gaps between pings longer than this
MOVING_SOG = 0.5           # knots
DESIGN_SPEED_KNOTS = 14
CO2_PER_TONNE_FUEL = 3.114


def haversine_km(lat1, lon1, lat2, lon2):
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * 6371 * asin(sqrt(least(lit(1.0), a)))


# Gold Delta write helper (Lab 3 Task 6 & 9 format)
def save(df, table):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    return spark.table(table).count()


with PipelineLogger(spark, layer="gold", parameter="aggregations") as logger:
    pos = spark.table("workspace.silver.fact_vessel_position")

    # ---- port congestion ----
    ports = spark.table("workspace.silver.dim_port").filter(
        col("latitude").isNotNull() & col("longitude").isNotNull()
    ).select("port_code", "port_name", col("latitude").alias("p_lat"), col("longitude").alias("p_lon"))

    # One point per slow-moving vessel per day keeps the port join small
    stationary = (
        pos.filter(col("sog") < 1)
        .withColumn("date", to_date("timestamp"))
        .groupBy("mmsi", "date")
        .agg(_avg("latitude").alias("lat"), _avg("longitude").alias("lon"))
    )

    near_box = (_abs(col("lat") - col("p_lat")) < 0.5) & (_abs(col("lon") - col("p_lon")) < 0.5)

    congestion = (
        stationary.join(broadcast(ports), near_box)
        .withColumn("dist_km", haversine_km(col("lat"), col("lon"), col("p_lat"), col("p_lon")))
        .filter(col("dist_km") <= PORT_RADIUS_KM)
        .withColumn("rn", row_number().over(Window.partitionBy("mmsi", "date").orderBy("dist_km")))
        .filter("rn = 1")
        .groupBy("port_code", "port_name", "date")
        .agg(_count("*").alias("vessels_in_port"))
    )

    # ---- per-leg metrics (consecutive pings of the same vessel) ----
    w = Window.partitionBy("mmsi").orderBy("timestamp")
    legs = (
        pos.withColumn("prev_lat", lag("latitude").over(w))
        .withColumn("prev_lon", lag("longitude").over(w))
        .withColumn("prev_ts", lag("timestamp").over(w))
        .withColumn("hours", (unix_timestamp("timestamp") - unix_timestamp("prev_ts")) / 3600)
        .filter((col("hours") > 0) & (col("hours") <= MAX_GAP_HOURS))
        .withColumn("distance_km", haversine_km(col("prev_lat"), col("prev_lon"), col("latitude"), col("longitude")))
        .withColumn("date", to_date("timestamp"))
    )

    moving_hours = _sum(when(col("sog") >= MOVING_SOG, col("hours")).otherwise(0))

    activity = legs.groupBy("mmsi", "date").agg(
        _round(_sum("distance_km"), 2).alias("total_distance_km"),
        _round(_avg("sog"), 2).alias("avg_speed_knots"),
        _round(moving_hours, 2).alias("operating_hours"),
    )

    # ---- emissions proxy: rough fuel burn using the cube law on speed ----
    vessels = spark.table("workspace.silver.dim_vessel").select("mmsi", "vessel_type", "length")

    vt = col("vessel_type")
    type_factor = (
        when(vt.between(80, 89), 1.4)    # tankers
        .when(vt.between(70, 79), 1.2)     # cargo
        .when(vt.between(60, 69), 1.5)     # passenger
        .when(vt.between(30, 39), 0.3)     # fishing / towing / etc.
        .otherwise(0.6)
    )
    length_m = coalesce(col("length"), lit(100.0))
    tonnes_per_hour = type_factor * (length_m / 100) ** 2
    speed_ratio = least(col("sog"), lit(30.0)) / DESIGN_SPEED_KNOTS
    fuel_tonnes = tonnes_per_hour * speed_ratio ** 3 * col("hours")

    emissions = (
        legs.join(vessels, "mmsi", "left")
        .withColumn("fuel_tonnes", fuel_tonnes)
        .groupBy("mmsi", "vessel_type", "date")
        .agg(
            _round(_sum("fuel_tonnes"), 3).alias("fuel_burn_proxy_tonnes"),
            _round(moving_hours, 2).alias("operating_hours"),
        )
        .withColumn("co2_proxy_tonnes", _round(col("fuel_burn_proxy_tonnes") * CO2_PER_TONNE_FUEL, 3))
    )

    c_count = save(congestion, "workspace.gold.port_congestion_daily")
    a_count = save(activity, "workspace.gold.daily_vessel_activity")
    e_count = save(emissions, "workspace.gold.vessel_emissions_proxy")
    total = c_count + a_count + e_count

    logger.set_metrics(rows_inserted=total)
    print("Gold tables refreshed:")
    print(f"  - port_congestion_daily: {c_count} rows")
    print(f"  - daily_vessel_activity: {a_count} rows")
    print(f"  - vessel_emissions_proxy: {e_count} rows")

    # Multi-table verification check (Lab 4 Task 7)
    spark.sql("""
        SELECT 'port_congestion_daily' AS table_name, COUNT(*) AS total_records FROM workspace.gold.port_congestion_daily
        UNION ALL
        SELECT 'daily_vessel_activity' AS table_name, COUNT(*) AS total_records FROM workspace.gold.daily_vessel_activity
        UNION ALL
        SELECT 'vessel_emissions_proxy' AS table_name, COUNT(*) AS total_records FROM workspace.gold.vessel_emissions_proxy
    """).show()

