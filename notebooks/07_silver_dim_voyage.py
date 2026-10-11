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
    min as _min,
    count as _count,
    sum as _sum,
    struct,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    least,
    abs as _abs,
    broadcast,
    row_number,
    concat_ws,
    unix_timestamp,
    current_timestamp,
)
from pyspark.sql.window import Window
from audit_logger import PipelineLogger

TARGET = "workspace.silver.dim_voyage"

STOPPED_STATUSES = [1, 5]   # at anchor, moored
STOPPED_SOG = 0.5           # knots
PORT_RADIUS_KM = 20


def haversine_km(lat1, lon1, lat2, lon2):
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * 6371 * asin(sqrt(least(lit(1.0), a)))


with PipelineLogger(spark, layer="silver", parameter="dim_voyage") as logger:
    pos = spark.table("workspace.silver.fact_vessel_position").select(
        "mmsi", "timestamp", "latitude", "longitude", "sog", "status"
    )

    w = Window.partitionBy("mmsi").orderBy("timestamp")

    stopped_by_status = coalesce(col("status").isin(STOPPED_STATUSES), lit(False))
    pos = pos.withColumn("is_stopped", stopped_by_status | (col("sog") < STOPPED_SOG))

    # A voyage starts when a vessel that was stopped (or is new to us) starts moving
    prev_stopped = coalesce(lag("is_stopped").over(w), lit(True))
    pos = pos.withColumn("departed", (~col("is_stopped") & prev_stopped).cast("int"))
    pos = pos.withColumn("voyage_seq", _sum("departed").over(w))

    voyages = (
        pos.filter("voyage_seq > 0")
        .groupBy("mmsi", "voyage_seq")
        .agg(
            _min(when(~col("is_stopped"), col("timestamp"))).alias("departure_time"),
            _min(when(col("is_stopped"), struct("timestamp", "latitude", "longitude"))).alias("arrival"),
            _count("*").alias("position_count"),
        )
        .select(
            "mmsi",
            "voyage_seq",
            "departure_time",
            col("arrival.timestamp").alias("arrival_time"),
            col("arrival.latitude").alias("arrival_lat"),
            col("arrival.longitude").alias("arrival_lon"),
            "position_count",
        )
    )

    ports = spark.table("workspace.silver.dim_port").filter(
        col("latitude").isNotNull() & col("longitude").isNotNull()
    ).select("port_code", col("latitude").alias("p_lat"), col("longitude").alias("p_lon"))

    near_box = (_abs(col("arrival_lat") - col("p_lat")) < 0.5) & (
        _abs(col("arrival_lon") - col("p_lon")) < 0.5
    )

    nearest_port = (
        voyages.filter(col("arrival_time").isNotNull())
        .join(broadcast(ports), near_box)
        .withColumn("dist_km", haversine_km(col("arrival_lat"), col("arrival_lon"), col("p_lat"), col("p_lon")))
        .filter(col("dist_km") <= PORT_RADIUS_KM)
        .withColumn("rn", row_number().over(Window.partitionBy("mmsi", "voyage_seq").orderBy("dist_km")))
        .filter("rn = 1")
        .select("mmsi", "voyage_seq", col("port_code").alias("arrival_port_code"))
    )

    result = (
        voyages.join(nearest_port, ["mmsi", "voyage_seq"], "left")
        .withColumn("voyage_id", concat_ws("_", "mmsi", unix_timestamp("departure_time")))
        .withColumn("load_timestamp", current_timestamp())
    )

    # Silver Delta overwrite write (Lab 3 Task 4 format)
    result.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TARGET)

    total = spark.table(TARGET).count()
    logger.set_metrics(rows_inserted=total)
    print(f"Derived {total} voyages into {TARGET}")

    # Verification count query (Lab 3 Task 10)
    spark.sql(f"""
        SELECT COUNT(*) AS total_records
        FROM {TARGET}
    """).show()

