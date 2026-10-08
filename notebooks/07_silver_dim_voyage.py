import os
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from audit_logger import PipelineLogger

TARGET = "workspace.silver.dim_voyage"

STOPPED_STATUSES = [1, 5]   # at anchor, moored
STOPPED_SOG = 0.5           # knots
PORT_RADIUS_KM = 20


def haversine_km(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * 6371 * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


with PipelineLogger(spark, layer="silver", parameter="dim_voyage") as logger:
    pos = spark.table("workspace.silver.fact_vessel_position").select(
        "mmsi", "timestamp", "latitude", "longitude", "sog", "status"
    )

    w = Window.partitionBy("mmsi").orderBy("timestamp")

    stopped_by_status = F.coalesce(F.col("status").isin(STOPPED_STATUSES), F.lit(False))
    pos = pos.withColumn("is_stopped", stopped_by_status | (F.col("sog") < STOPPED_SOG))

    # a voyage starts when a vessel that was stopped (or is new to us) starts moving
    prev_stopped = F.coalesce(F.lag("is_stopped").over(w), F.lit(True))
    pos = pos.withColumn("departed", (~F.col("is_stopped") & prev_stopped).cast("int"))
    pos = pos.withColumn("voyage_seq", F.sum("departed").over(w))

    voyages = (
        pos.filter("voyage_seq > 0")
        .groupBy("mmsi", "voyage_seq")
        .agg(
            F.min(F.when(~F.col("is_stopped"), F.col("timestamp"))).alias("departure_time"),
            F.min(F.when(F.col("is_stopped"), F.struct("timestamp", "latitude", "longitude"))).alias("arrival"),
            F.count("*").alias("position_count"),
        )
        .select(
            "mmsi",
            "voyage_seq",
            "departure_time",
            F.col("arrival.timestamp").alias("arrival_time"),
            F.col("arrival.latitude").alias("arrival_lat"),
            F.col("arrival.longitude").alias("arrival_lon"),
            "position_count",
        )
    )

    ports = spark.table("workspace.silver.dim_port").filter(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ).select("port_code", F.col("latitude").alias("p_lat"), F.col("longitude").alias("p_lon"))

    near_box = (F.abs(F.col("arrival_lat") - F.col("p_lat")) < 0.5) & (
        F.abs(F.col("arrival_lon") - F.col("p_lon")) < 0.5
    )

    nearest_port = (
        voyages.filter(F.col("arrival_time").isNotNull())
        .join(F.broadcast(ports), near_box)
        .withColumn("dist_km", haversine_km(F.col("arrival_lat"), F.col("arrival_lon"), F.col("p_lat"), F.col("p_lon")))
        .filter(F.col("dist_km") <= PORT_RADIUS_KM)
        .withColumn("rn", F.row_number().over(Window.partitionBy("mmsi", "voyage_seq").orderBy("dist_km")))
        .filter("rn = 1")
        .select("mmsi", "voyage_seq", F.col("port_code").alias("arrival_port_code"))
    )

    result = (
        voyages.join(nearest_port, ["mmsi", "voyage_seq"], "left")
        .withColumn("voyage_id", F.concat_ws("_", "mmsi", F.unix_timestamp("departure_time")))
        .withColumn("load_timestamp", F.current_timestamp())
    )

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET)
    )

    total = spark.table(TARGET).count()
    logger.set_metrics(rows_inserted=total)
    print(f"Derived {total} voyages into {TARGET}")
