import os
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from audit_logger import PipelineLogger

TARGET = "workspace.silver.fact_vessel_position"


def merge_into(df, target, keys):
    df.createOrReplaceTempView("src")
    if not spark.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""
        MERGE INTO {target} t USING src s ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    m = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()["operationMetrics"]
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))


with PipelineLogger(spark, layer="silver", parameter="fact_vessel_position") as logger:
    noaa = spark.table("workspace.bronze.raw_noaa_ais").select(
        F.col("MMSI").cast("long").alias("mmsi"),
        F.to_timestamp("BaseDateTime").alias("timestamp"),
        F.col("LAT").cast("double").alias("latitude"),
        F.col("LON").cast("double").alias("longitude"),
        F.col("SOG").cast("double").alias("sog"),
        F.col("COG").cast("double").alias("cog"),
        F.col("Heading").cast("double").alias("heading"),
        F.col("Status").cast("int").alias("status"),
        F.lit("noaa").alias("source"),
    )

    live = spark.table("workspace.bronze.raw_aisstream_incremental").select(
        F.col("mmsi"),
        F.to_timestamp(F.substring("time_utc", 1, 19)).alias("timestamp"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("sog"),
        F.col("cog"),
        F.col("true_heading").cast("double").alias("heading"),
        F.col("nav_status").alias("status"),
        F.lit("aisstream").alias("source"),
    )

    # nulls fail the comparisons below, so they get filtered out too
    positions = (
        noaa.unionByName(live)
        .filter(
            F.col("mmsi").isNotNull()
            & F.col("timestamp").isNotNull()
            & F.col("latitude").between(-90, 90)
            & F.col("longitude").between(-180, 180)
            & (F.col("sog") >= 0)
        )
        .dropDuplicates(["mmsi", "timestamp"])
        .withColumn("load_timestamp", F.current_timestamp())
    )

    inserted, updated = merge_into(positions, TARGET, ["mmsi", "timestamp"])
    logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
