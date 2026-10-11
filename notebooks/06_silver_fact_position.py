import os
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql.functions import (
    col,
    lit,
    to_timestamp,
    substring,
    current_timestamp,
)
from delta.tables import DeltaTable
from audit_logger import PipelineLogger

TARGET = "workspace.silver.fact_vessel_position"


def merge_into(df, target, keys):
    if not spark.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)

    target_table = DeltaTable.forName(spark, target)
    join_condition = " AND ".join(f"target.{k} = source.{k}" for k in keys)

    (
        target_table.alias("target")
        .merge(df.alias("source"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    m = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()["operationMetrics"]
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))


with PipelineLogger(spark, layer="silver", parameter="fact_vessel_position") as logger:
    noaa = spark.table("workspace.bronze.raw_noaa_ais").select(
        col("MMSI").cast("long").alias("mmsi"),
        to_timestamp("BaseDateTime").alias("timestamp"),
        col("LAT").cast("double").alias("latitude"),
        col("LON").cast("double").alias("longitude"),
        col("SOG").cast("double").alias("sog"),
        col("COG").cast("double").alias("cog"),
        col("Heading").cast("double").alias("heading"),
        col("Status").cast("int").alias("status"),
        lit("noaa").alias("source"),
    )

    live = spark.table("workspace.bronze.raw_aisstream_incremental").select(
        col("mmsi"),
        to_timestamp(substring("time_utc", 1, 19)).alias("timestamp"),
        col("latitude"),
        col("longitude"),
        col("sog"),
        col("cog"),
        col("true_heading").cast("double").alias("heading"),
        col("nav_status").alias("status"),
        lit("aisstream").alias("source"),
    )

    positions = (
        noaa.unionByName(live)
        .filter(
            col("mmsi").isNotNull()
            & col("timestamp").isNotNull()
            & col("latitude").between(-90, 90)
            & col("longitude").between(-180, 180)
            & (col("sog") >= 0)
        )
        .dropDuplicates(["mmsi", "timestamp"])
        .withColumn("load_timestamp", current_timestamp())
    )

    inserted, updated = merge_into(positions, TARGET, ["mmsi", "timestamp"])
    logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
    total = spark.table(TARGET).count()
    print(f"MERGE completed on {TARGET}: {inserted} inserted, {updated} updated (total: {total})")

    spark.sql(f"""
        SELECT mmsi, timestamp, COUNT(*) AS occurrences
        FROM {TARGET}
        GROUP BY mmsi, timestamp
        HAVING COUNT(*) > 1
    """).show()

    spark.sql(f"""
        SELECT COUNT(*) AS total_records
        FROM {TARGET}
    """).show()

