import os
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from audit_logger import PipelineLogger

TARGET = "workspace.silver.dim_vessel"
COLS = ["vessel_name", "imo", "call_sign", "vessel_type", "length", "width", "draft"]


def text(c):
    return F.when(F.trim(c) != "", F.trim(c))


def positive(c):
    return F.when(c.cast("double") > 0, c.cast("double"))


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


with PipelineLogger(spark, layer="silver", parameter="dim_vessel") as logger:
    noaa = spark.table("workspace.bronze.raw_noaa_ais").select(
        F.col("MMSI").cast("long").alias("mmsi"),
        F.to_timestamp("BaseDateTime").alias("ts"),
        text(F.col("VesselName")).alias("vessel_name"),
        text(F.regexp_replace(F.col("IMO"), "^IMO", "")).alias("imo"),
        text(F.col("CallSign")).alias("call_sign"),
        F.col("VesselType").cast("int").alias("vessel_type"),
        positive(F.col("Length")).alias("length"),
        positive(F.col("Width")).alias("width"),
        positive(F.col("Draft")).alias("draft"),
    )

    live = spark.table("workspace.bronze.raw_aisstream_incremental").select(
        F.col("mmsi"),
        F.to_timestamp(F.substring("time_utc", 1, 19)).alias("ts"),
        text(F.col("ship_name")).alias("vessel_name"),
        text(F.col("imo")).alias("imo"),
        text(F.col("call_sign")).alias("call_sign"),
        F.col("ship_type").alias("vessel_type"),
        positive(F.col("length")).alias("length"),
        positive(F.col("width")).alias("width"),
        positive(F.col("draft")).alias("draft"),
    )

    vessels = noaa.unionByName(live).filter(F.length(F.col("mmsi").cast("string")) == 9)

    # for each vessel take the most recent non-null value of every attribute
    w = (
        Window.partitionBy("mmsi")
        .orderBy(F.col("ts").desc_nulls_last())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    dim = (
        vessels.select("mmsi", *[F.first(c, ignorenulls=True).over(w).alias(c) for c in COLS])
        .dropDuplicates(["mmsi"])
        .withColumn("load_timestamp", F.current_timestamp())
    )

    inserted, updated = merge_into(dim, TARGET, ["mmsi"])
    logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
