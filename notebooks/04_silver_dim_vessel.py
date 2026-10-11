import os
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim,
    regexp_replace,
    to_timestamp,
    substring,
    length,
    first,
    current_timestamp,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from audit_logger import PipelineLogger

TARGET = "workspace.silver.dim_vessel"
COLS = ["vessel_name", "imo", "call_sign", "vessel_type", "length", "width", "draft"]


def text(c):
    return when(trim(c) != "", trim(c))


def positive(c):
    return when(c.cast("double") > 0, c.cast("double"))


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


with PipelineLogger(spark, layer="silver", parameter="dim_vessel") as logger:
    noaa = spark.table("workspace.bronze.raw_noaa_ais").select(
        col("MMSI").cast("long").alias("mmsi"),
        to_timestamp("BaseDateTime").alias("ts"),
        text(col("VesselName")).alias("vessel_name"),
        text(regexp_replace(col("IMO"), "^IMO", "")).alias("imo"),
        text(col("CallSign")).alias("call_sign"),
        col("VesselType").cast("int").alias("vessel_type"),
        positive(col("Length")).alias("length"),
        positive(col("Width")).alias("width"),
        positive(col("Draft")).alias("draft"),
    )

    live = spark.table("workspace.bronze.raw_aisstream_incremental").select(
        col("mmsi"),
        to_timestamp(substring("time_utc", 1, 19)).alias("ts"),
        text(col("ship_name")).alias("vessel_name"),
        text(col("imo")).alias("imo"),
        text(col("call_sign")).alias("call_sign"),
        col("ship_type").alias("vessel_type"),
        positive(col("length")).alias("length"),
        positive(col("width")).alias("width"),
        positive(col("draft")).alias("draft"),
    )

    vessels = noaa.unionByName(live).filter(length(col("mmsi").cast("string")) == 9)

    w = (
        Window.partitionBy("mmsi")
        .orderBy(col("ts").desc_nulls_last())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    dim = (
        vessels.select("mmsi", *[first(c, ignorenulls=True).over(w).alias(c) for c in COLS])
        .dropDuplicates(["mmsi"])
        .withColumn("load_timestamp", current_timestamp())
    )

    inserted, updated = merge_into(dim, TARGET, ["mmsi"])
    logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
    total = spark.table(TARGET).count()
    print(f"MERGE completed on {TARGET}: {inserted} inserted, {updated} updated (total: {total})")

    spark.sql(f"""
        SELECT mmsi, COUNT(*) AS occurrences
        FROM {TARGET}
        GROUP BY mmsi
        HAVING COUNT(*) > 1
    """).show()

    spark.sql(f"""
        SELECT COUNT(*) AS total_records
        FROM {TARGET}
    """).show()

