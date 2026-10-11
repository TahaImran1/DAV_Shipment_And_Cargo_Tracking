# Databricks notebook source
import os
import sys
from datetime import datetime, timezone

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql.functions import (
    col,
    lit,
    coalesce,
    current_timestamp,
    get_json_object,
    expr,
)
from audit_logger import PipelineLogger

try:
    dbutils.widgets.text("source_path", "/Volumes/workspace/bronze/raw_data/ais_daily_20261003.json", "Source Path")
    dbutils.widgets.text("batch_id", datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"), "Batch ID")
    SOURCE = dbutils.widgets.get("source_path")
    batch_id = dbutils.widgets.get("batch_id")
except Exception:
    SOURCE = "/Volumes/workspace/bronze/raw_data/ais_daily_20261003.json"
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

TARGET = "workspace.bronze.raw_aisstream_incremental"
QUARANTINE = "workspace.bronze.quarantine"


# Stringified JSON extraction (Lab 4 Task 4 syntax using get_json_object)
def j(path):
    return get_json_object(col("value"), path)


def position(field):
    return coalesce(
        j(f"$.Message.PositionReport.{field}"),
        j(f"$.Message.StandardClassBPositionReport.{field}"),
    )


def static(field):
    return j(f"$.Message.ShipStaticData.{field}")


with PipelineLogger(spark, layer="bronze", parameter="aisstream_incremental") as logger:
    lines = spark.read.text(SOURCE)

    parsed = lines.select(
        j("$.MessageType").alias("message_type"),
        j("$.MetaData.MMSI").cast("long").alias("mmsi"),
        j("$.MetaData.ShipName").alias("ship_name"),
        j("$.MetaData.latitude").cast("double").alias("latitude"),
        j("$.MetaData.longitude").cast("double").alias("longitude"),
        j("$.MetaData.time_utc").alias("time_utc"),
        position("Sog").cast("double").alias("sog"),
        position("Cog").cast("double").alias("cog"),
        position("TrueHeading").cast("int").alias("true_heading"),
        position("NavigationalStatus").cast("int").alias("nav_status"),
        static("ImoNumber").alias("imo"),
        static("CallSign").alias("call_sign"),
        static("Type").cast("int").alias("ship_type"),
        (static("Dimension.A").cast("double") + static("Dimension.B").cast("double")).alias("length"),
        (static("Dimension.C").cast("double") + static("Dimension.D").cast("double")).alias("width"),
        static("MaximumStaticDraught").cast("double").alias("draft"),
        col("value").alias("raw_json"),
    )

    bad = parsed.filter(col("mmsi").isNull())
    if not bad.isEmpty():
        bad.select(
            expr("uuid()").alias("quarantine_id"),
            lit("aisstream").alias("source"),
            lit(batch_id).alias("batch_id"),
            col("raw_json").alias("raw_payload"),
            lit("corrupt JSON or missing MMSI").alias("error_reason"),
            current_timestamp().alias("quarantine_timestamp"),
        ).write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(QUARANTINE)

    good = (
        parsed.filter(col("mmsi").isNotNull())
        .withColumn("source", lit("aisstream"))
        .withColumn("batch_id", lit(batch_id))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("load_timestamp", current_timestamp())
    )

    # Incremental append write (Lab 3 Task 7)
    good.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET)

    inserted = spark.table(TARGET).filter(col("batch_id") == batch_id).count()
    total = spark.table(TARGET).count()
    logger.set_metrics(rows_inserted=inserted)
    print(f"Ingested {inserted} incremental AIS records into {TARGET} (batch: {batch_id}, total: {total})")

    # Verification query (Lab 3 Task 10)
    spark.sql(f"""
        SELECT COUNT(*) AS total_records
        FROM {TARGET}
    """).show()

