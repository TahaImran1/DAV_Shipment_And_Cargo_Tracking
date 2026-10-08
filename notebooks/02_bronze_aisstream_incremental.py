import os
import sys
from datetime import datetime, timezone

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from audit_logger import PipelineLogger

SOURCE = "/Volumes/workspace/bronze/raw_data/ais_daily_20261003.json"
TARGET = "workspace.bronze.raw_aisstream_incremental"
QUARANTINE = "workspace.bronze.quarantine"

batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def j(path):
    return F.get_json_object("value", path)


def position(field):
    return F.coalesce(
        j(f"$.Message.PositionReport.{field}"),
        j(f"$.Message.StandardClassBPositionReport.{field}"),
    )


def static(field):
    return j(f"$.Message.ShipStaticData.{field}")


with PipelineLogger(spark, layer="bronze", parameter="aisstream_incremental") as logger:
    # one JSON message per line; get_json_object returns null on bad lines instead of failing
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
        F.col("value").alias("raw_json"),
    )

    bad = parsed.filter(F.col("mmsi").isNull())
    if not bad.isEmpty():
        (
            bad.select(
                F.expr("uuid()").alias("quarantine_id"),
                F.lit("aisstream").alias("source"),
                F.lit(batch_id).alias("batch_id"),
                F.col("raw_json").alias("raw_payload"),
                F.lit("corrupt JSON or missing MMSI").alias("error_reason"),
                F.current_timestamp().alias("quarantine_timestamp"),
            )
            .write.format("delta").mode("append").saveAsTable(QUARANTINE)
        )

    good = (
        parsed.filter(F.col("mmsi").isNotNull())
        .withColumn("source", F.lit("aisstream"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    good.write.format("delta").mode("append").saveAsTable(TARGET)

    logger.set_metrics(rows_inserted=spark.table(TARGET).filter(F.col("batch_id") == batch_id).count())
