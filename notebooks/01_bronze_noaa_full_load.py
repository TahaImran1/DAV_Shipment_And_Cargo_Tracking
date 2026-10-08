import os
import sys
from datetime import datetime, timezone

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from audit_logger import PipelineLogger

try:
    dbutils.widgets.text("source_path", "/Volumes/workspace/bronze/raw_data/AIS_Full_Load.csv", "Source Path")
    dbutils.widgets.text("batch_id", datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"), "Batch ID")
    SOURCE = dbutils.widgets.get("source_path")
    batch_id = dbutils.widgets.get("batch_id")
except Exception:
    SOURCE = "/Volumes/workspace/bronze/raw_data/AIS_Full_Load.csv"
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

TARGET = "workspace.bronze.raw_noaa_ais"
QUARANTINE = "workspace.bronze.quarantine"

NOAA_SCHEMA = StructType([
    StructField("MMSI", StringType(), True),
    StructField("BaseDateTime", StringType(), True),
    StructField("LAT", StringType(), True),
    StructField("LON", StringType(), True),
    StructField("SOG", StringType(), True),
    StructField("COG", StringType(), True),
    StructField("Heading", StringType(), True),
    StructField("VesselName", StringType(), True),
    StructField("IMO", StringType(), True),
    StructField("CallSign", StringType(), True),
    StructField("VesselType", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Length", StringType(), True),
    StructField("Width", StringType(), True),
    StructField("Draft", StringType(), True),
    StructField("Cargo", StringType(), True),
    StructField("TransceiverClass", StringType(), True),
])


def quarantine(df, source, reason):
    (
        df.select(
            F.expr("uuid()").alias("quarantine_id"),
            F.lit(source).alias("source"),
            F.lit(batch_id).alias("batch_id"),
            F.col("raw_payload"),
            F.lit(reason).alias("error_reason"),
            F.current_timestamp().alias("quarantine_timestamp"),
        )
        .write.format("delta").mode("append").saveAsTable(QUARANTINE)
    )


with PipelineLogger(spark, layer="bronze", parameter="noaa_full_load") as logger:
    raw = (
        spark.read
        .option("header", True)
        .schema(NOAA_SCHEMA)
        .csv(SOURCE)
    )
    raw = raw.toDF(*[c.replace("\ufeff", "").strip() for c in raw.columns])

    is_bad = F.col("MMSI").isNull() | F.col("BaseDateTime").isNull()

    good = (
        raw.filter(~is_bad)
        .withColumn("source", F.lit("noaa"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("load_timestamp", F.current_timestamp())
    )

    bad = raw.filter(is_bad).withColumn("raw_payload", F.to_json(F.struct(*raw.columns)))
    if not bad.isEmpty():
        quarantine(bad, "noaa", "missing MMSI or BaseDateTime")

    (
        good.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET)
    )

    total = spark.table(TARGET).count()
    logger.set_metrics(rows_inserted=total)
    print(f"Ingested {total} NOAA records into {TARGET} (batch: {batch_id})")
