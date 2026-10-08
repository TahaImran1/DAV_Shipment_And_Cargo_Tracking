import os
import sys
from datetime import datetime, timezone

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from audit_logger import PipelineLogger

SOURCE = "/Volumes/workspace/bronze/raw_data/AIS_Full_Load.csv"
TARGET = "workspace.bronze.raw_noaa_ais"
QUARANTINE = "workspace.bronze.quarantine"

batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


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
    # everything stays a string in bronze, typing happens in silver
    raw = spark.read.option("header", True).csv(SOURCE)
    raw = raw.toDF(*[c.replace("\ufeff", "").strip() for c in raw.columns])

    is_bad = F.col("MMSI").isNull() | F.col("BaseDateTime").isNull()

    good = (
        raw.filter(~is_bad)
        .withColumn("source", F.lit("noaa"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_timestamp", F.current_timestamp())
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
