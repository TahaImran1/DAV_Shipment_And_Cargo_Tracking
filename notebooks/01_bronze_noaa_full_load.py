import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from audit_logger import PipelineLogger

from pyspark.sql.functions import current_timestamp, lit, col, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

try:
    dbutils
except NameError:
    dbutils = None

if dbutils is not None:
    try:
        dbutils.widgets.text("source_path", "/Volumes/workspace/bronze/raw_data/AIS_Full_Load.csv", "Source File Path")
        dbutils.widgets.text("batch_id", "2024-01-full-load", "Batch Identifier")
        source_path = dbutils.widgets.get("source_path")
        batch_id = dbutils.widgets.get("batch_id")
    except Exception:
        source_path = "/Volumes/workspace/bronze/raw_data/AIS_Full_Load.csv"
        batch_id = "2024-01-full-load"
else:
    source_path = "/Volumes/workspace/bronze/raw_data/AIS_Full_Load.csv"
    batch_id = "2024-01-full-load"

schema = StructType([
    StructField("MMSI", StringType(), True),
    StructField("BaseDateTime", StringType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
    StructField("SOG", DoubleType(), True),
    StructField("COG", DoubleType(), True),
    StructField("Heading", DoubleType(), True),
    StructField("VesselName", StringType(), True),
    StructField("IMO", StringType(), True),
    StructField("CallSign", StringType(), True),
    StructField("VesselType", IntegerType(), True),
    StructField("Status", IntegerType(), True),
    StructField("Length", DoubleType(), True),
    StructField("Width", DoubleType(), True),
    StructField("Draft", DoubleType(), True),
    StructField("Cargo", IntegerType(), True),
    StructField("transceiverClass", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

with PipelineLogger(spark, layer="Raw-to-Bronze (NOAA)", parameter=batch_id) as logger:
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .load(source_path)
    )

    corrupt_df = raw_df.filter(col("_corrupt_record").isNotNull())
    corrupt_count = corrupt_df.count()

    if corrupt_count > 0:
        quarantine_records = (
            corrupt_df.select(
                expr("uuid()").alias("quarantine_id"),
                lit("noaa").alias("source"),
                lit(batch_id).alias("batch_id"),
                col("_corrupt_record").alias("raw_payload"),
                lit("Malformed CSV record").alias("error_reason"),
                current_timestamp().alias("quarantine_timestamp"),
            )
        )
        quarantine_records.write.format("delta").mode("append").saveAsTable("bronze.quarantine")
        print(f"Quarantined {corrupt_count} corrupt records")

    valid_df = (
        raw_df.filter(col("_corrupt_record").isNull())
        .drop("_corrupt_record")
        .withColumn("source", lit("noaa"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
        .withColumn("load_timestamp", current_timestamp())
    )

    (
        valid_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("bronze.raw_noaa_ais")
    )

    valid_count = valid_df.count()
    logger.set_metrics(rows_inserted=valid_count, rows_updated=0)
    print(f"Ingested {valid_count} records into bronze.raw_noaa_ais")
