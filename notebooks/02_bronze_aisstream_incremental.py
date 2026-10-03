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
    LongType,
    BooleanType,
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
        dbutils.widgets.text("batch_file", "/Volumes/workspace/bronze/raw_data/ais_daily_20261003.json", "Source Batch File or Directory")
        dbutils.widgets.text("batch_id", "incremental-auto", "Batch Identifier")
        batch_file = dbutils.widgets.get("batch_file")
        batch_id = dbutils.widgets.get("batch_id")
    except Exception:
        batch_file = "/Volumes/workspace/bronze/raw_data/ais_daily_20261003.json"
        batch_id = "incremental-auto"
else:
    batch_file = "/Volumes/workspace/bronze/raw_data/ais_daily_20261003.json"
    batch_id = "incremental-auto"

dimension_schema = StructType([
    StructField("A", IntegerType(), True),
    StructField("B", IntegerType(), True),
    StructField("C", IntegerType(), True),
    StructField("D", IntegerType(), True),
])

eta_schema = StructType([
    StructField("Month", IntegerType(), True),
    StructField("Day", IntegerType(), True),
    StructField("Hour", IntegerType(), True),
    StructField("Minute", IntegerType(), True),
])

position_report_schema = StructType([
    StructField("MessageID", IntegerType(), True),
    StructField("RepeatIndicator", IntegerType(), True),
    StructField("UserID", LongType(), True),
    StructField("Valid", BooleanType(), True),
    StructField("NavigationalStatus", IntegerType(), True),
    StructField("RateOfTurn", DoubleType(), True),
    StructField("Sog", DoubleType(), True),
    StructField("PositionAccuracy", BooleanType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Cog", DoubleType(), True),
    StructField("TrueHeading", DoubleType(), True),
    StructField("Timestamp", IntegerType(), True),
    StructField("SpecialManoeuvreIndicator", IntegerType(), True),
    StructField("Spare", IntegerType(), True),
    StructField("Raim", BooleanType(), True),
    StructField("CommunicationState", LongType(), True),
])

ship_static_data_schema = StructType([
    StructField("MessageID", IntegerType(), True),
    StructField("RepeatIndicator", IntegerType(), True),
    StructField("UserID", LongType(), True),
    StructField("Valid", BooleanType(), True),
    StructField("AisVersion", IntegerType(), True),
    StructField("ImoNumber", LongType(), True),
    StructField("CallSign", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Type", IntegerType(), True),
    StructField("Dimension", dimension_schema, True),
    StructField("FixType", IntegerType(), True),
    StructField("Eta", eta_schema, True),
    StructField("MaximumStaticDraught", DoubleType(), True),
    StructField("Destination", StringType(), True),
    StructField("Dte", IntegerType(), True),
    StructField("Spare", BooleanType(), True),
])

static_data_report_schema = StructType([
    StructField("MessageID", IntegerType(), True),
    StructField("RepeatIndicator", IntegerType(), True),
    StructField("UserID", LongType(), True),
    StructField("Valid", BooleanType(), True),
    StructField("PartNumber", IntegerType(), True),
])

message_body_schema = StructType([
    StructField("PositionReport", position_report_schema, True),
    StructField("StandardClassBPositionReport", position_report_schema, True),
    StructField("ExtendedClassBPositionReport", position_report_schema, True),
    StructField("ShipStaticData", ship_static_data_schema, True),
    StructField("StaticDataReport", static_data_report_schema, True),
])

metadata_schema = StructType([
    StructField("MMSI", LongType(), True),
    StructField("MMSI_String", LongType(), True),
    StructField("ShipName", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("time_utc", StringType(), True),
])

schema = StructType([
    StructField("MetaData", metadata_schema, True),
    StructField("MessageType", StringType(), True),
    StructField("Message", message_body_schema, True),
    StructField("_collected_at", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

with PipelineLogger(spark, layer="Raw-to-Bronze (AISStream)", parameter=batch_id) as logger:
    raw_df = (
        spark.read.format("json")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .load(batch_file)
    )

    corrupt_df = raw_df.filter(col("_corrupt_record").isNotNull())
    corrupt_count = corrupt_df.count()

    if corrupt_count > 0:
        quarantine_records = (
            corrupt_df.select(
                expr("uuid()").alias("quarantine_id"),
                lit("aisstream").alias("source"),
                lit(batch_id).alias("batch_id"),
                col("_corrupt_record").alias("raw_payload"),
                lit("Malformed JSON record").alias("error_reason"),
                current_timestamp().alias("quarantine_timestamp"),
            )
        )
        quarantine_records.write.format("delta").mode("append").saveAsTable("bronze.quarantine")
        print(f"Quarantined {corrupt_count} records")

    valid_df = (
        raw_df.filter(col("_corrupt_record").isNull())
        .drop("_corrupt_record")
        .withColumn("source", lit("aisstream"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
        .withColumn("load_timestamp", current_timestamp())
    )

    (
        valid_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("bronze.raw_ais_messages")
    )

    valid_count = valid_df.count()
    logger.set_metrics(rows_inserted=valid_count, rows_updated=0)
    print(f"Ingested {valid_count} records into bronze.raw_ais_messages")
