import os
import re
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from audit_logger import PipelineLogger

try:
    dbutils.widgets.text("source_path", "/Volumes/workspace/bronze/raw_data/WPI.csv", "Source Path")
    dbutils.widgets.text("batch_id", "wpi-reference-v1", "Batch ID")
    SOURCE = dbutils.widgets.get("source_path")
    batch_id = dbutils.widgets.get("batch_id")
except Exception:
    SOURCE = "/Volumes/workspace/bronze/raw_data/WPI.csv"
    batch_id = "wpi-reference-v1"

TARGET = "workspace.bronze.raw_wpi_ports"


def clean_name(name):
    name = name.replace("\ufeff", "").strip()
    return re.sub(r"[ ,;{}()\n\t=]+", "_", name)


with PipelineLogger(spark, layer="bronze", parameter="wpi_reference") as logger:
    raw = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(SOURCE)
    )

    df = raw.toDF(*[clean_name(c) for c in raw.columns])
    df = (
        df.withColumn("portNumber", F.col("portNumber").cast("int"))
        .withColumn("source", F.lit("wpi"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("load_timestamp", F.current_timestamp())
    )

    missing = df.filter(F.col("portNumber").isNull()).count()
    if missing:
        print(f"Dropping {missing} rows without a valid portNumber")
    df = df.filter(F.col("portNumber").isNotNull())

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET)
    )

    total = spark.table(TARGET).count()
    logger.set_metrics(rows_inserted=total)
    print(f"Ingested {total} ports into {TARGET}")
