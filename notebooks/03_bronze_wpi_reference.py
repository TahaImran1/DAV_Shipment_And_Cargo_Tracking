import os
import re
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from audit_logger import PipelineLogger

SOURCE = "/Volumes/workspace/bronze/raw_data/WPI.csv"
TARGET = "workspace.bronze.raw_wpi_ports"


def clean_name(name):
    # the file starts with a BOM, so the first header comes through as "\ufeffportNumber"
    name = name.replace("\ufeff", "").strip()
    return re.sub(r"[ ,;{}()\n\t=]+", "_", name)


with PipelineLogger(spark, layer="bronze", parameter="wpi_reference") as logger:
    # escape='"' matters here: DMS values like "30°20'00""N" contain doubled quotes
    raw = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(SOURCE)
    )

    df = raw.toDF(*[clean_name(c) for c in raw.columns])
    df = df.withColumn("portNumber", F.col("portNumber").cast("int"))

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
