# Databricks notebook source
# COMMAND ----------
import time
import uuid
import traceback
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)

AUDIT_LOG_SCHEMA = StructType([
    StructField("log_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("parameter", StringType(), True),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("rows_inserted", LongType(), True),
    StructField("rows_updated", LongType(), True),
    StructField("error_message", StringType(), True),
])

AUDIT_TABLE_NAME = "maritime_ops.pipeline_execution_logs"

# COMMAND ----------
def init_audit_table(spark: SparkSession) -> None:
    spark.sql("CREATE SCHEMA IF NOT EXISTS maritime_ops")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_TABLE_NAME} (
            log_id STRING NOT NULL,
            layer STRING NOT NULL,
            parameter STRING,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            status STRING NOT NULL,
            rows_inserted BIGINT,
            rows_updated BIGINT,
            error_message STRING
        )
        USING DELTA
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    """)

# COMMAND ----------
class PipelineLogger:
    def __init__(self, spark: SparkSession, layer: str, parameter: str = None):
        self.spark = spark
        self.layer = layer
        self.parameter = parameter or "N/A"
        self.log_id = str(uuid.uuid4())
        self.start_time = datetime.now(timezone.utc)
        self.end_time = None
        self.status = "Running"
        self.rows_inserted = 0
        self.rows_updated = 0
        self.error_message = None

    def __enter__(self):
        init_audit_table(self.spark)
        return self

    def set_metrics(self, rows_inserted: int = 0, rows_updated: int = 0):
        self.rows_inserted = int(rows_inserted or 0)
        self.rows_updated = int(rows_updated or 0)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = datetime.now(timezone.utc)
        if exc_type is not None:
            self.status = "Failure"
            self.error_message = f"{exc_type.__name__}: {str(exc_val)}"
        else:
            self.status = "Success"
            self.error_message = None

        self._persist_log()
        return False

    def _persist_log(self):
        try:
            log_data = [(
                self.log_id,
                self.layer,
                self.parameter,
                self.start_time,
                self.end_time,
                self.status,
                self.rows_inserted,
                self.rows_updated,
                self.error_message,
            )]
            df = self.spark.createDataFrame(log_data, schema=AUDIT_LOG_SCHEMA)
            df.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE_NAME)
        except Exception as e:
            print(f"Failed to write audit log: {e}")
