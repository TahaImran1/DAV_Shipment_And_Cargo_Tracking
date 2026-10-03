try:
    import os, sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass
from audit_logger import PipelineLogger, init_audit_table

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

with PipelineLogger(spark, layer="Platform-Setup", parameter="all_schemas") as logger:
    for s in ["bronze", "silver", "gold", "maritime_ops"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")
        print(f"Verified schema: {s}")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.quarantine (
            quarantine_id STRING NOT NULL,
            source STRING NOT NULL,
            batch_id STRING,
            raw_payload STRING,
            error_reason STRING,
            quarantine_timestamp TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    print("Verified table: bronze.quarantine")

    init_audit_table(spark)
    print("Verified table: maritime_ops.pipeline_execution_logs")

    logger.set_metrics(rows_inserted=1, rows_updated=0)
    print("Platform setup completed successfully.")
