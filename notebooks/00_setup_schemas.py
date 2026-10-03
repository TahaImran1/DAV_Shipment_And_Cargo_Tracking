# Databricks notebook source
# COMMAND ----------
"""
Notebook: 00_setup_schemas
Purpose:
  1. Create the 4 Medallion schemas:
     - bronze: raw source tables and schema drift quarantine
     - silver: conformed star schema (fact_vessel_position, dim_vessel, dim_port, dim_voyage)
     - gold: business aggregation summary tables
     - maritime_ops: audit logs, pipeline watermarks, execution metrics
  2. Create bronze.quarantine table for non-conforming or corrupted records
  3. Initialize maritime_ops.pipeline_execution_logs
"""

# COMMAND ----------
# MAGIC %run ./99_audit_logger

# COMMAND ----------
with PipelineLogger(spark, layer="Platform-Setup", parameter="all_schemas") as logger:
    # 1. Create Databricks Schemas
    schemas = ["bronze", "silver", "gold", "maritime_ops"]
    for s in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")
        print(f"Verified schema: {s}")

    # 2. Create Bronze Quarantine Table for Schema Drift & Corrupted Records
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

    # 3. Create Audit Log Table
    init_audit_table(spark)
    print("Verified table: maritime_ops.pipeline_execution_logs")

    logger.set_metrics(rows_inserted=1, rows_updated=0)
    print("Platform setup completed successfully.")
