# Databricks notebook source
# Setup Schemas and Operational Tables (Lab 3 Convention)

print("Spark Version:", spark.version)

for schema in ["bronze", "silver", "gold", "maritime_ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{schema}")

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.bronze.quarantine (
        quarantine_id STRING,
        source STRING,
        batch_id STRING,
        raw_payload STRING,
        error_reason STRING,
        quarantine_timestamp TIMESTAMP
    ) USING DELTA
""")

# Keep these columns in sync with audit_logger.py
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.maritime_ops.pipeline_execution_logs (
        run_id STRING,
        layer STRING,
        parameter STRING,
        status STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_seconds DOUBLE,
        rows_inserted LONG,
        rows_updated LONG,
        error_message STRING
    ) USING DELTA
""")

print("Schemas and setup tables verified successfully.")

spark.sql("""
    SHOW SCHEMAS IN workspace
""").show()

