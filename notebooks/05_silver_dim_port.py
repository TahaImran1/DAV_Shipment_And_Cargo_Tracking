import os
import re
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim,
    concat,
    current_timestamp,
    row_number,
    udf,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from audit_logger import PipelineLogger

SOURCE = "workspace.bronze.raw_wpi_ports"
TARGET = "workspace.silver.dim_port"


def parse_dms(s):
    if not s:
        return None
    s = str(s).strip()
    dir_match = re.search(r"[NSEWnsew]", s)
    direction = dir_match.group(0).upper() if dir_match else ""
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    if not nums:
        try:
            return float(s)
        except Exception:
            return None
    deg = float(nums[0])
    minute = float(nums[1]) if len(nums) > 1 else 0.0
    sec = float(nums[2]) if len(nums) > 2 else 0.0
    dec = deg + (minute / 60.0) + (sec / 3600.0)
    if direction in ("S", "W"):
        dec = -dec
    return round(dec, 6)


parse_dms_udf = udf(parse_dms, DoubleType())


def merge_into(df, target, keys):
    if not spark.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)

    target_table = DeltaTable.forName(spark, target)
    join_condition = " AND ".join(f"target.{k} = source.{k}" for k in keys)

    (
        target_table.alias("target")
        .merge(df.alias("source"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    m = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()["operationMetrics"]
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))


with PipelineLogger(spark, layer="silver", parameter="dim_port") as logger:
    unlo = trim(col("unloCode"))
    port_code = when(unlo.isNotNull() & (unlo != ""), unlo).otherwise(
        concat(lit("WPI_"), col("portNumber"))
    )

    ports = (
        spark.table(SOURCE)
        .withColumn("port_code", port_code)
        .select(
            "port_code",
            col("portNumber").alias("port_number"),
            trim(col("portName")).alias("port_name"),
            col("countryName").alias("country"),
            col("countryCode").alias("country_code"),
            parse_dms_udf("latitude").alias("latitude"),
            parse_dms_udf("longitude").alias("longitude"),
            col("harborSize").alias("harbor_size"),
            col("harborType").alias("harbor_type"),
            current_timestamp().alias("load_timestamp"),
        )
    )

    w = Window.partitionBy("port_code").orderBy("port_number")
    ports = ports.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")

    inserted, updated = merge_into(ports, TARGET, ["port_code"])
    logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
    total = spark.table(TARGET).count()
    print(f"MERGE completed on {TARGET}: {inserted} inserted, {updated} updated (total: {total})")

    spark.sql(f"""
        SELECT port_code, COUNT(*) AS occurrences
        FROM {TARGET}
        GROUP BY port_code
        HAVING COUNT(*) > 1
    """).show()

    spark.sql(f"""
        SELECT COUNT(*) AS total_records
        FROM {TARGET}
    """).show()

