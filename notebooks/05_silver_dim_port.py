import os
import re
import sys

try:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    pass

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
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


parse_dms_udf = F.udf(parse_dms, DoubleType())


def merge_into(df, target, keys):
    df.createOrReplaceTempView("src")
    if not spark.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""
        MERGE INTO {target} t USING src s ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    m = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()["operationMetrics"]
    return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))


with PipelineLogger(spark, layer="silver", parameter="dim_port") as logger:
    unlo = F.trim(F.col("unloCode"))
    port_code = F.when(unlo.isNotNull() & (unlo != ""), unlo).otherwise(
        F.concat(F.lit("WPI_"), F.col("portNumber"))
    )

    ports = (
        spark.table(SOURCE)
        .withColumn("port_code", port_code)
        .select(
            "port_code",
            F.col("portNumber").alias("port_number"),
            F.trim("portName").alias("port_name"),
            F.col("countryName").alias("country"),
            F.col("countryCode").alias("country_code"),
            parse_dms_udf("latitude").alias("latitude"),
            parse_dms_udf("longitude").alias("longitude"),
            F.col("harborSize").alias("harbor_size"),
            F.col("harborType").alias("harbor_type"),
            F.current_timestamp().alias("load_timestamp"),
        )
    )

    # a few ports can share a UN/LOCODE, keep the lowest port number
    w = Window.partitionBy("port_code").orderBy("port_number")
    ports = ports.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    inserted, updated = merge_into(ports, TARGET, ["port_code"])
    logger.set_metrics(rows_inserted=inserted, rows_updated=updated)
