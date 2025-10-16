from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from shared.bronze_ingest import build_table_name


def list_partitions(path: str, partition_key: str) -> List[str]:
    try:
        df = spark.read.format("delta").load(path).select(partition_key).dropna().distinct()
    except Exception:
        return []
    return sorted([row[partition_key] for row in df.collect()])


def resolve_target_partitions(
    source_partitions: List[str],
    target_path: str,
    partition_key: str,
    run_mode: str,
) -> List[str]:
    if run_mode == "full":
        return source_partitions
    existing = list_partitions(target_path, partition_key)
    existing_set = set(existing)
    return [partition for partition in source_partitions if partition not in existing_set]


def read_partitioned_delta(
    path: str,
    partition_key: str,
    partitions: List[str],
) -> DataFrame:
    if not partitions:
        try:
            schema = spark.read.format("delta").load(path).schema
        except Exception:
            schema = T.StructType([])
        return spark.createDataFrame([], schema=schema)
    return (
        spark.read.format("delta")
        .load(path)
        .filter(F.col(partition_key).isin(partitions))
    )


def write_silver_delta(
    df: DataFrame,
    target_path: str,
    partition_key: str,
    run_mode: str,
    *,
    catalog_name: str,
    schema_name: str,
    table_prefix: str,
    output_alias: str,
) -> Dict[str, str]:
    if df.rdd.isEmpty():
        return {"status": "skipped", "rows_written": 0}

    table_name = build_table_name(catalog_name, schema_name, table_prefix, output_alias)

    df_with_metadata = df.withColumn("silver_loaded_on_utc", F.current_timestamp())
    row_count = df_with_metadata.count()

    storage_mode = "append" if run_mode == "incremental" else "overwrite"
    (
        df_with_metadata.write.format("delta")
        .mode(storage_mode)
        .partitionBy(partition_key)
        .save(target_path)
    )

    if table_name:
        table_mode = "append" if run_mode == "incremental" else "overwrite"
        (
            df_with_metadata.write.format("delta")
            .mode(table_mode)
            .partitionBy(partition_key)
            .saveAsTable(table_name)
        )
    return {
        "status": "loaded",
        "rows_written": row_count,
        "target_path": target_path,
        "table_name": table_name or "",
    }
