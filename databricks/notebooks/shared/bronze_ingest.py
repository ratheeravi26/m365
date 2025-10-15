import json
import re
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_target_path(base_path: str, output_alias: str) -> str:
    return f"{base_path.rstrip('/')}/{output_alias}"


def build_table_name(
    catalog_name: str,
    schema_name: str,
    table_prefix: str,
    output_alias: str,
) -> Optional[str]:
    catalog_name = catalog_name.strip()
    schema_name = schema_name.strip()
    table_prefix = table_prefix.strip()
    if not (catalog_name and schema_name):
        return None
    prefix = f"{table_prefix}_" if table_prefix else ""
    return f"{catalog_name}.{schema_name}.{prefix}{output_alias}"


def _list_subdirs(path: str) -> List[str]:
    try:
        return [entry for entry in dbutils.fs.ls(path)]
    except Exception:
        return []


def list_available_partitions(source_path: str) -> List[str]:
    dates: List[str] = []
    year_pattern = re.compile(r"year=(\d{4})/?$")
    month_pattern = re.compile(r"month=(\d{2})/?$")
    day_pattern = re.compile(r"day=(\d{2})/?$")

    for year_entry in _list_subdirs(source_path):
        year_match = year_pattern.match(year_entry.name)
        if not year_match:
            continue
        year = year_match.group(1)
        for month_entry in _list_subdirs(year_entry.path):
            month_match = month_pattern.match(month_entry.name)
            if not month_match:
                continue
            month = month_match.group(1)
            for day_entry in _list_subdirs(month_entry.path):
                day_match = day_pattern.match(day_entry.name)
                if not day_match:
                    continue
                day = day_match.group(1)
                dates.append(f"{year}{month}{day}")

    return sorted(set(dates))


def list_loaded_partitions(target_path: str, partition_key: str) -> List[str]:
    try:
        df = (
            spark.read.format("delta")
            .load(target_path)
            .select(partition_key)
            .dropna()
            .distinct()
        )
    except Exception:
        return []
    return sorted(row[partition_key] for row in df.collect())


def resolve_partitions_to_load(
    available_partitions: List[str],
    loaded_partitions: List[str],
    run_mode: str,
) -> List[str]:
    if run_mode == "full":
        return available_partitions
    loaded = set(loaded_partitions)
    return [partition for partition in available_partitions if partition not in loaded]


def read_json_with_partition(
    source_path: str,
    partition_key: str,
    partitions: List[str],
) -> DataFrame:
    if not partitions:
        return spark.createDataFrame([], schema=T.StructType([]))

    partition_paths = [
        f"{source_path}/year={value[:4]}/month={value[4:6]}/day={value[6:8]}"
        for value in partitions
    ]
    df = (
        spark.read.json(partition_paths)
        .withColumn("_file_path", F.input_file_name())
        .withColumn(
            partition_key,
            F.concat(
                F.regexp_extract(F.col("_file_path"), r"year=(\d{4})", 1),
                F.regexp_extract(F.col("_file_path"), r"month=(\d{2})", 1),
                F.regexp_extract(F.col("_file_path"), r"day=(\d{2})", 1),
            ),
        )
        .withColumn(
            "snapshot_timestamp_utc",
            F.to_timestamp(
                F.coalesce(
                    F.col("SnapshotDate"),
                    F.col("snapshotDate"),
                    F.concat_ws(
                        "-",
                        F.substring(F.col(partition_key), 1, 4),
                        F.substring(F.col(partition_key), 5, 2),
                        F.substring(F.col(partition_key), 7, 2),
                    ),
                )
            ),
        )
        .withColumn(
            partition_key,
            F.coalesce(
                F.col(partition_key),
                F.date_format(F.col("snapshot_timestamp_utc"), "yyyyMMdd"),
            ),
        )
        .drop("_file_path")
    )
    return df


def write_delta(
    df: DataFrame,
    target_path: str,
    partition_key: str,
    table_name: Optional[str],
) -> Dict[str, str]:
    if df.rdd.isEmpty():
        return {"status": "skipped", "reason": "No new partitions detected"}

    df_with_loaded_ts = df.withColumn("bronze_loaded_on_utc", F.current_timestamp())

    (
        df_with_loaded_ts.write.format("delta")
        .mode("append")
        .partitionBy(partition_key)
        .save(target_path)
    )

    rows_written = df.count()

    if table_name:
        (
            df_with_loaded_ts.write.format("delta")
            .mode("append")
            .partitionBy(partition_key)
            .saveAsTable(table_name)
        )

    return {
        "status": "loaded",
        "rows_written": str(rows_written),
        "target_path": target_path,
        "table_name": table_name or "",
    }


def ingest_dataset(
    dataset_conf: Dict[str, str],
    *,
    source_base_path: str,
    bronze_base_path: str,
    run_mode: str,
    catalog_name: str,
    schema_name: str,
    table_prefix: str,
) -> Dict[str, str]:
    dataset_name = dataset_conf["dataset_name"]
    output_alias = dataset_conf["output_alias"]
    partition_key = dataset_conf["partition_key"]

    source_path = f"{source_base_path.rstrip('/')}/{dataset_name}"
    target_path = build_target_path(bronze_base_path, output_alias)
    table_name = build_table_name(catalog_name, schema_name, table_prefix, output_alias)

    available_partitions = list_available_partitions(source_path)
    loaded_partitions = list_loaded_partitions(target_path, partition_key)
    partitions_to_load = resolve_partitions_to_load(available_partitions, loaded_partitions, run_mode)

    df = read_json_with_partition(source_path, partition_key, partitions_to_load)
    write_result = write_delta(df, target_path, partition_key, table_name)

    return {
        "dataset_name": dataset_name,
        "output_alias": output_alias,
        **write_result,
        "partitions_loaded": ",".join(partitions_to_load),
    }


def exit_with_results(results: List[Dict[str, str]]) -> None:
    """
    Helper for child notebooks to return results to an orchestrator.
    """
    dbutils.notebook.exit(json.dumps(results))
