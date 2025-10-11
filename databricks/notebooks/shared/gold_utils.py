from typing import Iterable, List, Optional

from pyspark.sql import DataFrame

from shared.bronze_ingest import build_table_name
from shared.silver_utils import list_partitions


def filter_partitions_to_process(
    candidate_partitions: Iterable[str],
    existing_partitions: Iterable[str],
    run_mode: str,
) -> List[str]:
    candidates = sorted(set(candidate_partitions))
    if run_mode == "full":
        return candidates
    existing = set(existing_partitions)
    return [partition for partition in candidates if partition not in existing]


def write_gold_delta(
    df: DataFrame,
    target_path: str,
    partition_key: str,
    table_name: Optional[str],
) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy(partition_key)
        .save(target_path)
    )

    if table_name:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{target_path}'")
