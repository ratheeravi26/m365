# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold build – direct SharePoint entitlements
# MAGIC
# MAGIC Consumes the enriched permissions staging dataset and emits direct user entitlements into a staging
# MAGIC Delta table for the final gold union.

# COMMAND ----------

import json
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.environment import configure_environment, validate_run_mode
from shared.gold_transformations import build_direct_entitlements

spark: SparkSession

# COMMAND ----------

ENV_CONFIG: Dict[str, Dict[str, str]] = {
    "dev": {
        "staging_base_path": "abfss://gold@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint/staging",
    },
    "uat": {
        "staging_base_path": "abfss://gold@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint/staging",
    },
    "prod": {
        "staging_base_path": "abfss://gold@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint/staging",
    },
}

WIDGET_DEFAULTS: Dict[str, str] = {
    "staging_base_path": "/mnt/gold/mgdc/sharepoint/staging",
    "run_mode": "incremental",
    "table_prefix": "mgdc_sp",
}

resolved_widgets = configure_environment(ENV_CONFIG, WIDGET_DEFAULTS)

STAGING_BASE_PATH = resolved_widgets["staging_base_path"].rstrip("/")
RUN_MODE = validate_run_mode(resolved_widgets["run_mode"])
TABLE_PREFIX = resolved_widgets["table_prefix"].strip()

snapshot_partitions_raw = dbutils.widgets.get("snapshot_partitions")
SNAPSHOT_PARTITIONS: List[str] = [p for p in snapshot_partitions_raw.split(",") if p] if snapshot_partitions_raw else []

if not SNAPSHOT_PARTITIONS:
    dbutils.notebook.exit(json.dumps([{"status": "skipped", "reason": "No snapshot partitions supplied."}]))

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

permissions_enriched_path = f"{STAGING_BASE_PATH}/{TABLE_PREFIX or 'default'}/permissions_enriched"
direct_staging_path = f"{STAGING_BASE_PATH}/{TABLE_PREFIX or 'default'}/direct_entitlements"

permissions_enriched_df = (
    spark.read.format("delta")
    .load(permissions_enriched_path)
    .filter(F.col("snapshot_date").isin(SNAPSHOT_PARTITIONS))
)

direct_df = build_direct_entitlements(permissions_enriched_df)

(
    direct_df.write.format("delta")
    .mode("overwrite" if RUN_MODE == "full" else "overwrite")
    .partitionBy("snapshot_date_key")
    .save(direct_staging_path)
)

result = {
    "dataset": "direct_entitlements",
    "status": "loaded",
    "rows_written": direct_df.count(),
    "target_path": direct_staging_path,
    "partitions_processed": ",".join(SNAPSHOT_PARTITIONS),
}

# COMMAND ----------

dbutils.notebook.exit(json.dumps([result]))
