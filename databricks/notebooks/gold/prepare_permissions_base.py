# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold preparation – permissions enrichment
# MAGIC
# MAGIC Loads the silver SharePoint permissions and sites tables, enriches them, and writes the unified
# MAGIC dataset to a staging location for downstream entitlement notebooks.

# COMMAND ----------

import json
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.environment import configure_environment, validate_run_mode
from shared.gold_transformations import annotate_permissions

spark: SparkSession

# COMMAND ----------

ENV_CONFIG: Dict[str, Dict[str, str]] = {
    "dev": {
        "silver_base_path": "abfss://silver@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint",
        "staging_base_path": "abfss://gold@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint/staging",
    },
    "uat": {
        "silver_base_path": "abfss://silver@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint",
        "staging_base_path": "abfss://gold@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint/staging",
    },
    "prod": {
        "silver_base_path": "abfss://silver@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint",
        "staging_base_path": "abfss://gold@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint/staging",
    },
}

WIDGET_DEFAULTS: Dict[str, str] = {
    "silver_base_path": "/mnt/silver/mgdc/sharepoint",
    "staging_base_path": "/mnt/gold/mgdc/sharepoint/staging",
    "run_mode": "incremental",
    "table_prefix": "mgdc_sp",
}

resolved_widgets = configure_environment(ENV_CONFIG, WIDGET_DEFAULTS)

SILVER_BASE_PATH = resolved_widgets["silver_base_path"].rstrip("/")
STAGING_BASE_PATH = resolved_widgets["staging_base_path"].rstrip("/")
RUN_MODE = validate_run_mode(resolved_widgets["run_mode"])
TABLE_PREFIX = resolved_widgets["table_prefix"].strip()

snapshot_partitions_raw = dbutils.widgets.get("snapshot_partitions")
SNAPSHOT_PARTITIONS: List[str] = [p for p in snapshot_partitions_raw.split(",") if p] if snapshot_partitions_raw else []

if not SNAPSHOT_PARTITIONS:
    dbutils.notebook.exit(json.dumps([{"status": "skipped", "reason": "No snapshot partitions supplied."}]))

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

permissions_path = f"{SILVER_BASE_PATH}/sharepoint_permissions"
sites_path = f"{SILVER_BASE_PATH}/sharepoint_sites"
staging_path = f"{STAGING_BASE_PATH}/{TABLE_PREFIX or 'default'}/permissions_enriched"

permissions_df = (
    spark.read.format("delta")
    .load(permissions_path)
    .filter(F.col("snapshot_date").isin(SNAPSHOT_PARTITIONS))
)

sites_df = (
    spark.read.format("delta")
    .load(sites_path)
    .filter(F.col("snapshot_date").isin(SNAPSHOT_PARTITIONS))
)

permissions_enriched_df = annotate_permissions(permissions_df, sites_df)

(
    permissions_enriched_df.write.format("delta")
    .mode("overwrite" if RUN_MODE == "full" else "overwrite")
    .partitionBy("snapshot_date")
    .save(staging_path)
)

result = {
    "dataset": "permissions_enriched",
    "status": "loaded",
    "rows_written": permissions_enriched_df.count(),
    "target_path": staging_path,
    "partitions_processed": ",".join(SNAPSHOT_PARTITIONS),
}

# COMMAND ----------

dbutils.notebook.exit(json.dumps([result]))
