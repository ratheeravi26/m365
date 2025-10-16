# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold build – Azure AD group entitlements
# MAGIC
# MAGIC Generates user entitlements derived from Azure AD group memberships by joining the enriched permissions
# MAGIC staging data with the silver AAD group dimension tables.

# COMMAND ----------

import json
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.environment import configure_environment, validate_run_mode
from shared.gold_transformations import build_aad_group_entitlements

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

dbutils.widgets.text("snapshot_partitions", "")
snapshot_partitions_raw = dbutils.widgets.get("snapshot_partitions")
SNAPSHOT_PARTITIONS: List[str] = [p for p in snapshot_partitions_raw.split(",") if p] if snapshot_partitions_raw else []

if not SNAPSHOT_PARTITIONS:
    dbutils.notebook.exit(json.dumps([{"status": "skipped", "reason": "No snapshot partitions supplied."}]))

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

permissions_enriched_path = f"{STAGING_BASE_PATH}/{TABLE_PREFIX or 'default'}/permissions_enriched"
aad_groups_path = f"{SILVER_BASE_PATH}/aad_groups"
aad_group_members_path = f"{SILVER_BASE_PATH}/aad_group_members"
staging_path = f"{STAGING_BASE_PATH}/{TABLE_PREFIX or 'default'}/aad_group_entitlements"

permissions_enriched_df = (
    spark.read.format("delta")
    .load(permissions_enriched_path)
    .filter(F.col("snapshot_date").isin(SNAPSHOT_PARTITIONS))
)

aad_groups_df = (
    spark.read.format("delta")
    .load(aad_groups_path)
    .filter(F.col("snapshot_date").isin(SNAPSHOT_PARTITIONS))
)

aad_group_members_df = (
    spark.read.format("delta")
    .load(aad_group_members_path)
    .filter(F.col("snapshot_date").isin(SNAPSHOT_PARTITIONS))
)

aad_entitlements_df = build_aad_group_entitlements(
    permissions_enriched_df, aad_group_members_df, aad_groups_df
)

(
    aad_entitlements_df.write.format("delta")
    .mode("overwrite" if RUN_MODE == "full" else "overwrite")
    .partitionBy("snapshot_date_key")
    .save(staging_path)
)

result = {
    "dataset": "aad_group_entitlements",
    "status": "loaded",
    "rows_written": aad_entitlements_df.count(),
    "target_path": staging_path,
    "partitions_processed": ",".join(SNAPSHOT_PARTITIONS),
}

# COMMAND ----------

dbutils.notebook.exit(json.dumps([result]))
