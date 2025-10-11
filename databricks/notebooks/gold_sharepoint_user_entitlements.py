# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold orchestration – SharePoint user entitlements
# MAGIC
# MAGIC Executes modular notebooks to prepare enriched permissions, derive entitlement slices, and assemble
# MAGIC the final SharePoint user entitlement Delta table.

# COMMAND ----------

import json
from typing import Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.environment import configure_environment, ensure_catalog_schema_pair, validate_run_mode
from shared.bronze_ingest import exit_with_results
from shared.gold_transformations import append_lineage
from shared.gold_utils import filter_partitions_to_process, write_gold_delta
from shared.silver_utils import list_partitions

spark: SparkSession

# COMMAND ----------

ENV_CONFIG: Dict[str, Dict[str, str]] = {
    "dev": {
        "silver_base_path": "abfss://silver@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint",
        "gold_base_path": "abfss://gold@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint",
        "catalog_name": "ubs_entitlements_dev",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp_dev",
    },
    "uat": {
        "silver_base_path": "abfss://silver@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint",
        "gold_base_path": "abfss://gold@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint",
        "catalog_name": "ubs_entitlements_uat",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp_uat",
    },
    "prod": {
        "silver_base_path": "abfss://silver@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint",
        "gold_base_path": "abfss://gold@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint",
        "catalog_name": "ubs_entitlements_prod",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp",
    },
}

WIDGET_DEFAULTS: Dict[str, str] = {
    "silver_base_path": "/mnt/silver/mgdc/sharepoint",
    "gold_base_path": "/mnt/gold/mgdc/sharepoint",
    "catalog_name": "",
    "schema_name": "",
    "run_mode": "incremental",
    "table_name": "sharepoint_user_entitlements",
    "table_prefix": "mgdc_sp",
}

resolved_widgets = configure_environment(ENV_CONFIG, WIDGET_DEFAULTS)

SILVER_BASE_PATH = resolved_widgets["silver_base_path"].rstrip("/")
GOLD_BASE_PATH = resolved_widgets["gold_base_path"].rstrip("/")
CATALOG_NAME = resolved_widgets["catalog_name"].strip()
SCHEMA_NAME = resolved_widgets["schema_name"].strip()
RUN_MODE = validate_run_mode(resolved_widgets["run_mode"])
TABLE_NAME = resolved_widgets["table_name"].strip() or "sharepoint_user_entitlements"
TABLE_PREFIX = resolved_widgets["table_prefix"].strip()

ensure_catalog_schema_pair(CATALOG_NAME, SCHEMA_NAME)

# COMMAND ----------

def build_gold_path(table_name: str) -> str:
    prefix = f"{TABLE_PREFIX}_" if TABLE_PREFIX else ""
    return f"{GOLD_BASE_PATH}/{prefix}{table_name}"


def build_table_identifier(table_name: str) -> Optional[str]:
    if not (CATALOG_NAME and SCHEMA_NAME):
        return None
    prefix = f"{TABLE_PREFIX}_" if TABLE_PREFIX else ""
    return f"{CATALOG_NAME}.{SCHEMA_NAME}.{prefix}{table_name}"


permissions_path = f"{SILVER_BASE_PATH}/sharepoint_permissions"
gold_path = build_gold_path(TABLE_NAME)
staging_base_path = f"{GOLD_BASE_PATH}/staging"
staging_prefix = TABLE_PREFIX or "default"

available_partitions = list_partitions(permissions_path, "snapshot_date")
existing_partitions = list_partitions(gold_path, "snapshot_date_key")
partitions_to_process = filter_partitions_to_process(available_partitions, existing_partitions, RUN_MODE)

if not partitions_to_process:
    empty_df = spark.createDataFrame([], "snapshot_date_key string, status string")
    display(empty_df)
    dbutils.notebook.exit("No new partitions to process.")

snapshot_partition_csv = ",".join(partitions_to_process)

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

child_arguments = {
    "environment": resolved_widgets["environment"],
    "silver_base_path": SILVER_BASE_PATH,
    "staging_base_path": staging_base_path,
    "run_mode": RUN_MODE,
    "table_prefix": TABLE_PREFIX,
    "snapshot_partitions": snapshot_partition_csv,
}

child_notebooks = [
    "./gold/prepare_permissions_base",
    "./gold/build_direct_entitlements",
    "./gold/build_sharepoint_group_entitlements",
    "./gold/build_aad_group_entitlements",
]

child_results: List[Dict[str, object]] = []
for path in child_notebooks:
    response = dbutils.notebook.run(path, 0, child_arguments)
    child_results.extend(json.loads(response))

# COMMAND ----------

staging_direct = f"{staging_base_path.rstrip('/')}/{staging_prefix}/direct_entitlements"
staging_sp_group = f"{staging_base_path.rstrip('/')}/{staging_prefix}/sharepoint_group_entitlements"
staging_aad_group = f"{staging_base_path.rstrip('/')}/{staging_prefix}/aad_group_entitlements"
users_dim_path = f"{SILVER_BASE_PATH}/graph_users_dim"

direct_df = spark.read.format("delta").load(staging_direct).filter(F.col("snapshot_date_key").isin(partitions_to_process))
sp_group_df = spark.read.format("delta").load(staging_sp_group).filter(F.col("snapshot_date_key").isin(partitions_to_process))
aad_group_df = spark.read.format("delta").load(staging_aad_group).filter(F.col("snapshot_date_key").isin(partitions_to_process))
users_dim_df = (
    spark.read.format("delta")
    .load(users_dim_path)
    .filter(F.col("snapshot_date").isin(partitions_to_process))
    .dropDuplicates(["user_aad_object_id", "snapshot_date"])
)

union_df = direct_df.unionByName(sp_group_df, allowMissingColumns=True).unionByName(
    aad_group_df, allowMissingColumns=True
)
enriched_union_df = (
    union_df.alias("ent")
    .join(
        users_dim_df.select(
            "user_aad_object_id",
            "snapshot_date",
            "user_job_title",
            "user_department",
            "user_office_location",
            "user_account_enabled",
            "user_company_name",
            "user_country",
            "user_city",
            "user_state",
        ).alias("usr"),
        (F.col("ent.user_aad_object_id") == F.col("usr.user_aad_object_id"))
        & (F.col("usr.snapshot_date") == F.col("ent.snapshot_date_key")),
        "left",
    )
    .select(
        "ent.*",
        F.col("usr.user_job_title").alias("user_job_title"),
        F.col("usr.user_department").alias("user_department"),
        F.col("usr.user_office_location").alias("user_office_location"),
        F.col("usr.user_account_enabled").alias("user_account_enabled"),
        F.col("usr.user_company_name").alias("user_company_name"),
        F.col("usr.user_country").alias("user_country"),
        F.col("usr.user_city").alias("user_city"),
        F.col("usr.user_state").alias("user_state"),
    )
)

final_df = append_lineage(enriched_union_df)

table_identifier = build_table_identifier(TABLE_NAME)
write_gold_delta(final_df, gold_path, "snapshot_date_key", table_identifier)

audit_df = final_df.groupBy("snapshot_date_key", "entitlement_source").count()

# COMMAND ----------

display(audit_df)

# COMMAND ----------

result_summary = [
    {"dataset": "gold_sharepoint_user_entitlements", "status": "loaded", "target_path": gold_path, "table_name": table_identifier or "", "partitions_processed": snapshot_partition_csv, "rows_written": final_df.count()}
]

exit_with_results(child_results + result_summary)
