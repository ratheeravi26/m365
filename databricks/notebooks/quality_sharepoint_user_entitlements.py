# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quality checks for SharePoint user entitlement pipeline
# MAGIC
# MAGIC Run this notebook after the gold layer load to confirm schema coverage, detect duplicate keys, and validate basic row counts across bronze, silver, and gold layers.

# COMMAND ----------

from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

ENV_CONFIG: Dict[str, Dict[str, str]] = {
    "dev": {
        "bronze_base_path": "abfss://bronze@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint",
        "silver_base_path": "abfss://silver@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint",
        "gold_base_path": "abfss://gold@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint",
        "catalog_name": "ubs_entitlements_dev",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp_dev",
    },
    "uat": {
        "bronze_base_path": "abfss://bronze@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint",
        "silver_base_path": "abfss://silver@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint",
        "gold_base_path": "abfss://gold@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint",
        "catalog_name": "ubs_entitlements_uat",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp_uat",
    },
    "prod": {
        "bronze_base_path": "abfss://bronze@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint",
        "silver_base_path": "abfss://silver@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint",
        "gold_base_path": "abfss://gold@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint",
        "catalog_name": "ubs_entitlements_prod",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp",
    },
}

dbutils.widgets.dropdown("environment", "dev", sorted(ENV_CONFIG.keys()))
ENVIRONMENT = dbutils.widgets.get("environment")
ENV_DEFAULTS = ENV_CONFIG.get(ENVIRONMENT, {})

dbutils.widgets.text(
    "bronze_base_path",
    ENV_DEFAULTS.get("bronze_base_path", "/mnt/bronze/mgdc/sharepoint"),
)
dbutils.widgets.text(
    "silver_base_path",
    ENV_DEFAULTS.get("silver_base_path", "/mnt/silver/mgdc/sharepoint"),
)
dbutils.widgets.text(
    "gold_base_path",
    ENV_DEFAULTS.get("gold_base_path", "/mnt/gold/mgdc/sharepoint"),
)
dbutils.widgets.text(
    "catalog_name",
    ENV_DEFAULTS.get("catalog_name", ""),
)
dbutils.widgets.text(
    "schema_name",
    ENV_DEFAULTS.get("schema_name", ""),
)
dbutils.widgets.text(
    "table_prefix",
    ENV_DEFAULTS.get("table_prefix", "mgdc_sp"),
)
dbutils.widgets.text("table_name", "sharepoint_user_entitlements")

BRONZE_BASE_PATH = (
    dbutils.widgets.get("bronze_base_path").strip()
    or ENV_DEFAULTS.get("bronze_base_path", "/mnt/bronze/mgdc/sharepoint")
).rstrip("/")
SILVER_BASE_PATH = (
    dbutils.widgets.get("silver_base_path").strip()
    or ENV_DEFAULTS.get("silver_base_path", "/mnt/silver/mgdc/sharepoint")
).rstrip("/")
GOLD_BASE_PATH = (
    dbutils.widgets.get("gold_base_path").strip()
    or ENV_DEFAULTS.get("gold_base_path", "/mnt/gold/mgdc/sharepoint")
).rstrip("/")
CATALOG_NAME = dbutils.widgets.get("catalog_name").strip() or ENV_DEFAULTS.get("catalog_name", "")
SCHEMA_NAME = dbutils.widgets.get("schema_name").strip() or ENV_DEFAULTS.get("schema_name", "")
TABLE_PREFIX = dbutils.widgets.get("table_prefix").strip() or ENV_DEFAULTS.get("table_prefix", "mgdc_sp")
TABLE_NAME = dbutils.widgets.get("table_name").strip() or "sharepoint_user_entitlements"
TABLE_PATH_PREFIX = f"{TABLE_PREFIX}_" if TABLE_PREFIX else ""
GOLD_TABLE_PATH = f"{GOLD_BASE_PATH}/{TABLE_PATH_PREFIX}{TABLE_NAME}"

# COMMAND ----------

def load_delta(path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def expected_columns_met(df: DataFrame, expected_columns: List[str]) -> bool:
    actual_cols = set(df.columns)
    missing = [c for c in expected_columns if c not in actual_cols]
    return len(missing) == 0


def detect_duplicates(df: DataFrame, key_columns: List[str]) -> int:
    if not key_columns:
        return 0
    duplicates = (
        df.groupBy([F.col(c) for c in key_columns])
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    return duplicates


def record_check(results: List[Dict[str, str]], name: str, status: str, details: str) -> None:
    results.append({"check_name": name, "status": status, "details": details})


# COMMAND ----------

checks: List[Dict[str, str]] = []

# Bronze level row count comparison (permissions as proxy for entitlement volume)
bronze_permissions = load_delta(f"{BRONZE_BASE_PATH}/sharepoint_permissions")
bronze_count = bronze_permissions.count()
record_check(
    checks,
    "bronze_sharepoint_permissions_rowcount",
    "pass" if bronze_count > 0 else "fail",
    f"rows={bronze_count}",
)

# Silver schema validations
sharepoint_permissions_silver = load_delta(f"{SILVER_BASE_PATH}/sharepoint_permissions")
expected_perm_columns = [
    "site_id",
    "list_id",
    "list_item_id",
    "principal_email",
    "principal_aad_object_id",
    "principal_login_name",
    "role_definition",
    "snapshot_date",
]
schema_ok = expected_columns_met(sharepoint_permissions_silver, expected_perm_columns)
record_check(
    checks,
    "silver_sharepoint_permissions_schema",
    "pass" if schema_ok else "fail",
    f"missing_columns={[c for c in expected_perm_columns if c not in sharepoint_permissions_silver.columns]}",
)

perm_duplicates = detect_duplicates(
    sharepoint_permissions_silver,
    [
        "site_id",
        "list_id",
        "list_item_id",
        "principal_aad_object_id",
        "principal_login_name",
        "role_definition",
        "snapshot_date",
    ],
)
record_check(
    checks,
    "silver_sharepoint_permissions_duplicates",
    "pass" if perm_duplicates == 0 else "fail",
    f"duplicate_groups={perm_duplicates}",
)

sharepoint_group_members = load_delta(f"{SILVER_BASE_PATH}/sharepoint_group_members")
group_member_duplicates = detect_duplicates(
    sharepoint_group_members,
    ["sp_group_id", "member_aad_object_id", "snapshot_date"],
)
record_check(
    checks,
    "silver_sharepoint_group_members_duplicates",
    "pass" if group_member_duplicates == 0 else "fail",
    f"duplicate_groups={group_member_duplicates}",
)

aad_group_members = load_delta(f"{SILVER_BASE_PATH}/aad_group_members")
aad_member_duplicates = detect_duplicates(
    aad_group_members,
    ["aad_group_id", "member_aad_object_id", "snapshot_date"],
)
record_check(
    checks,
    "silver_aad_group_members_duplicates",
    "pass" if aad_member_duplicates == 0 else "fail",
    f"duplicate_groups={aad_member_duplicates}",
)

# Gold level checks
gold_entitlements = load_delta(GOLD_TABLE_PATH)
gold_snapshot_counts = (
    gold_entitlements.groupBy("snapshot_date_key", "entitlement_source").count().orderBy("snapshot_date_key")
)
# flag if any snapshot has zero rows
empty_snapshots = (
    gold_snapshot_counts.groupBy("snapshot_date_key").agg(F.sum("count").alias("total")).filter(F.col("total") == 0).count()
)
record_check(
    checks,
    "gold_snapshot_rowcount",
    "pass" if empty_snapshots == 0 else "fail",
    f"snapshots_zero={empty_snapshots}",
)

hash_duplicates = detect_duplicates(gold_entitlements, ["entitlement_hash"])
record_check(
    checks,
    "gold_entitlement_hash_uniqueness",
    "pass" if hash_duplicates == 0 else "fail",
    f"duplicate_hashes={hash_duplicates}",
)

checks_df = spark.createDataFrame(checks)

# COMMAND ----------

display(checks_df)

# COMMAND ----------

display(gold_snapshot_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Guidance
# MAGIC * All checks must pass before refreshing downstream marts or exporting to consumers.
# MAGIC * For failed checks, drill into the offending partitions using the snapshot keys surfaced in the results table.
