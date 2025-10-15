# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver transformation – SharePoint permissions
# MAGIC
# MAGIC This notebook explodes and normalises the SharePointPermissions_v1 bronze dataset into the
# MAGIC silver `sharepoint_permissions` Delta table.

# COMMAND ----------

import json
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shared.environment import configure_environment, ensure_catalog_schema_pair, validate_run_mode
from shared.bronze_ingest import build_target_path
from shared.silver_utils import list_partitions, read_partitioned_delta, resolve_target_partitions, write_silver_delta

spark: SparkSession

# COMMAND ----------

ENV_CONFIG: Dict[str, Dict[str, str]] = {
    "dev": {
        "bronze_base_path": "abfss://bronze@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint",
        "silver_base_path": "abfss://silver@ubsadlsdev.dfs.core.windows.net/mgdc/sharepoint",
        "catalog_name": "ubs_entitlements_dev",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp_dev",
    },
    "uat": {
        "bronze_base_path": "abfss://bronze@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint",
        "silver_base_path": "abfss://silver@ubsadlsuat.dfs.core.windows.net/mgdc/sharepoint",
        "catalog_name": "ubs_entitlements_uat",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp_uat",
    },
    "prod": {
        "bronze_base_path": "abfss://bronze@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint",
        "silver_base_path": "abfss://silver@ubsadlsprod.dfs.core.windows.net/mgdc/sharepoint",
        "catalog_name": "ubs_entitlements_prod",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp",
    },
}

WIDGET_DEFAULTS: Dict[str, str] = {
    "bronze_base_path": "/mnt/bronze/mgdc/sharepoint",
    "silver_base_path": "/mnt/silver/mgdc/sharepoint",
    "catalog_name": "",
    "schema_name": "",
    "run_mode": "incremental",
    "table_prefix": "mgdc_sp",
}

resolved_widgets = configure_environment(ENV_CONFIG, WIDGET_DEFAULTS)

BRONZE_BASE_PATH = resolved_widgets["bronze_base_path"].rstrip("/")
SILVER_BASE_PATH = resolved_widgets["silver_base_path"].rstrip("/")
CATALOG_NAME = resolved_widgets["catalog_name"].strip()
SCHEMA_NAME = resolved_widgets["schema_name"].strip()
RUN_MODE = validate_run_mode(resolved_widgets["run_mode"])
TABLE_PREFIX = resolved_widgets["table_prefix"].strip()

ensure_catalog_schema_pair(CATALOG_NAME, SCHEMA_NAME)

# COMMAND ----------

def transform_sharepoint_permissions(df: DataFrame) -> DataFrame:
    exploded_df = (
        df.withColumn("shared_with_entry", F.explode_outer("SharedWith"))
        .withColumn("shared_with_count_entry", F.explode_outer("SharedWithCount"))
    )

    transformed = (
        exploded_df.select(
            F.col("SiteId").alias("site_id"),
            F.col("WebId").alias("web_id"),
            F.col("ListId").alias("list_id"),
            F.col("ListTitle").alias("list_title"),
            F.col("ListItemId").alias("list_item_id"),
            F.col("UniqueId").alias("unique_item_id"),
            F.col("ItemType").alias("item_type"),
            F.col("ItemURL").alias("item_url"),
            F.col("FileExtension").alias("file_extension"),
            F.col("RoleDefinition").alias("role_definition"),
            F.col("LinkId").alias("link_id"),
            F.col("ScopeId").alias("scope_id"),
            F.col("LinkScope").alias("link_scope"),
            F.col("TotalUserCount").alias("total_user_count"),
            F.col("ShareCreatedBy").alias("share_created_by"),
            F.col("ShareCreatedTime").alias("share_created_datetime"),
            F.col("ShareLastModifiedBy").alias("share_last_modified_by"),
            F.col("ShareLastModifiedTime").alias("share_last_modified_datetime"),
            F.col("ShareExpirationTime").alias("share_expiration_datetime"),
            F.col("snapshot_date"),
            F.col("shared_with_entry.Type").alias("principal_type"),
            F.col("shared_with_entry.TypeV2").alias("principal_type_v2"),
            F.col("shared_with_entry.Name").alias("principal_display_name"),
            F.col("shared_with_entry.Email").alias("principal_email"),
            F.col("shared_with_entry.AadObjectId").alias("principal_aad_object_id"),
            F.col("shared_with_entry.UPN").alias("principal_upn"),
            F.col("shared_with_entry.UserLoginName").alias("principal_login_name"),
            F.col("shared_with_entry.UserCount").alias("principal_user_count"),
            F.col("shared_with_count_entry.Type").alias("principal_scope_type"),
            F.col("shared_with_count_entry.Count").alias("principal_scope_count"),
        )
        .withColumn(
            "principal_group_key",
            F.lower(
                F.coalesce(
                    F.col("principal_login_name"),
                    F.col("principal_display_name"),
                )
            ),
        )
        .withColumn("snapshot_date", F.col("snapshot_date").cast("string"))
        .dropDuplicates(
            [
                "site_id",
                "list_id",
                "list_item_id",
                "principal_email",
                "principal_aad_object_id",
                "principal_login_name",
                "role_definition",
                "snapshot_date",
            ]
        )
    )
    return transformed


# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

bronze_path = f"{BRONZE_BASE_PATH}/sharepoint_permissions"
target_path = build_target_path(SILVER_BASE_PATH, "sharepoint_permissions")

bronze_partitions = list_partitions(bronze_path, "snapshot_date")
partitions_to_process = resolve_target_partitions(bronze_partitions, target_path, "snapshot_date", RUN_MODE)

if partitions_to_process:
    source_df = read_partitioned_delta(bronze_path, "snapshot_date", partitions_to_process)
    transformed_df = transform_sharepoint_permissions(source_df)
    write_result = write_silver_delta(
        transformed_df,
        target_path,
        "snapshot_date",
        RUN_MODE,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        table_prefix=TABLE_PREFIX,
        output_alias="sharepoint_permissions",
    )
else:
    write_result = {"status": "skipped", "rows_written": 0, "target_path": target_path, "table_name": ""}

write_result["partitions_processed"] = ",".join(partitions_to_process)
write_result["dataset"] = "sharepoint_permissions"

audit_df = spark.createDataFrame([write_result])

# COMMAND ----------

display(audit_df)

# COMMAND ----------

dbutils.notebook.exit(json.dumps([write_result]))
