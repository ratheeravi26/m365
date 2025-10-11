# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver transformation – SharePoint sites
# MAGIC
# MAGIC This notebook curates the SharePointSites_v1 bronze table into an analytics-friendly silver table.

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

def transform_sharepoint_sites(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("Id").alias("site_id"),
            F.col("Url").alias("site_url"),
            F.col("GroupId").alias("associated_group_id"),
            F.col("RootWeb.Id").alias("root_web_id"),
            F.col("RootWeb.Title").alias("site_title"),
            F.col("RootWeb.WebTemplate").alias("site_template"),
            F.col("RootWeb.WebTemplateId").alias("site_template_id"),
            F.col("RootWeb.Configuration").alias("site_template_configuration"),
            F.col("RootWeb.LastItemModifiedDate").alias("last_item_modified_datetime"),
            F.col("StorageQuota").alias("storage_quota_bytes"),
            F.col("StorageUsed").alias("storage_used_bytes"),
            F.col("StorageMetrics.MetadataSize").alias("storage_metadata_bytes"),
            F.col("StorageMetrics.TotalFileCount").alias("total_file_count"),
            F.col("ArchiveState").alias("archive_state"),
            F.col("IsHubSite").alias("is_hub_site"),
            F.col("IsTeamsConnectedSite").alias("is_teams_connected_site"),
            F.col("IsTeamsChannelSite").alias("is_teams_channel_site"),
            F.col("IsCommunicationSite").alias("is_communication_site"),
            F.col("IsOneDrive").alias("is_onedrive"),
            F.col("Privacy").alias("site_privacy"),
            F.col("Classification").alias("site_classification"),
            F.col("SensitivityLabelInfo.DisplayName").alias("sensitivity_label_name"),
            F.col("SensitivityLabelInfo.Id").alias("sensitivity_label_id"),
            F.col("Owner.AadObjectId").alias("owner_aad_object_id"),
            F.col("Owner.Email").alias("owner_email"),
            F.col("Owner.UPN").alias("owner_upn"),
            F.col("Owner.Name").alias("owner_display_name"),
            F.col("SecondaryContact.AadObjectId").alias("secondary_contact_aad_object_id"),
            F.col("SecondaryContact.Email").alias("secondary_contact_email"),
            F.col("SecondaryContact.UPN").alias("secondary_contact_upn"),
            F.col("SecondaryContact.Name").alias("secondary_contact_display_name"),
            F.col("CreatedTime").alias("created_datetime_utc"),
            F.col("LastSecurityModifiedDate").alias("last_security_modified_datetime"),
            F.col("LastContentChange").alias("last_content_change_datetime"),
            F.col("SnapshotDate").alias("snapshot_datetime_utc"),
            F.col("snapshot_date"),
        )
        .withColumn("snapshot_date", F.col("snapshot_date").cast("string"))
        .dropDuplicates(["site_id", "snapshot_date"])
    )


# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

bronze_path = f"{BRONZE_BASE_PATH}/sharepoint_sites"
target_path = build_target_path(SILVER_BASE_PATH, "sharepoint_sites")

bronze_partitions = list_partitions(bronze_path, "snapshot_date")
partitions_to_process = resolve_target_partitions(bronze_partitions, target_path, "snapshot_date", RUN_MODE)

if partitions_to_process:
    source_df = read_partitioned_delta(bronze_path, "snapshot_date", partitions_to_process)
    transformed_df = transform_sharepoint_sites(source_df)
    write_result = write_silver_delta(
        transformed_df,
        target_path,
        "snapshot_date",
        RUN_MODE,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        table_prefix=TABLE_PREFIX,
        output_alias="sharepoint_sites",
    )
else:
    write_result = {"status": "skipped", "rows_written": 0, "target_path": target_path, "table_name": ""}

write_result["partitions_processed"] = ",".join(partitions_to_process)
write_result["dataset"] = "sharepoint_sites"

audit_df = spark.createDataFrame([write_result])

# COMMAND ----------

display(audit_df)

# COMMAND ----------

dbutils.notebook.exit(json.dumps([write_result]))
