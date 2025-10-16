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
from pyspark.sql import types as T

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

def has_column(df: DataFrame, col_path: str) -> bool:
    parts = col_path.split(".")
    schema = df.schema
    for part in parts:
        if part not in schema.names:
            return False
        field = schema[part]
        if isinstance(field.dataType, T.StructType):
            schema = field.dataType
        else:
            schema = field.dataType
    return True


def col_or_null(df: DataFrame, col_path: str, dtype: T.DataType) -> F.Column:
    return F.col(col_path) if has_column(df, col_path) else F.lit(None).cast(dtype)


def transform_sharepoint_sites(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("ptenant").alias("tenant_id"),
            F.col("Id").alias("site_id"),
            F.col("Url").alias("site_url"),
            F.col("GroupId").alias("associated_group_id"),
            col_or_null(df, "RootWeb.Id", T.StringType()).alias("root_web_id"),
            col_or_null(df, "RootWeb.Title", T.StringType()).alias("root_web_title"),
            col_or_null(df, "RootWeb.WebTemplate", T.StringType()).alias("root_web_template"),
            col_or_null(df, "RootWeb.WebTemplateId", T.IntegerType()).alias("root_web_template_id"),
            col_or_null(df, "RootWeb.Configuration", T.IntegerType()).alias("root_web_configuration"),
            col_or_null(df, "RootWeb.LastItemModifiedDate", T.TimestampType()).alias("root_web_last_item_modified"),
            F.col("WebCount").alias("web_count"),
            F.col("StorageQuota").alias("storage_quota_bytes"),
            F.col("StorageUsed").alias("storage_used_bytes"),
            col_or_null(df, "StorageMetrics.MetadataSize", T.LongType()).alias("storage_metadata_bytes"),
            col_or_null(df, "StorageMetrics.TotalFileCount", T.LongType()).alias("storage_total_file_count"),
            col_or_null(df, "StorageMetrics.TotalFileStreamSize", T.LongType()).alias("storage_total_file_stream_size"),
            col_or_null(df, "StorageMetrics.TotalSize", T.LongType()).alias("storage_total_size_bytes"),
            F.col("ArchiveState").alias("archive_state"),
            F.col("GeoLocation").alias("geo_location"),
            F.col("IsInRecycleBin").alias("is_in_recycle_bin"),
            F.col("RecycleBinItemCount").alias("recycle_bin_item_count"),
            F.col("RecycleBinItemSize").alias("recycle_bin_item_size"),
            F.col("SecondStageRecycleBinStorageUsage").alias("second_stage_recycle_bin_storage_usage"),
            F.col("IsTeamsConnectedSite").alias("is_teams_connected_site"),
            F.col("IsTeamsChannelSite").alias("is_teams_channel_site"),
            F.col("TeamsChannelType").alias("teams_channel_type"),
            F.col("IsHubSite").alias("is_hub_site"),
            F.col("HubSiteId").alias("hub_site_id"),
            F.col("IsCommunicationSite").alias("is_communication_site"),
            F.col("IsOneDrive").alias("is_onedrive"),
            F.col("BlockAccessFromUnmanagedDevices").alias("block_access_from_unmanaged_devices"),
            F.col("BlockDownloadOfAllFilesOnUnmanagedDevices").alias("block_download_of_all_files_on_unmanaged_devices"),
            F.col("BlockDownloadOfViewableFilesOnUnmanagedDevices").alias("block_download_of_viewable_files_on_unmanaged_devices"),
            F.col("ShareByEmailEnabled").alias("share_by_email_enabled"),
            F.col("ShareByLinkEnabled").alias("share_by_link_enabled"),
            F.col("IsExternalSharingEnabled").alias("is_external_sharing_enabled"),
            F.col("SiteConnectedToPrivateGroup").alias("site_connected_to_private_group"),
            F.col("Privacy").alias("site_privacy"),
            F.col("Classification").alias("site_classification"),
            col_or_null(df, "SensitivityLabelInfo.DisplayName", T.StringType()).alias("sensitivity_label_name"),
            col_or_null(df, "SensitivityLabelInfo.Id", T.StringType()).alias("sensitivity_label_id"),
            F.col("IBMode").alias("ib_mode"),
            col_or_null(df, "Owner.AadObjectId", T.StringType()).alias("owner_aad_object_id"),
            col_or_null(df, "Owner.Email", T.StringType()).alias("owner_email"),
            col_or_null(df, "Owner.UPN", T.StringType()).alias("owner_upn"),
            col_or_null(df, "Owner.Name", T.StringType()).alias("owner_display_name"),
            col_or_null(df, "SecondaryContact.AadObjectId", T.StringType()).alias("secondary_contact_aad_object_id"),
            col_or_null(df, "SecondaryContact.Email", T.StringType()).alias("secondary_contact_email"),
            col_or_null(df, "SecondaryContact.UPN", T.StringType()).alias("secondary_contact_upn"),
            col_or_null(df, "SecondaryContact.Name", T.StringType()).alias("secondary_contact_display_name"),
            F.col("ReadLocked").alias("read_locked"),
            F.col("ReadOnly").alias("read_only"),
            F.col("CreatedTime").alias("created_datetime_utc"),
            F.col("LastSecurityModifiedDate").alias("last_security_modified_datetime"),
            col_or_null(df, "LastUserAccessDate", T.TimestampType()).alias("last_user_access_datetime"),
            F.col("Operation").alias("operation"),
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
