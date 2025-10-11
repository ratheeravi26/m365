# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver transformation – Azure AD groups and memberships
# MAGIC
# MAGIC Converts the GroupDetails_v0, GroupMembers_v0, and GroupOwners_v0 bronze tables into their silver
# MAGIC counterparts used by the SharePoint entitlement pipeline.

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

def transform_group_details(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("id").alias("aad_group_id"),
            F.col("displayName").alias("aad_group_display_name"),
            F.col("description").alias("aad_group_description"),
            F.col("mail").alias("aad_group_mail"),
            F.col("mailNickname").alias("aad_group_mail_nickname"),
            F.col("mailEnabled").alias("mail_enabled"),
            F.col("securityEnabled").alias("security_enabled"),
            F.col("groupTypes").alias("group_types"),
            F.col("visibility").alias("group_visibility"),
            F.col("classification").alias("group_classification"),
            F.col("preferredDataLocation").alias("preferred_data_location"),
            F.col("createdDateTime").alias("group_created_datetime"),
            F.col("renewedDateTime").alias("group_renewed_datetime"),
            F.col("deletedDateTime").alias("group_deleted_datetime"),
            F.col("resourceProvisioningOptions").alias("resource_provisioning_options"),
            F.col("preferredLanguage").alias("preferred_language"),
            F.col("isAssignableToRole").alias("is_assignable_to_role"),
            F.col("snapshot_date"),
        )
        .withColumn("snapshot_date", F.col("snapshot_date").cast("string"))
        .dropDuplicates(["aad_group_id", "snapshot_date"])
    )


def transform_group_members(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("id").alias("aad_group_id"),
            F.col("userPrincipalName").alias("member_user_principal_name"),
            F.col("displayName").alias("member_display_name"),
            F.coalesce(F.col("puser"), F.col("pObjectId")).alias("member_aad_object_id"),
            F.col("snapshot_date"),
        )
        .withColumn("snapshot_date", F.col("snapshot_date").cast("string"))
        .dropDuplicates(["aad_group_id", "member_aad_object_id", "snapshot_date"])
    )


def transform_group_owners(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("id").alias("aad_group_id"),
            F.col("userPrincipalName").alias("owner_user_principal_name"),
            F.col("displayName").alias("owner_display_name"),
            F.coalesce(F.col("puser"), F.col("pObjectId")).alias("owner_aad_object_id"),
            F.col("snapshot_date"),
        )
        .withColumn("snapshot_date", F.col("snapshot_date").cast("string"))
        .dropDuplicates(["aad_group_id", "owner_aad_object_id", "snapshot_date"])
    )


# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

bronze_details_path = f"{BRONZE_BASE_PATH}/group_details"
bronze_members_path = f"{BRONZE_BASE_PATH}/group_members"
bronze_owners_path = f"{BRONZE_BASE_PATH}/group_owners"

target_details_path = build_target_path(SILVER_BASE_PATH, "aad_groups")
target_members_path = build_target_path(SILVER_BASE_PATH, "aad_group_members")
target_owners_path = build_target_path(SILVER_BASE_PATH, "aad_group_owners")

results: List[Dict[str, object]] = []

# Group details
detail_partitions = list_partitions(bronze_details_path, "snapshot_date")
detail_partitions_to_process = resolve_target_partitions(detail_partitions, target_details_path, "snapshot_date", RUN_MODE)
if detail_partitions_to_process:
    details_df = read_partitioned_delta(bronze_details_path, "snapshot_date", detail_partitions_to_process)
    transformed_details = transform_group_details(details_df)
    detail_result = write_silver_delta(
        transformed_details,
        target_details_path,
        "snapshot_date",
        RUN_MODE,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        table_prefix=TABLE_PREFIX,
        output_alias="aad_groups",
    )
else:
    detail_result = {"status": "skipped", "rows_written": 0, "target_path": target_details_path, "table_name": ""}
detail_result.update(
    {
        "dataset": "aad_groups",
        "partitions_processed": ",".join(detail_partitions_to_process),
    }
)
results.append(detail_result)

# Group members
member_partitions = list_partitions(bronze_members_path, "snapshot_date")
member_partitions_to_process = resolve_target_partitions(member_partitions, target_members_path, "snapshot_date", RUN_MODE)
if member_partitions_to_process:
    members_df = read_partitioned_delta(bronze_members_path, "snapshot_date", member_partitions_to_process)
    transformed_members = transform_group_members(members_df)
    member_result = write_silver_delta(
        transformed_members,
        target_members_path,
        "snapshot_date",
        RUN_MODE,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        table_prefix=TABLE_PREFIX,
        output_alias="aad_group_members",
    )
else:
    member_result = {"status": "skipped", "rows_written": 0, "target_path": target_members_path, "table_name": ""}
member_result.update(
    {
        "dataset": "aad_group_members",
        "partitions_processed": ",".join(member_partitions_to_process),
    }
)
results.append(member_result)

# Group owners
owner_partitions = list_partitions(bronze_owners_path, "snapshot_date")
owner_partitions_to_process = resolve_target_partitions(owner_partitions, target_owners_path, "snapshot_date", RUN_MODE)
if owner_partitions_to_process:
    owners_df = read_partitioned_delta(bronze_owners_path, "snapshot_date", owner_partitions_to_process)
    transformed_owners = transform_group_owners(owners_df)
    owner_result = write_silver_delta(
        transformed_owners,
        target_owners_path,
        "snapshot_date",
        RUN_MODE,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        table_prefix=TABLE_PREFIX,
        output_alias="aad_group_owners",
    )
else:
    owner_result = {"status": "skipped", "rows_written": 0, "target_path": target_owners_path, "table_name": ""}
owner_result.update(
    {
        "dataset": "aad_group_owners",
        "partitions_processed": ",".join(owner_partitions_to_process),
    }
)
results.append(owner_result)

audit_df = spark.createDataFrame(results)

# COMMAND ----------

display(audit_df)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(results))
