# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver transformation – SharePoint groups
# MAGIC
# MAGIC This notebook produces the silver SharePoint group dimension (`sharepoint_groups`) and the
# MAGIC associated membership bridge (`sharepoint_group_members`) from the bronze SharePointGroups_v1 dataset.

# COMMAND ----------

import json
from typing import Dict, List, Tuple

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

def transform_sharepoint_groups(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    groups_df = (
        df.select(
            F.col("GroupId").alias("sp_group_id"),
            F.col("SiteId").alias("site_id"),
            F.col("DisplayName").alias("sp_group_display_name"),
            F.col("DisplayName").alias("sp_group_login_name"),
            F.col("GroupLinkId").alias("sp_group_link_id"),
            F.col("GroupType").alias("sp_group_type"),
            F.col("Owner.Type").alias("owner_type"),
            F.col("Owner.AadObjectId").alias("owner_aad_object_id"),
            F.col("Owner.Name").alias("owner_display_name"),
            F.col("Owner.Email").alias("owner_email"),
            F.col("Owner.TypeV2").alias("owner_type_v2"),
            F.col("Owner.LoginName").alias("owner_login_name"),
            F.col("Owner.UPN").alias("owner_upn"),
            F.col("SnapshotDate").alias("snapshot_datetime_utc"),
            F.col("snapshot_date"),
        )
        .withColumn(
            "group_key",
            F.lower(
                F.coalesce(
                    F.col("sp_group_login_name"),
                    F.col("sp_group_display_name"),
                )
            ),
        )
        .withColumn("snapshot_date", F.col("snapshot_date").cast("string"))
        .dropDuplicates(["sp_group_id", "snapshot_date"])
    )

    members_df = (
        df.withColumn("member", F.explode_outer("Members"))
        .select(
            F.col("GroupId").alias("sp_group_id"),
            F.col("SiteId").alias("site_id"),
            F.col("member.Type").alias("member_type"),
            F.col("member.TypeV2").alias("member_type_v2"),
            F.col("member.Name").alias("member_display_name"),
            F.col("member.Email").alias("member_email"),
            F.col("member.LoginName").alias("member_login_name"),
            F.col("member.AadObjectId").alias("member_aad_object_id"),
            F.col("member.UPN").alias("member_upn"),
            F.col("SnapshotDate").alias("snapshot_datetime_utc"),
            F.col("snapshot_date"),
        )
        .withColumn("snapshot_date", F.col("snapshot_date").cast("string"))
        .dropDuplicates(
            [
                "sp_group_id",
                "member_aad_object_id",
                "member_login_name",
                "member_upn",
                "snapshot_date",
            ]
        )
    )

    return groups_df, members_df


# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

bronze_path = f"{BRONZE_BASE_PATH}/sharepoint_groups"
group_target_path = build_target_path(SILVER_BASE_PATH, "sharepoint_groups")
member_target_path = build_target_path(SILVER_BASE_PATH, "sharepoint_group_members")

bronze_partitions = list_partitions(bronze_path, "snapshot_date")
partitions_to_process = resolve_target_partitions(bronze_partitions, group_target_path, "snapshot_date", RUN_MODE)

results: List[Dict[str, object]] = []

if partitions_to_process:
    source_df = read_partitioned_delta(bronze_path, "snapshot_date", partitions_to_process)
    groups_df, members_df = transform_sharepoint_groups(source_df)

    groups_result = write_silver_delta(
        groups_df,
        group_target_path,
        "snapshot_date",
        RUN_MODE,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        table_prefix=TABLE_PREFIX,
        output_alias="sharepoint_groups",
    )
    groups_result.update(
        {
            "partitions_processed": ",".join(partitions_to_process),
            "dataset": "sharepoint_groups",
        }
    )
    results.append(groups_result)

    members_result = write_silver_delta(
        members_df,
        member_target_path,
        "snapshot_date",
        RUN_MODE,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        table_prefix=TABLE_PREFIX,
        output_alias="sharepoint_group_members",
    )
    members_result.update(
        {
            "partitions_processed": ",".join(partitions_to_process),
            "dataset": "sharepoint_group_members",
        }
    )
    results.append(members_result)
else:
    results.append(
        {
            "status": "skipped",
            "rows_written": 0,
            "target_path": group_target_path,
            "table_name": "",
            "partitions_processed": "",
            "dataset": "sharepoint_groups",
        }
    )
    results.append(
        {
            "status": "skipped",
            "rows_written": 0,
            "target_path": member_target_path,
            "table_name": "",
            "partitions_processed": "",
            "dataset": "sharepoint_group_members",
        }
    )

audit_df = spark.createDataFrame(results)

# COMMAND ----------

display(audit_df)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(results))
