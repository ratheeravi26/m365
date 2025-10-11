# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver transformation – Graph users dimension
# MAGIC
# MAGIC Cleanses the bronze Graph users dataset into a reusable user dimension Delta table that enriches the
# MAGIC SharePoint entitlement gold layer.

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

def transform_graph_users(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("id").alias("user_aad_object_id"),
            F.col("userPrincipalName").alias("user_principal_name"),
            F.col("displayName").alias("user_display_name"),
            F.col("mail").alias("user_mail"),
            F.col("jobTitle").alias("user_job_title"),
            F.col("department").alias("user_department"),
            F.col("officeLocation").alias("user_office_location"),
            F.col("accountEnabled").alias("user_account_enabled"),
            F.col("companyName").alias("user_company_name"),
            F.col("country").alias("user_country"),
            F.col("city").alias("user_city"),
            F.col("state").alias("user_state"),
            F.col("createdDateTime").alias("user_created_datetime"),
            F.col("lastPasswordChangeDateTime").alias("user_last_password_change"),
            F.col("snapshot_date"),
        )
        .withColumn("snapshot_date", F.col("snapshot_date").cast("string"))
        .dropDuplicates(["user_aad_object_id", "snapshot_date"])
    )


# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

bronze_path = f"{BRONZE_BASE_PATH}/graph_users"
target_path = build_target_path(SILVER_BASE_PATH, "graph_users_dim")

bronze_partitions = list_partitions(bronze_path, "snapshot_date")
partitions_to_process = resolve_target_partitions(bronze_partitions, target_path, "snapshot_date", RUN_MODE)

if partitions_to_process:
    source_df = read_partitioned_delta(bronze_path, "snapshot_date", partitions_to_process)
    transformed_df = transform_graph_users(source_df)
    write_result = write_silver_delta(
        transformed_df,
        target_path,
        "snapshot_date",
        RUN_MODE,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        table_prefix=TABLE_PREFIX,
        output_alias="graph_users_dim",
    )
else:
    write_result = {"status": "skipped", "rows_written": 0, "target_path": target_path, "table_name": ""}

write_result.update(
    {
        "partitions_processed": ",".join(partitions_to_process),
        "dataset": "graph_users_dim",
    }
)

audit_df = spark.createDataFrame([write_result])

# COMMAND ----------

display(audit_df)

# COMMAND ----------

dbutils.notebook.exit(json.dumps([write_result]))
