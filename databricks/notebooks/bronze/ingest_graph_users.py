# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze ingestion – Graph users dataset
# MAGIC
# MAGIC Ingests raw Microsoft Graph `/users` exports (landed in the data lake as JSON) into a bronze Delta table.
# MAGIC This notebook mirrors the ingestion pattern of the other MGDC datasets so it can be orchestrated alongside them.

# COMMAND ----------

from typing import Dict, List

from pyspark.sql import SparkSession

from shared.environment import configure_environment, ensure_catalog_schema_pair, validate_run_mode
from shared.bronze_ingest import ingest_dataset, exit_with_results

spark: SparkSession

# COMMAND ----------

ENV_CONFIG: Dict[str, Dict[str, str]] = {
    "dev": {
        "source_base_path": "abfss://bronze@ubsadlsdev.dfs.core.windows.net/mgdc",
        "bronze_base_path": "abfss://bronze@ubsadlsdev.dfs.core.windows.net/delta/mgdc",
        "catalog_name": "ubs_entitlements_dev",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp_dev",
    },
    "uat": {
        "source_base_path": "abfss://bronze@ubsadlsuat.dfs.core.windows.net/mgdc",
        "bronze_base_path": "abfss://bronze@ubsadlsuat.dfs.core.windows.net/delta/mgdc",
        "catalog_name": "ubs_entitlements_uat",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp_uat",
    },
    "prod": {
        "source_base_path": "abfss://bronze@ubsadlsprod.dfs.core.windows.net/mgdc",
        "bronze_base_path": "abfss://bronze@ubsadlsprod.dfs.core.windows.net/delta/mgdc",
        "catalog_name": "ubs_entitlements_prod",
        "schema_name": "sharepoint",
        "table_prefix": "mgdc_sp",
    },
}

WIDGET_DEFAULTS: Dict[str, str] = {
    "source_base_path": "/mnt/bronze/mgdc",
    "bronze_base_path": "/mnt/bronze/delta/mgdc",
    "catalog_name": "",
    "schema_name": "",
    "run_mode": "incremental",
    "table_prefix": "mgdc_sp",
}

resolved_widgets = configure_environment(ENV_CONFIG, WIDGET_DEFAULTS)

SOURCE_BASE_PATH = resolved_widgets["source_base_path"].rstrip("/")
BRONZE_BASE_PATH = resolved_widgets["bronze_base_path"].rstrip("/")
CATALOG_NAME = resolved_widgets["catalog_name"].strip()
SCHEMA_NAME = resolved_widgets["schema_name"].strip()
RUN_MODE = validate_run_mode(resolved_widgets["run_mode"])
TABLE_PREFIX = resolved_widgets["table_prefix"].strip()

ensure_catalog_schema_pair(CATALOG_NAME, SCHEMA_NAME)

# COMMAND ----------

DATASET_CONFIG: List[Dict[str, str]] = [
    {
        "dataset_name": "GraphUsers",
        "output_alias": "graph_users",
        "partition_key": "snapshot_date",
    },
]

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

results = [
    ingest_dataset(
        dataset_conf,
        source_base_path=SOURCE_BASE_PATH,
        bronze_base_path=BRONZE_BASE_PATH,
        run_mode=RUN_MODE,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        table_prefix=TABLE_PREFIX,
    )
    for dataset_conf in DATASET_CONFIG
]

audit_df = spark.createDataFrame(results)

# COMMAND ----------

display(audit_df)

# COMMAND ----------

exit_with_results(results)
