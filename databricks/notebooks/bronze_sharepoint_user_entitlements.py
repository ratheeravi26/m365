# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze orchestration – SharePoint entitlement datasets
# MAGIC
# MAGIC This notebook orchestrates the ingestion of all Microsoft Graph Data Connect datasets required for
# MAGIC the SharePoint entitlement solution. It delegates the actual ingestion work to the modular notebooks
# MAGIC located under `bronze/` and aggregates their audit results for observability.

# COMMAND ----------

import json
from typing import Dict, List

from pyspark.sql import SparkSession

from shared.environment import configure_environment, ensure_catalog_schema_pair, validate_run_mode
from shared.bronze_ingest import exit_with_results

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

resolved_widgets["source_base_path"] = resolved_widgets["source_base_path"].rstrip("/")
resolved_widgets["bronze_base_path"] = resolved_widgets["bronze_base_path"].rstrip("/")
resolved_widgets["catalog_name"] = resolved_widgets["catalog_name"].strip()
resolved_widgets["schema_name"] = resolved_widgets["schema_name"].strip()
resolved_widgets["table_prefix"] = resolved_widgets["table_prefix"].strip()
resolved_widgets["run_mode"] = validate_run_mode(resolved_widgets["run_mode"])

ensure_catalog_schema_pair(resolved_widgets["catalog_name"], resolved_widgets["schema_name"])

# COMMAND ----------

child_arguments = {
    "environment": resolved_widgets["environment"],
    "source_base_path": resolved_widgets["source_base_path"],
    "bronze_base_path": resolved_widgets["bronze_base_path"],
    "catalog_name": resolved_widgets["catalog_name"],
    "schema_name": resolved_widgets["schema_name"],
    "run_mode": resolved_widgets["run_mode"],
    "table_prefix": resolved_widgets["table_prefix"],
}

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

sharepoint_result = dbutils.notebook.run("./bronze/ingest_sharepoint", 0, child_arguments)
aad_result = dbutils.notebook.run("./bronze/ingest_aad_groups", 0, child_arguments)
graph_user_result = dbutils.notebook.run("./bronze/ingest_graph_users", 0, child_arguments)

combined_results: List[Dict[str, str]] = []
combined_results.extend(json.loads(sharepoint_result))
combined_results.extend(json.loads(aad_result))
combined_results.extend(json.loads(graph_user_result))

audit_df = spark.createDataFrame(combined_results)

# COMMAND ----------

display(audit_df)

# COMMAND ----------

exit_with_results(combined_results)
