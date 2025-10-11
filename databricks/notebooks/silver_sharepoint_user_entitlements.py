# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver orchestration – SharePoint entitlement datasets
# MAGIC
# MAGIC Coordinates individual transformation notebooks to produce the silver Delta tables required for the
# MAGIC SharePoint entitlement gold layer. Each child notebook focuses on a discrete domain, improving
# MAGIC maintainability while keeping this orchestrator lightweight.

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

resolved_widgets["bronze_base_path"] = resolved_widgets["bronze_base_path"].rstrip("/")
resolved_widgets["silver_base_path"] = resolved_widgets["silver_base_path"].rstrip("/")
resolved_widgets["catalog_name"] = resolved_widgets["catalog_name"].strip()
resolved_widgets["schema_name"] = resolved_widgets["schema_name"].strip()
resolved_widgets["table_prefix"] = resolved_widgets["table_prefix"].strip()
resolved_widgets["run_mode"] = validate_run_mode(resolved_widgets["run_mode"])

ensure_catalog_schema_pair(resolved_widgets["catalog_name"], resolved_widgets["schema_name"])

# COMMAND ----------

child_arguments = {
    "environment": resolved_widgets["environment"],
    "bronze_base_path": resolved_widgets["bronze_base_path"],
    "silver_base_path": resolved_widgets["silver_base_path"],
    "catalog_name": resolved_widgets["catalog_name"],
    "schema_name": resolved_widgets["schema_name"],
    "run_mode": resolved_widgets["run_mode"],
    "table_prefix": resolved_widgets["table_prefix"],
}

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

notebook_paths = [
    "./silver/transform_sharepoint_sites",
    "./silver/transform_sharepoint_permissions",
    "./silver/transform_sharepoint_groups",
    "./silver/transform_aad_groups",
    "./silver/transform_users_dim",
]

combined_results: List[Dict[str, object]] = []
for path in notebook_paths:
    child_response = dbutils.notebook.run(path, 0, child_arguments)
    combined_results.extend(json.loads(child_response))

audit_df = spark.createDataFrame(combined_results)

# COMMAND ----------

display(audit_df)

# COMMAND ----------

exit_with_results(combined_results)
