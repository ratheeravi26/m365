# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # M365 SharePoint User Entitlements – Insights & Queries
# MAGIC
# MAGIC This notebook provides ready-to-run analysis cells that answer the most common entitlement questions.
# MAGIC Parameter widgets allow you to target any environment, catalog/schema, and gold table name produced
# MAGIC by the entitlement pipeline.

# COMMAND ----------

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shared.environment import configure_environment, ensure_catalog_schema_pair

spark: SparkSession

# COMMAND ----------

ENV_CONFIG: Dict[str, Dict[str, str]] = {
    "dev": {
        "catalog_name": "ubs_entitlements_dev",
        "schema_name": "sharepoint",
        "gold_table_name": "mgdc_sp_dev_sharepoint_user_entitlements",
    },
    "uat": {
        "catalog_name": "ubs_entitlements_uat",
        "schema_name": "sharepoint",
        "gold_table_name": "mgdc_sp_uat_sharepoint_user_entitlements",
    },
    "prod": {
        "catalog_name": "ubs_entitlements_prod",
        "schema_name": "sharepoint",
        "gold_table_name": "mgdc_sp_sharepoint_user_entitlements",
    },
}

WIDGET_DEFAULTS: Dict[str, str] = {
    "catalog_name": "",
    "schema_name": "",
    "gold_table_name": "sharepoint_user_entitlements",
    "snapshot_filter": "",
    "user_filter": "",
}

resolved_widgets = configure_environment(ENV_CONFIG, WIDGET_DEFAULTS)

CATALOG_NAME = resolved_widgets["catalog_name"].strip()
SCHEMA_NAME = resolved_widgets["schema_name"].strip()
GOLD_TABLE_NAME = resolved_widgets["gold_table_name"].strip()
SNAPSHOT_FILTER = resolved_widgets["snapshot_filter"].strip()
USER_FILTER = resolved_widgets["user_filter"].strip()

ensure_catalog_schema_pair(CATALOG_NAME, SCHEMA_NAME)

FULL_TABLE_NAME = (
    f"{CATALOG_NAME}.{SCHEMA_NAME}.{GOLD_TABLE_NAME}"
    if CATALOG_NAME and SCHEMA_NAME
    else GOLD_TABLE_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper view
# MAGIC
# MAGIC Creates a temporary view `entitlements_base` filtered by optional snapshot and user criteria to keep
# MAGIC subsequent queries concise.

# COMMAND ----------

base_df = spark.read.table(FULL_TABLE_NAME)

if SNAPSHOT_FILTER:
    base_df = base_df.filter(F.col("snapshot_date_key") == SNAPSHOT_FILTER)

if USER_FILTER:
    user_filter_lower = USER_FILTER.lower()
    base_df = base_df.filter(
        F.lower(F.col("user_principal_name")).contains(user_filter_lower)
        | F.lower(F.col("user_display_name")).contains(user_filter_lower)
        | F.lower(F.col("user_email")).contains(user_filter_lower)
    )

base_df.createOrReplaceTempView("entitlements_base")

display(base_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. What access does a specific user have across SharePoint?
# MAGIC
# MAGIC > **Question:** _List all SharePoint assets (site/list/item) a user can access, how the access is granted, and the permission role._

# COMMAND ----------

user_access_df = spark.sql(
    """
    SELECT
        user_display_name,
        user_principal_name,
        entitlement_source,
        entitlement_source_display_name,
        site_display_name,
        site_url,
        sharepoint_object_type,
        sharepoint_object_url,
        permission_role_name,
        snapshot_date_key
    FROM entitlements_base
    ORDER BY user_display_name, entitlement_source, site_display_name
    """
)

display(user_access_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Which users have access to a specific SharePoint site or list?
# MAGIC
# MAGIC > **Question:** _Provide a roster of users accessing a site (or list) along with entitlement source and role._

# COMMAND ----------

dbutils.widgets.text("target_site_keyword", "")
TARGET_SITE_KEYWORD = dbutils.widgets.get("target_site_keyword").strip().lower()

site_access_df = (
    spark.sql(
        """
        SELECT
            site_display_name,
            site_url,
            sharepoint_object_type,
            sharepoint_object_url,
            user_display_name,
            user_principal_name,
            user_department,
            entitlement_source,
            permission_role_name,
            snapshot_date_key
        FROM entitlements_base
        """
    )
    .filter(F.lower(F.col("site_display_name")).contains(TARGET_SITE_KEYWORD) | F.lower(F.col("site_url")).contains(TARGET_SITE_KEYWORD))
    .orderBy("site_display_name", "user_display_name")
)

display(site_access_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. What is the entitlement footprint for sensitive sites?
# MAGIC
# MAGIC > **Question:** _Identify sensitive-labelled sites, their total entitlements, and the top departments accessing them._

# COMMAND ----------

sensitive_sites_df = spark.sql(
    """
    SELECT
        site_display_name,
        site_url,
        COUNT(DISTINCT entitlement_hash) AS entitlement_count,
        COUNT(DISTINCT user_principal_name) AS unique_users,
        COLLECT_SET(user_department) AS departments,
        MIN(snapshot_date_key) AS earliest_snapshot,
        MAX(snapshot_date_key) AS latest_snapshot
    FROM entitlements_base
    WHERE sites_sensitivity_label_name IS NOT NULL
    GROUP BY site_display_name, site_url
    ORDER BY entitlement_count DESC
    """
)

display(sensitive_sites_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. Which departments and job titles have the most SharePoint entitlements?
# MAGIC
# MAGIC > **Question:** _Summarise entitlement volumes by department and job title to understand business impact._

# COMMAND ----------

department_impact_df = spark.sql(
    """
    SELECT
        COALESCE(user_department, 'Unspecified') AS department,
        COALESCE(user_job_title, 'Unspecified') AS job_title,
        COUNT(DISTINCT entitlement_hash) AS entitlement_count,
        COUNT(DISTINCT user_principal_name) AS unique_users
    FROM entitlements_base
    GROUP BY department, job_title
    ORDER BY entitlement_count DESC
    """
)

display(department_impact_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5. How are entitlements distributed by source (Direct vs. SharePoint/AAD groups)?
# MAGIC
# MAGIC > **Question:** _Break down entitlements by source type to highlight group vs direct grants._

# COMMAND ----------

source_mix_df = spark.sql(
    """
    SELECT
        entitlement_source,
        COUNT(DISTINCT entitlement_hash) AS entitlement_count,
        COUNT(DISTINCT user_principal_name) AS unique_users
    FROM entitlements_base
    GROUP BY entitlement_source
    ORDER BY entitlement_count DESC
    """
)

display(source_mix_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q6. Which users are disabled in Azure AD but still retain SharePoint access?
# MAGIC
# MAGIC > **Question:** _Identify account management gaps by finding entitlements where `user_account_enabled` is false._

# COMMAND ----------

disabled_users_df = spark.sql(
    """
    SELECT
        user_display_name,
        user_principal_name,
        user_department,
        entitlement_source,
        site_display_name,
        permission_role_name,
        snapshot_date_key
    FROM entitlements_base
    WHERE user_account_enabled = false
    ORDER BY snapshot_date_key DESC, user_display_name
    """
)

display(disabled_users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q7. How has a user's SharePoint access changed over time?
# MAGIC
# MAGIC > **Question:** _Track the entitlement history for a user across snapshots to support access reviews._

# COMMAND ----------

user_history_df = spark.sql(
    """
    SELECT
        snapshot_date_key,
        user_display_name,
        user_principal_name,
        site_display_name,
        sharepoint_object_type,
        permission_role_name,
        entitlement_source
    FROM entitlements_base
    ORDER BY user_principal_name, snapshot_date_key, site_display_name
    """
)

display(user_history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q8. What are the top sites by unique user count?
# MAGIC
# MAGIC > **Question:** _Highlight heavily accessed SharePoint sites for governance and monitoring._

# COMMAND ----------

top_sites_df = spark.sql(
    """
    SELECT
        site_display_name,
        site_url,
        COUNT(DISTINCT user_principal_name) AS unique_users,
        COUNT(DISTINCT entitlement_hash) AS entitlement_count
    FROM entitlements_base
    GROUP BY site_display_name, site_url
    ORDER BY unique_users DESC
    LIMIT 25
    """
)

display(top_sites_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC * Adjust widgets (`environment`, `snapshot_filter`, `user_filter`, `target_site_keyword`) to refine the analysis.
# MAGIC * Export result sets using Databricks' built-in download/export capabilities for stakeholder reporting.
# MAGIC * Extend with additional business questions by reusing the `entitlements_base` temporary view.
