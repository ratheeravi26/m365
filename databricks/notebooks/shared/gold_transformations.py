from typing import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def parse_snapshot_date(column: Column) -> Column:
    return F.when(
        F.length(column) == 8, F.to_date(column, "yyyyMMdd")
    ).otherwise(F.to_date(column))


def normalize_principal_type(column: Column) -> Column:
    return F.lower(F.trim(F.coalesce(column, F.lit("unknown"))))


def annotate_permissions(perms_df: DataFrame, sites_df: DataFrame) -> DataFrame:
    sites_projection = sites_df.select(
        F.col("site_id").alias("sites_site_id"),
        F.col("snapshot_date").alias("sites_snapshot_date"),
        F.col("site_url").alias("sites_site_url"),
        F.col("root_web_title").alias("sites_root_web_title"),
        F.col("root_web_template").alias("sites_root_web_template"),
        F.col("site_privacy").alias("sites_site_privacy"),
        F.col("is_hub_site").alias("sites_is_hub_site"),
        F.col("is_teams_connected_site").alias("sites_is_teams_connected_site"),
    )

    perms_enriched = (
        perms_df.alias("perms")
        .join(
            sites_projection.alias("sites"),
            (
                (F.col("perms.site_id") == F.col("sites.sites_site_id"))
                & (F.col("perms.snapshot_date") == F.col("sites.sites_snapshot_date"))
            ),
            how="left",
        )
        .select(
            "perms.*",
            F.col("sites.sites_site_url").alias("sites_site_url"),
            F.col("sites.sites_root_web_title").alias("sites_root_web_title"),
            F.col("sites.sites_root_web_template").alias("sites_root_web_template"),
            F.col("sites.sites_site_privacy").alias("sites_site_privacy"),
            F.col("sites.sites_is_hub_site").alias("sites_is_hub_site"),
            F.col("sites.sites_is_teams_connected_site").alias("sites_is_teams_connected_site"),
        )
        .withColumn(
            "principal_type_normalized",
            normalize_principal_type(
                F.coalesce(F.col("principal_type_v2"), F.col("principal_type"))
            ),
        )
        .withColumn(
            "site_url_resolved",
            F.col("sites_site_url"),
        )
        .withColumn(
            "site_title_resolved",
            F.coalesce(F.col("sites_root_web_title"), F.col("site_id")),
        )
        .withColumn(
            "sharepoint_object_type",
            F.when(F.col("list_item_id").isNotNull(), F.lit("ListItem"))
            .when(F.col("list_id").isNotNull(), F.lit("List"))
            .otherwise(F.lit("Site")),
        )
        .withColumn(
            "sharepoint_object_id",
            F.when(
                F.col("list_item_id").isNotNull(),
                F.coalesce(F.col("unique_item_id"), F.col("list_item_id")),
            )
            .when(F.col("list_id").isNotNull(), F.col("list_id"))
            .otherwise(F.col("site_id")),
        )
        .withColumn(
            "sharepoint_object_url",
            F.coalesce(
                F.col("item_url"),
                F.col("sites_site_url"),
            ),
        )
    )

    return perms_enriched


def build_direct_entitlements(perms_df: DataFrame) -> DataFrame:
    user_type_values = [
        "user",
        "internaluser",
        "externaluser",
        "b2buser",
    ]

    return (
        perms_df.where(F.col("principal_type_normalized").isin(user_type_values))
        .select(
            F.col("snapshot_date").alias("snapshot_date_key"),
            parse_snapshot_date(F.col("snapshot_date")).alias("snapshot_date"),
            F.lit("Direct").alias("entitlement_source"),
            F.coalesce(
                F.col("principal_aad_object_id"),
                F.col("principal_upn"),
                F.col("principal_login_name"),
                F.col("principal_email"),
            ).alias("entitlement_source_id"),
            F.coalesce(
                F.col("principal_display_name"),
                F.col("principal_email"),
                F.col("principal_upn"),
            ).alias("entitlement_source_display_name"),
            F.col("principal_aad_object_id").alias("user_aad_object_id"),
            F.coalesce(
                F.col("principal_upn"),
                F.col("principal_login_name"),
                F.col("principal_email"),
            ).alias("user_principal_name"),
            F.col("principal_display_name").alias("user_display_name"),
            F.coalesce(F.col("principal_email"), F.col("principal_upn")).alias("user_email"),
            F.col("site_id"),
            F.col("site_url_resolved").alias("site_url"),
            F.col("site_title_resolved").alias("site_display_name"),
            F.col("sharepoint_object_type"),
            F.col("sharepoint_object_id"),
            F.col("sharepoint_object_url"),
            F.col("role_definition").alias("permission_role_name"),
            F.lit(None).cast("string").alias("permission_role_description"),
            F.col("share_created_by"),
            F.col("share_created_datetime"),
            F.col("share_last_modified_by"),
            F.col("share_last_modified_datetime"),
            F.col("share_expiration_datetime"),
            F.col("total_user_count"),
            F.col("principal_user_count"),
            F.col("principal_scope_type"),
            F.col("principal_scope_count"),
            F.col("principal_type_v2").alias("principal_type_v2"),
            F.lit(None).cast("timestamp").alias("membership_added_datetime"),
            F.lit(None).cast("timestamp").alias("membership_removed_datetime"),
        )
    )


def build_sharepoint_group_entitlements(
    perms_df: DataFrame,
    group_members_df: DataFrame,
    groups_df: DataFrame,
) -> DataFrame:
    group_type_values = [
        "sharepointgroup",
        "sharepointprincipal.sharepointgroup",
        "sharepointprincipaltype.sharepointgroup",
    ]

    groups_prepared = groups_df.select(
        "sp_group_id",
        "site_id",
        "sp_group_display_name",
        "sp_group_login_name",
        "group_key",
        "snapshot_date",
    )

    members_prepared = (
        group_members_df.where(F.lower(F.col("member_type")) == "user")
        .select(
            "sp_group_id",
            "snapshot_date",
            "member_aad_object_id",
            "member_display_name",
            "member_email",
            "member_upn",
        )
    )

    group_perms = (
        perms_df.where(F.col("principal_type_normalized").isin(group_type_values))
        .alias("perms")
        .join(
            groups_prepared.alias("groups"),
            (
                (F.col("perms.site_id") == F.col("groups.site_id"))
                & (F.col("perms.snapshot_date") == F.col("groups.snapshot_date"))
                & (F.col("perms.principal_group_key") == F.col("groups.group_key"))
            ),
            how="inner",
        )
        .join(
            members_prepared.alias("members"),
            (
                (F.col("groups.sp_group_id") == F.col("members.sp_group_id"))
                & (F.col("perms.snapshot_date") == F.col("members.snapshot_date"))
            ),
            how="inner",
        )
    )

    return group_perms.select(
        F.col("perms.snapshot_date").alias("snapshot_date_key"),
        parse_snapshot_date(F.col("perms.snapshot_date")).alias("snapshot_date"),
        F.lit("SharePointGroup").alias("entitlement_source"),
        F.coalesce(
            F.col("groups.sp_group_id").cast("string"),
            F.col("perms.principal_login_name"),
            F.col("perms.principal_display_name"),
        ).alias("entitlement_source_id"),
        F.coalesce(F.col("groups.sp_group_display_name"), F.col("perms.principal_display_name")).alias(
            "entitlement_source_display_name"
        ),
        F.col("members.member_aad_object_id").alias("user_aad_object_id"),
        F.coalesce(F.col("members.member_upn"), F.col("members.member_login_name")).alias("user_principal_name"),
        F.col("members.member_display_name").alias("user_display_name"),
        F.col("members.member_email").alias("user_email"),
        F.col("perms.site_id"),
        F.col("perms.site_url_resolved").alias("site_url"),
        F.col("perms.site_title_resolved").alias("site_display_name"),
        F.col("perms.sharepoint_object_type"),
        F.col("perms.sharepoint_object_id"),
        F.col("perms.sharepoint_object_url"),
        F.col("perms.role_definition").alias("permission_role_name"),
        F.lit(None).cast("string").alias("permission_role_description"),
        F.col("perms.share_created_by"),
        F.col("perms.share_created_datetime"),
        F.col("perms.share_last_modified_by"),
        F.col("perms.share_last_modified_datetime"),
        F.col("perms.share_expiration_datetime"),
        F.col("perms.total_user_count"),
        F.col("perms.principal_user_count"),
        F.col("perms.principal_scope_type"),
        F.col("perms.principal_scope_count"),
        F.col("perms.principal_type_v2").alias("principal_type_v2"),
        F.lit(None).cast("timestamp").alias("membership_added_datetime"),
        F.lit(None).cast("timestamp").alias("membership_removed_datetime"),
    )


def build_aad_group_entitlements(
    perms_df: DataFrame,
    aad_members_df: DataFrame,
    aad_groups_df: DataFrame,
) -> DataFrame:
    aad_group_type_values = [
        "group",
        "azureadgroup",
        "microsoft.directory.group",
        "microsoft.graph.group",
    ]

    groups_prepared = aad_groups_df.select(
        "aad_group_id",
        "aad_group_display_name",
        "aad_group_mail",
        "security_enabled",
        "group_types",
        "snapshot_date",
    )

    members_prepared = aad_members_df.select(
        "aad_group_id",
        "member_aad_object_id",
        "member_user_principal_name",
        "member_display_name",
        "snapshot_date",
    )

    aad_perms = (
        perms_df.where(F.col("principal_type_normalized").isin(aad_group_type_values))
        .alias("perms")
        .join(
            groups_prepared.alias("groups"),
            (
                (F.col("perms.principal_aad_object_id") == F.col("groups.aad_group_id"))
                & (F.col("perms.snapshot_date") == F.col("groups.snapshot_date"))
            ),
            how="left",
        )
        .join(
            members_prepared.alias("members"),
            (
                (F.col("groups.aad_group_id") == F.col("members.aad_group_id"))
                & (F.col("perms.snapshot_date") == F.col("members.snapshot_date"))
            ),
            how="inner",
        )
    )

    return aad_perms.select(
        F.col("perms.snapshot_date").alias("snapshot_date_key"),
        parse_snapshot_date(F.col("perms.snapshot_date")).alias("snapshot_date"),
        F.lit("AzureADGroup").alias("entitlement_source"),
        F.coalesce(
            F.col("groups.aad_group_id"),
            F.col("perms.principal_aad_object_id"),
        ).alias("entitlement_source_id"),
        F.coalesce(F.col("groups.aad_group_display_name"), F.col("perms.principal_display_name")).alias(
            "entitlement_source_display_name"
        ),
        F.col("members.member_aad_object_id").alias("user_aad_object_id"),
        F.col("members.member_user_principal_name").alias("user_principal_name"),
        F.col("members.member_display_name").alias("user_display_name"),
        F.col("members.member_user_principal_name").alias("user_email"),
        F.col("perms.site_id"),
        F.col("perms.site_url_resolved").alias("site_url"),
        F.col("perms.site_title_resolved").alias("site_display_name"),
        F.col("perms.sharepoint_object_type"),
        F.col("perms.sharepoint_object_id"),
        F.col("perms.sharepoint_object_url"),
        F.col("perms.role_definition").alias("permission_role_name"),
        F.lit(None).cast("string").alias("permission_role_description"),
        F.col("perms.share_created_by"),
        F.col("perms.share_created_datetime"),
        F.col("perms.share_last_modified_by"),
        F.col("perms.share_last_modified_datetime"),
        F.col("perms.share_expiration_datetime"),
        F.col("perms.total_user_count"),
        F.col("perms.principal_user_count"),
        F.col("perms.principal_scope_type"),
        F.col("perms.principal_scope_count"),
        F.col("perms.principal_type_v2").alias("principal_type_v2"),
        F.lit(None).cast("timestamp").alias("membership_added_datetime"),
        F.lit(None).cast("timestamp").alias("membership_removed_datetime"),
    )


def append_lineage(df: DataFrame) -> DataFrame:
    entitlement_hash = F.sha2(
        F.concat_ws(
            "||",
            F.col("snapshot_date_key"),
            F.coalesce(F.col("user_aad_object_id"), F.lit("NA")),
            F.coalesce(F.col("user_principal_name"), F.lit("NA")),
            F.coalesce(F.col("entitlement_source"), F.lit("NA")),
            F.coalesce(F.col("entitlement_source_id"), F.lit("NA")),
            F.coalesce(F.col("sharepoint_object_id"), F.lit("NA")),
            F.coalesce(F.col("permission_role_name"), F.lit("NA")),
        ),
        256,
    )

    return (
        df.withColumn("entitlement_hash", entitlement_hash)
        .withColumn("gold_loaded_on_utc", F.current_timestamp())
        .dropDuplicates(["entitlement_hash"])
    )
