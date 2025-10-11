import os
import sys
from typing import Dict, List

import pandas as pd

CURRENT_DIR = os.path.dirname(__file__)
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

from mock_gold_generation import (
    add_lineage_hash,
    append_common_columns,
    build_aad_group_entitlements,
    build_direct_entitlements,
    build_sharepoint_group_entitlements,
    build_user_dimension,
    annotate_permissions,
)


def build_raw_samples() -> Dict[str, List[Dict]]:
    return {
        "SharePointPermissions_v1": [
            {
                "SiteId": "site-001",
                "SiteUrl": "https://contoso.sharepoint.com/sites/hr",
                "WebId": "web-001",
                "ListId": "list-hr",
                "ListTitle": "HR Documents",
                "ListUrl": "https://contoso.sharepoint.com/sites/hr/Shared%20Documents",
                "ItemId": None,
                "UniqueId": None,
                "ItemType": "Site",
                "ItemURL": None,
                "FileExtension": None,
                "RoleDefinition": "Full Control",
                "LinkId": None,
                "ScopeId": "scope-hr",
                "LinkScope": None,
                "SharedWith": [
                    {
                        "Type": "User",
                        "TypeV2": "InternalUser",
                        "Name": "Adele Vance",
                        "Email": "adele.vance@contoso.com",
                        "AadObjectId": "user-adele",
                        "UPN": "adele.vance@contoso.com",
                        "UserLoginName": "i:0#.f|membership|adele.vance@contoso.com",
                        "UserCount": 1,
                    }
                ],
                "SharedWithCount": [{"Type": "Internal", "Count": 1}],
                "TotalUserCount": 1,
                "ShareCreatedBy": {"Type": "User", "Name": "HR Admin", "Email": "hr.admin@contoso.com", "UPN": "hr.admin@contoso.com"},
                "ShareCreatedTime": "2024-01-14T10:00:00Z",
                "ShareLastModifiedBy": {"Type": "User", "Name": "HR Admin", "Email": "hr.admin@contoso.com", "UPN": "hr.admin@contoso.com"},
                "ShareLastModifiedTime": "2024-01-14T10:00:00Z",
                "ShareExpirationTime": None,
                "SnapshotDate": "2024-01-15T00:00:00Z",
            },
            {
                "SiteId": "site-001",
                "SiteUrl": "https://contoso.sharepoint.com/sites/hr",
                "WebId": "web-001",
                "ListId": "list-hr",
                "ListTitle": "HR Documents",
                "ListUrl": "https://contoso.sharepoint.com/sites/hr/Shared%20Documents",
                "ItemId": None,
                "UniqueId": None,
                "ItemType": "Site",
                "ItemURL": None,
                "FileExtension": None,
                "RoleDefinition": "Read",
                "LinkId": None,
                "ScopeId": "scope-hr",
                "LinkScope": None,
                "SharedWith": [
                    {
                        "Type": "SharePointGroup",
                        "TypeV2": "SharePointGroup",
                        "Name": "HR Owners",
                        "Email": None,
                        "AadObjectId": None,
                        "UPN": None,
                        "UserLoginName": "HR Owners",
                        "UserCount": 5,
                    }
                ],
                "SharedWithCount": [{"Type": "SharePointGroup", "Count": 1}],
                "TotalUserCount": 5,
                "ShareCreatedBy": None,
                "ShareCreatedTime": None,
                "ShareLastModifiedBy": None,
                "ShareLastModifiedTime": None,
                "ShareExpirationTime": None,
                "SnapshotDate": "2024-01-15T00:00:00Z",
            },
            {
                "SiteId": "site-002",
                "SiteUrl": "https://contoso.sharepoint.com/sites/finance",
                "WebId": "web-002",
                "ListId": "list-fin",
                "ListTitle": "Finance Docs",
                "ListUrl": "https://contoso.sharepoint.com/sites/finance/Shared%20Documents",
                "ItemId": None,
                "UniqueId": None,
                "ItemType": "Site",
                "ItemURL": None,
                "FileExtension": None,
                "RoleDefinition": "Read",
                "LinkId": None,
                "ScopeId": "scope-fin",
                "LinkScope": None,
                "SharedWith": [
                    {
                        "Type": "Group",
                        "TypeV2": "SecurityGroup",
                        "Name": "Finance Analysts",
                        "Email": "financeanalysts@contoso.com",
                        "AadObjectId": "aad-fin-analysts",
                        "UPN": None,
                        "UserLoginName": None,
                        "UserCount": 12,
                    }
                ],
                "SharedWithCount": [{"Type": "SecurityGroup", "Count": 1}],
                "TotalUserCount": 12,
                "ShareCreatedBy": None,
                "ShareCreatedTime": None,
                "ShareLastModifiedBy": None,
                "ShareLastModifiedTime": None,
                "ShareExpirationTime": None,
                "SnapshotDate": "2024-01-15T00:00:00Z",
            },
        ],
        "SharePointSites_v1": [
            {
                "Id": "site-001",
                "Url": "https://contoso.sharepoint.com/sites/hr",
                "GroupId": "aad-hr",
                "RootWeb": {
                    "Id": "web-001",
                    "Title": "HR Portal",
                    "WebTemplate": "SITEPAGEPUBLISHING#0",
                    "WebTemplateId": 1,
                    "Configuration": 0,
                    "LastItemModifiedDate": "2024-01-14T00:00:00Z",
                },
                "StorageQuota": 1073741824,
                "StorageUsed": 536870912,
                "StorageMetrics": {"MetadataSize": 1024, "TotalFileCount": 1200, "TotalFileStreamSize": 400000000, "AdditionalFileStreamSize": 1000000, "TotalSize": 500000000},
                "ArchiveState": "None",
                "IsHubSite": False,
                "IsTeamsConnectedSite": True,
                "IsTeamsChannelSite": False,
                "IsCommunicationSite": False,
                "IsOneDrive": False,
                "Privacy": "Private",
                "Classification": "Confidential",
                "SensitivityLabelInfo": {"DisplayName": "Confidential", "Id": "label-conf"},
                "Owner": {"AadObjectId": "user-hr-admin", "Email": "hr.admin@contoso.com", "UPN": "hr.admin@contoso.com", "Name": "HR Admin"},
                "SecondaryContact": {"AadObjectId": "user-hr-backup", "Email": "hr.backup@contoso.com", "UPN": "hr.backup@contoso.com", "Name": "HR Backup"},
                "CreatedTime": "2021-03-01T00:00:00Z",
                "LastSecurityModifiedDate": "2024-01-14T00:00:00Z",
                "LastContentChange": "2024-01-14T00:00:00Z",
                "SnapshotDate": "2024-01-15T00:00:00Z",
            },
            {
                "Id": "site-002",
                "Url": "https://contoso.sharepoint.com/sites/finance",
                "GroupId": "aad-fin",
                "RootWeb": {
                    "Id": "web-002",
                    "Title": "Finance Workspace",
                    "WebTemplate": "STS#3",
                    "WebTemplateId": 2,
                    "Configuration": 0,
                    "LastItemModifiedDate": "2024-01-13T00:00:00Z",
                },
                "StorageQuota": 1073741824,
                "StorageUsed": 734003200,
                "StorageMetrics": {"MetadataSize": 2048, "TotalFileCount": 1500, "TotalFileStreamSize": 650000000, "AdditionalFileStreamSize": 2000000, "TotalSize": 720000000},
                "ArchiveState": "None",
                "IsHubSite": False,
                "IsTeamsConnectedSite": False,
                "IsTeamsChannelSite": False,
                "IsCommunicationSite": False,
                "IsOneDrive": False,
                "Privacy": "Private",
                "Classification": "Highly Confidential",
                "SensitivityLabelInfo": {"DisplayName": "Highly Confidential", "Id": "label-high"},
                "Owner": {"AadObjectId": "user-fin-admin", "Email": "fin.admin@contoso.com", "UPN": "fin.admin@contoso.com", "Name": "Finance Admin"},
                "SecondaryContact": None,
                "CreatedTime": "2020-06-01T00:00:00Z",
                "LastSecurityModifiedDate": "2024-01-10T00:00:00Z",
                "LastContentChange": "2024-01-13T00:00:00Z",
                "SnapshotDate": "2024-01-15T00:00:00Z",
            },
        ],
        "SharePointGroups_v1": [
            {
                "GroupId": "spg-hr-owners",
                "SiteId": "site-001",
                "GroupLinkId": 12345,
                "GroupType": "SharePointGroup",
                "DisplayName": "HR Owners",
                "Owner": {
                    "Type": "User",
                    "AadObjectId": "user-hr-admin",
                    "Name": "HR Admin",
                    "Email": "hr.admin@contoso.com",
                    "TypeV2": "InternalUser",
                    "LoginName": "i:0#.f|membership|hr.admin@contoso.com",
                    "UPN": "hr.admin@contoso.com",
                },
                "Members": [
                    {
                        "Type": "User",
                        "TypeV2": "InternalUser",
                        "Name": "Nestor Wilke",
                        "Email": "nestor.wilke@contoso.com",
                        "LoginName": "i:0#.f|membership|nestor.wilke@contoso.com",
                        "AadObjectId": "user-nestor",
                        "UPN": "nestor.wilke@contoso.com",
                    },
                    {
                        "Type": "User",
                        "TypeV2": "InternalUser",
                        "Name": "Isaiah Langer",
                        "Email": "isaiah.langer@contoso.com",
                        "LoginName": "i:0#.f|membership|isaiah.langer@contoso.com",
                        "AadObjectId": "user-isaiah",
                        "UPN": "isaiah.langer@contoso.com",
                    },
                ],
                "SnapshotDate": "2024-01-15T00:00:00Z",
            }
        ],
        "GroupDetails_v0": [
            {
                "id": "aad-fin-analysts",
                "displayName": "Finance Analysts",
                "description": "Finance analyst security group",
                "mail": "financeanalysts@contoso.com",
                "mailNickname": "financeanalysts",
                "mailEnabled": False,
                "securityEnabled": True,
                "groupTypes": ["Security"],
                "visibility": "Private",
                "classification": "Confidential",
                "preferredDataLocation": "EUR",
                "createdDateTime": "2019-01-01T00:00:00Z",
                "renewedDateTime": "2024-01-01T00:00:00Z",
                "deletedDateTime": None,
                "resourceProvisioningOptions": [],
                "preferredLanguage": "en-US",
                "isAssignableToRole": False,
                "snapshot_date": "20240115",
            }
        ],
        "GroupMembers_v0": [
            {
                "id": "aad-fin-analysts",
                "userPrincipalName": "julia.cole@contoso.com",
                "displayName": "Julia Cole",
                "puser": "user-julia",
                "pObjectId": "user-julia",
                "ptenant": "tenant-001",
                "snapshot_date": "20240115",
            },
            {
                "id": "aad-fin-analysts",
                "userPrincipalName": "lidia.holloway@contoso.com",
                "displayName": "Lidia Holloway",
                "puser": "user-lidia",
                "pObjectId": "user-lidia",
                "ptenant": "tenant-001",
                "snapshot_date": "20240115",
            },
        ],
        "GroupOwners_v0": [
            {
                "id": "aad-fin-analysts",
                "userPrincipalName": "fin.admin@contoso.com",
                "displayName": "Finance Admin",
                "puser": "user-fin-admin",
                "pObjectId": "user-fin-admin",
                "ptenant": "tenant-001",
                "snapshot_date": "20240115",
            }
        ],
        "GraphUsers": [
            {
                "SnapshotDate": "2024-01-15T00:00:00Z",
                "id": "user-adele",
                "userPrincipalName": "adele.vance@contoso.com",
                "displayName": "Adele Vance",
                "mail": "adele.vance@contoso.com",
                "jobTitle": "HR Business Partner",
                "department": "Human Resources",
                "officeLocation": "Zurich",
                "accountEnabled": True,
                "companyName": "Contoso",
                "country": "Switzerland",
                "city": "Zurich",
                "state": "ZH",
                "createdDateTime": "2020-01-10T12:00:00Z",
                "lastPasswordChangeDateTime": "2023-12-01T08:00:00Z",
            },
            {
                "SnapshotDate": "2024-01-15T00:00:00Z",
                "id": "user-megan",
                "userPrincipalName": "megan.bowen@contoso.com",
                "displayName": "Megan Bowen",
                "mail": "megan.bowen@contoso.com",
                "jobTitle": "Financial Analyst",
                "department": "Finance",
                "officeLocation": "London",
                "accountEnabled": True,
                "companyName": "Contoso",
                "country": "United Kingdom",
                "city": "London",
                "state": "LDN",
                "createdDateTime": "2019-02-15T09:30:00Z",
                "lastPasswordChangeDateTime": "2024-02-01T09:00:00Z",
            },
            {
                "SnapshotDate": "2024-01-15T00:00:00Z",
                "id": "user-nestor",
                "userPrincipalName": "nestor.wilke@contoso.com",
                "displayName": "Nestor Wilke",
                "mail": "nestor.wilke@contoso.com",
                "jobTitle": "HR Site Owner",
                "department": "Human Resources",
                "officeLocation": "Zurich",
                "accountEnabled": True,
                "companyName": "Contoso",
                "country": "Switzerland",
                "city": "Zurich",
                "state": "ZH",
                "createdDateTime": "2018-05-20T08:00:00Z",
                "lastPasswordChangeDateTime": "2024-01-15T07:00:00Z",
            },
            {
                "SnapshotDate": "2024-01-15T00:00:00Z",
                "id": "user-isaiah",
                "userPrincipalName": "isaiah.langer@contoso.com",
                "displayName": "Isaiah Langer",
                "mail": "isaiah.langer@contoso.com",
                "jobTitle": "HR Manager",
                "department": "Human Resources",
                "officeLocation": "Zurich",
                "accountEnabled": True,
                "companyName": "Contoso",
                "country": "Switzerland",
                "city": "Zurich",
                "state": "ZH",
                "createdDateTime": "2017-03-10T10:00:00Z",
                "lastPasswordChangeDateTime": "2023-11-12T11:00:00Z",
            },
            {
                "SnapshotDate": "2024-01-15T00:00:00Z",
                "id": "user-julia",
                "userPrincipalName": "julia.cole@contoso.com",
                "displayName": "Julia Cole",
                "mail": "julia.cole@contoso.com",
                "jobTitle": "Senior Analyst",
                "department": "Finance",
                "officeLocation": "London",
                "accountEnabled": True,
                "companyName": "Contoso",
                "country": "United Kingdom",
                "city": "London",
                "state": "LDN",
                "createdDateTime": "2021-04-05T13:30:00Z",
                "lastPasswordChangeDateTime": "2024-01-20T06:45:00Z",
            },
            {
                "SnapshotDate": "2024-01-15T00:00:00Z",
                "id": "user-lidia",
                "userPrincipalName": "lidia.holloway@contoso.com",
                "displayName": "Lidia Holloway",
                "mail": "lidia.holloway@contoso.com",
                "jobTitle": "Finance Specialist",
                "department": "Finance",
                "officeLocation": "London",
                "accountEnabled": True,
                "companyName": "Contoso",
                "country": "United Kingdom",
                "city": "London",
                "state": "LDN",
                "createdDateTime": "2022-06-18T15:00:00Z",
                "lastPasswordChangeDateTime": "2024-02-10T08:15:00Z",
            },
        ],
    }


def bronze_from_raw(raw_rows: List[Dict], dataset_name: str) -> pd.DataFrame:
    df = pd.DataFrame(raw_rows)
    if "SnapshotDate" in df.columns:
        df["snapshot_date"] = pd.to_datetime(df["SnapshotDate"]).dt.strftime("%Y%m%d")
    elif "snapshot_date" in df.columns:
        df["snapshot_date"] = df["snapshot_date"].astype(str)
    else:
        df["snapshot_date"] = "20240115"
    return df


def silver_sharepoint_sites(bronze_df: pd.DataFrame) -> pd.DataFrame:
    df = bronze_df.copy()
    df["snapshot_date"] = df["snapshot_date"].astype(str)
    site_cols = {
        "site_id": df["Id"],
        "site_url": df["Url"],
        "associated_group_id": df["GroupId"],
        "root_web_id": df["RootWeb"].apply(lambda x: x.get("Id")),
        "site_title": df["RootWeb"].apply(lambda x: x.get("Title")),
        "site_template": df["RootWeb"].apply(lambda x: x.get("WebTemplate")),
        "site_template_id": df["RootWeb"].apply(lambda x: x.get("WebTemplateId")),
        "site_template_configuration": df["RootWeb"].apply(lambda x: x.get("Configuration")),
        "last_item_modified_datetime": df["RootWeb"].apply(lambda x: x.get("LastItemModifiedDate")),
        "storage_quota_bytes": df["StorageQuota"],
        "storage_used_bytes": df["StorageUsed"],
        "storage_metadata_bytes": df["StorageMetrics"].apply(lambda x: x.get("MetadataSize")),
        "total_file_count": df["StorageMetrics"].apply(lambda x: x.get("TotalFileCount")),
        "archive_state": df["ArchiveState"],
        "is_hub_site": df["IsHubSite"],
        "is_teams_connected_site": df["IsTeamsConnectedSite"],
        "is_teams_channel_site": df["IsTeamsChannelSite"],
        "is_communication_site": df["IsCommunicationSite"],
        "is_onedrive": df["IsOneDrive"],
        "site_privacy": df["Privacy"],
        "site_classification": df["Classification"],
        "sensitivity_label_name": df["SensitivityLabelInfo"].apply(lambda x: (x or {}).get("DisplayName")),
        "sensitivity_label_id": df["SensitivityLabelInfo"].apply(lambda x: (x or {}).get("Id")),
        "owner_aad_object_id": df["Owner"].apply(lambda x: (x or {}).get("AadObjectId")),
        "owner_email": df["Owner"].apply(lambda x: (x or {}).get("Email")),
        "owner_upn": df["Owner"].apply(lambda x: (x or {}).get("UPN")),
        "owner_display_name": df["Owner"].apply(lambda x: (x or {}).get("Name")),
        "secondary_contact_aad_object_id": df["SecondaryContact"].apply(lambda x: (x or {}).get("AadObjectId") if isinstance(x, dict) else None),
        "secondary_contact_email": df["SecondaryContact"].apply(lambda x: (x or {}).get("Email") if isinstance(x, dict) else None),
        "secondary_contact_upn": df["SecondaryContact"].apply(lambda x: (x or {}).get("UPN") if isinstance(x, dict) else None),
        "secondary_contact_display_name": df["SecondaryContact"].apply(lambda x: (x or {}).get("Name") if isinstance(x, dict) else None),
        "created_datetime_utc": df["CreatedTime"],
        "last_security_modified_datetime": df["LastSecurityModifiedDate"],
        "last_content_change_datetime": df["LastContentChange"],
        "snapshot_datetime_utc": df["SnapshotDate"],
        "snapshot_date": df["snapshot_date"],
    }
    silver_df = pd.DataFrame(site_cols)
    return silver_df.drop_duplicates(subset=["site_id", "snapshot_date"])


def silver_sharepoint_permissions(bronze_df: pd.DataFrame) -> pd.DataFrame:
    df = bronze_df.copy()
    df["snapshot_date"] = df["snapshot_date"].astype(str)
    df = df.explode("SharedWith", ignore_index=True)
    df = df.explode("SharedWithCount", ignore_index=True)

    df["principal_type"] = df["SharedWith"].apply(lambda x: (x or {}).get("Type"))
    df["principal_type_v2"] = df["SharedWith"].apply(lambda x: (x or {}).get("TypeV2"))
    df["principal_display_name"] = df["SharedWith"].apply(lambda x: (x or {}).get("Name"))
    df["principal_email"] = df["SharedWith"].apply(lambda x: (x or {}).get("Email"))
    df["principal_aad_object_id"] = df["SharedWith"].apply(lambda x: (x or {}).get("AadObjectId"))
    df["principal_upn"] = df["SharedWith"].apply(lambda x: (x or {}).get("UPN"))
    df["principal_login_name"] = df["SharedWith"].apply(lambda x: (x or {}).get("UserLoginName"))
    df["principal_user_count"] = df["SharedWith"].apply(lambda x: (x or {}).get("UserCount"))
    df["principal_scope_type"] = df["SharedWithCount"].apply(lambda x: (x or {}).get("Type"))
    df["principal_scope_count"] = df["SharedWithCount"].apply(lambda x: (x or {}).get("Count"))
    df["principal_group_key"] = (
        df["principal_login_name"]
        .fillna(df["principal_display_name"])
        .str.lower()
    )

    silver_df = df[
        [
            "SiteId",
            "SiteUrl",
            "WebId",
            "ListId",
            "ListTitle",
            "ListUrl",
            "ItemId",
            "UniqueId",
            "ItemType",
            "ItemURL",
            "FileExtension",
            "RoleDefinition",
            "LinkId",
            "ScopeId",
            "LinkScope",
            "TotalUserCount",
            "ShareCreatedBy",
            "ShareCreatedTime",
            "ShareLastModifiedBy",
            "ShareLastModifiedTime",
            "ShareExpirationTime",
            "snapshot_date",
            "principal_type",
            "principal_type_v2",
            "principal_display_name",
            "principal_email",
            "principal_aad_object_id",
            "principal_upn",
            "principal_login_name",
            "principal_user_count",
            "principal_scope_type",
            "principal_scope_count",
            "principal_group_key",
        ]
    ].rename(
        columns={
            "SiteId": "site_id",
            "SiteUrl": "site_url",
            "WebId": "web_id",
            "ListId": "list_id",
            "ListTitle": "list_title",
            "ListUrl": "list_url",
            "ItemId": "list_item_id",
            "UniqueId": "unique_item_id",
            "ItemType": "item_type",
            "ItemURL": "item_url",
            "RoleDefinition": "role_definition",
            "TotalUserCount": "total_user_count",
            "ShareCreatedBy": "share_created_by",
            "ShareCreatedTime": "share_created_datetime",
            "ShareLastModifiedBy": "share_last_modified_by",
            "ShareLastModifiedTime": "share_last_modified_datetime",
            "ShareExpirationTime": "share_expiration_datetime",
        }
    )

    return silver_df.drop_duplicates(
        subset=[
            "site_id",
            "list_id",
            "list_item_id",
            "principal_email",
            "principal_aad_object_id",
            "principal_login_name",
            "role_definition",
            "snapshot_date",
        ]
    )


def silver_sharepoint_groups(bronze_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    df = bronze_df.copy()
    df["snapshot_date"] = df["snapshot_date"].astype(str)

    groups_df = pd.DataFrame(
        {
            "sp_group_id": df["GroupId"],
            "site_id": df["SiteId"],
            "sp_group_display_name": df["DisplayName"],
            "sp_group_login_name": df["DisplayName"],
            "sp_group_link_id": df["GroupLinkId"],
            "sp_group_type": df["GroupType"],
            "owner_type": df["Owner"].apply(lambda x: (x or {}).get("Type")),
            "owner_aad_object_id": df["Owner"].apply(lambda x: (x or {}).get("AadObjectId")),
            "owner_display_name": df["Owner"].apply(lambda x: (x or {}).get("Name")),
            "owner_email": df["Owner"].apply(lambda x: (x or {}).get("Email")),
            "owner_type_v2": df["Owner"].apply(lambda x: (x or {}).get("TypeV2")),
            "owner_login_name": df["Owner"].apply(lambda x: (x or {}).get("LoginName")),
            "owner_upn": df["Owner"].apply(lambda x: (x or {}).get("UPN")),
            "snapshot_date": df["snapshot_date"],
        }
    )
    groups_df["group_key"] = (
        groups_df["sp_group_login_name"]
        .fillna(groups_df["sp_group_display_name"])
        .str.lower()
    )

    members_records = []
    for _, row in df.iterrows():
        for member in row.get("Members", []):
            members_records.append(
                {
                    "sp_group_id": row["GroupId"],
                    "site_id": row["SiteId"],
                    "member_type": member.get("Type"),
                    "member_type_v2": member.get("TypeV2"),
                    "member_display_name": member.get("Name"),
                    "member_email": member.get("Email"),
                    "member_login_name": member.get("LoginName"),
                    "member_aad_object_id": member.get("AadObjectId"),
                    "member_upn": member.get("UPN"),
                    "snapshot_date": row["snapshot_date"],
                }
            )
    members_df = pd.DataFrame(members_records)
    return {
        "sharepoint_groups": groups_df.drop_duplicates(subset=["sp_group_id", "snapshot_date"]),
        "sharepoint_group_members": members_df.drop_duplicates(
            subset=["sp_group_id", "member_aad_object_id", "member_upn", "snapshot_date"]
        ),
    }


def silver_group_details(bronze_df: pd.DataFrame) -> pd.DataFrame:
    df = bronze_df.copy()
    df["snapshot_date"] = df["snapshot_date"].astype(str)
    silver = df.rename(
        columns={
            "id": "aad_group_id",
            "displayName": "aad_group_display_name",
            "description": "aad_group_description",
            "mail": "aad_group_mail",
            "mailNickname": "aad_group_mail_nickname",
            "mailEnabled": "mail_enabled",
            "securityEnabled": "security_enabled",
            "groupTypes": "group_types",
            "visibility": "group_visibility",
            "classification": "group_classification",
            "preferredDataLocation": "preferred_data_location",
            "createdDateTime": "group_created_datetime",
            "renewedDateTime": "group_renewed_datetime",
            "deletedDateTime": "group_deleted_datetime",
            "resourceProvisioningOptions": "resource_provisioning_options",
            "preferredLanguage": "preferred_language",
            "isAssignableToRole": "is_assignable_to_role",
        }
    )
    return silver[
        [
            "aad_group_id",
            "aad_group_display_name",
            "aad_group_description",
            "aad_group_mail",
            "aad_group_mail_nickname",
            "mail_enabled",
            "security_enabled",
            "group_types",
            "group_visibility",
            "group_classification",
            "preferred_data_location",
            "group_created_datetime",
            "group_renewed_datetime",
            "group_deleted_datetime",
            "resource_provisioning_options",
            "preferred_language",
            "is_assignable_to_role",
            "snapshot_date",
        ]
    ]


def silver_group_members(bronze_df: pd.DataFrame) -> pd.DataFrame:
    df = bronze_df.copy()
    df["snapshot_date"] = df["snapshot_date"].astype(str)
    df["member_aad_object_id"] = df["puser"].combine_first(df["pObjectId"])
    df = df.rename(
        columns={
            "id": "aad_group_id",
            "userPrincipalName": "member_user_principal_name",
            "displayName": "member_display_name",
        }
    )
    return df[
        [
            "aad_group_id",
            "member_user_principal_name",
            "member_display_name",
            "member_aad_object_id",
            "snapshot_date",
        ]
    ]


def silver_group_owners(bronze_df: pd.DataFrame) -> pd.DataFrame:
    df = bronze_df.copy()
    df["snapshot_date"] = df["snapshot_date"].astype(str)
    df["owner_aad_object_id"] = df["puser"].combine_first(df["pObjectId"])
    df = df.rename(
        columns={
            "id": "aad_group_id",
            "userPrincipalName": "owner_user_principal_name",
            "displayName": "owner_display_name",
        }
    )
    return df[
        [
            "aad_group_id",
            "owner_user_principal_name",
            "owner_display_name",
            "owner_aad_object_id",
            "snapshot_date",
        ]
    ]


def main():
    raw = build_raw_samples()

    bronze_permissions = bronze_from_raw(raw["SharePointPermissions_v1"], "SharePointPermissions_v1")
    bronze_sites = bronze_from_raw(raw["SharePointSites_v1"], "SharePointSites_v1")
    bronze_groups = bronze_from_raw(raw["SharePointGroups_v1"], "SharePointGroups_v1")
    bronze_group_details = bronze_from_raw(raw["GroupDetails_v0"], "GroupDetails_v0")
    bronze_group_members = bronze_from_raw(raw["GroupMembers_v0"], "GroupMembers_v0")
    bronze_group_owners = bronze_from_raw(raw["GroupOwners_v0"], "GroupOwners_v0")
    bronze_graph_users = bronze_from_raw(raw["GraphUsers"], "GraphUsers")

    silver_permissions = silver_sharepoint_permissions(bronze_permissions)
    silver_sites = silver_sharepoint_sites(bronze_sites)
    silver_groups_outputs = silver_sharepoint_groups(bronze_groups)
    silver_sp_groups = silver_groups_outputs["sharepoint_groups"]
    silver_sp_group_members = silver_groups_outputs["sharepoint_group_members"]
    silver_aad_groups = silver_group_details(bronze_group_details)
    silver_aad_group_members = silver_group_members(bronze_group_members)
    silver_aad_group_owners = silver_group_owners(bronze_group_owners)
    silver_graph_users = build_user_dimension(bronze_graph_users)

    print("Bronze SharePointPermissions sample:")
    print(bronze_permissions[["SiteId", "RoleDefinition", "SnapshotDate", "snapshot_date"]])

    print("\nSilver SharePointPermissions flattened sample:")
    print(
        silver_permissions[
            ["site_id", "list_id", "principal_type_v2", "principal_display_name", "role_definition", "snapshot_date"]
        ]
    )

    print("\nSilver SharePointGroups sample:")
    print(
        silver_sp_groups[
            ["sp_group_id", "site_id", "sp_group_display_name", "group_key", "snapshot_date"]
        ]
    )

    print("\nSilver SharePointGroup Members sample:")
    print(
        silver_sp_group_members[
            ["sp_group_id", "member_display_name", "member_email", "snapshot_date"]
        ]
    )

    print("\nSilver AAD Group Members sample:")
    print(
        silver_aad_group_members[
            ["aad_group_id", "member_user_principal_name", "member_display_name", "snapshot_date"]
        ]
    )

    print("\nSilver Graph users dimension sample:")
    print(
        silver_graph_users[
            ["user_aad_object_id", "user_principal_name", "user_job_title", "user_department", "snapshot_date"]
        ]
    )

    permissions_enriched = annotate_permissions(silver_permissions, silver_sites)

    gold_direct = build_direct_entitlements(permissions_enriched)
    gold_sp_group = build_sharepoint_group_entitlements(permissions_enriched, silver_sp_groups, silver_sp_group_members)
    gold_aad = build_aad_group_entitlements(permissions_enriched, silver_aad_groups, silver_aad_group_members)

    gold_combined = pd.concat([gold_direct, gold_sp_group, gold_aad], ignore_index=True, sort=False)
    gold_combined = gold_combined.merge(
        silver_graph_users[
            [
                "snapshot_date",
                "user_aad_object_id",
                "user_job_title",
                "user_department",
                "user_office_location",
                "user_account_enabled",
                "user_company_name",
                "user_country",
                "user_city",
                "user_state",
            ]
        ],
        how="left",
        on=["snapshot_date", "user_aad_object_id"],
    )
    gold_combined = append_common_columns(gold_combined)
    gold_combined = add_lineage_hash(gold_combined)

    print("\nGold entitlements sample:")
    print(
        gold_combined[
            [
                "snapshot_date_key",
                "entitlement_source",
                "user_principal_name",
                "user_job_title",
                "site_display_name",
                "permission_role_name",
                "entitlement_hash",
            ]
        ]
    )


if __name__ == "__main__":
    main()

