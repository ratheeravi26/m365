import hashlib
import pandas as pd


def build_sample_data():
    sharepoint_permissions = pd.DataFrame(
        [
            {
                "snapshot_date": "20240115",
                "site_id": "site-001",
                "site_url": "https://contoso.sharepoint.com/sites/hr",
                "list_id": "list-hr",
                "list_item_id": None,
                "unique_item_id": None,
                "item_type": "Site",
                "item_url": None,
                "role_definition": "Full Control",
                "principal_type_v2": "internaluser",
                "principal_type": "User",
                "principal_display_name": "Adele Vance",
                "principal_email": "adele.vance@contoso.com",
                "principal_aad_object_id": "user-adele",
                "principal_upn": "adele.vance@contoso.com",
                "principal_login_name": "i:0#.f|membership|adele.vance@contoso.com",
                "principal_user_count": 1,
                "principal_group_key": None,
                "share_created_by": None,
                "share_created_datetime": None,
                "share_last_modified_by": None,
                "share_last_modified_datetime": None,
                "share_expiration_datetime": None,
                "total_user_count": 1,
                "principal_scope_type": None,
                "principal_scope_count": None,
            },
            {
                "snapshot_date": "20240115",
                "site_id": "site-001",
                "site_url": "https://contoso.sharepoint.com/sites/hr",
                "list_id": "list-hr",
                "list_item_id": None,
                "unique_item_id": None,
                "item_type": "Site",
                "item_url": None,
                "role_definition": "Read",
                "principal_type_v2": "sharepointgroup",
                "principal_type": "SharePointGroup",
                "principal_display_name": "HR Owners",
                "principal_email": None,
                "principal_aad_object_id": None,
                "principal_upn": None,
                "principal_login_name": "HR Owners",
                "principal_user_count": 5,
                "principal_group_key": "hr owners",
                "share_created_by": None,
                "share_created_datetime": None,
                "share_last_modified_by": None,
                "share_last_modified_datetime": None,
                "share_expiration_datetime": None,
                "total_user_count": 5,
                "principal_scope_type": None,
                "principal_scope_count": None,
            },
            {
                "snapshot_date": "20240115",
                "site_id": "site-002",
                "site_url": "https://contoso.sharepoint.com/sites/finance",
                "list_id": "list-fin",
                "list_item_id": None,
                "unique_item_id": None,
                "item_type": "Site",
                "item_url": None,
                "role_definition": "Contribute",
                "principal_type_v2": "externaluser",
                "principal_type": "User",
                "principal_display_name": "Megan Bowen",
                "principal_email": "megan.bowen@contoso.com",
                "principal_aad_object_id": "user-megan",
                "principal_upn": "megan.bowen@contoso.com",
                "principal_login_name": "i:0#.f|membership|megan.bowen@contoso.com",
                "principal_user_count": 1,
                "principal_group_key": None,
                "share_created_by": "Finance Admin",
                "share_created_datetime": "2024-01-14T12:00:00Z",
                "share_last_modified_by": "Finance Admin",
                "share_last_modified_datetime": "2024-01-14T12:00:00Z",
                "share_expiration_datetime": None,
                "total_user_count": 1,
                "principal_scope_type": None,
                "principal_scope_count": None,
            },
            {
                "snapshot_date": "20240115",
                "site_id": "site-002",
                "site_url": "https://contoso.sharepoint.com/sites/finance",
                "list_id": "list-fin",
                "list_item_id": None,
                "unique_item_id": None,
                "item_type": "Site",
                "item_url": None,
                "role_definition": "Read",
                "principal_type_v2": "securitygroup",
                "principal_type": "Group",
                "principal_display_name": "Finance Analysts",
                "principal_email": "financeanalysts@contoso.com",
                "principal_aad_object_id": "aad-fin-analysts",
                "principal_upn": None,
                "principal_login_name": None,
                "principal_user_count": 12,
                "principal_group_key": None,
                "share_created_by": None,
                "share_created_datetime": None,
                "share_last_modified_by": None,
                "share_last_modified_datetime": None,
                "share_expiration_datetime": None,
                "total_user_count": 12,
                "principal_scope_type": None,
                "principal_scope_count": None,
            },
        ]
    )

    sharepoint_sites = pd.DataFrame(
        [
            {
                "snapshot_date": "20240115",
                "site_id": "site-001",
                "site_title": "HR Portal",
                "site_template": "SITEPAGEPUBLISHING#0",
                "site_privacy": "Private",
                "sensitivity_label_name": "Confidential",
                "is_hub_site": False,
                "is_teams_connected_site": True,
            },
            {
                "snapshot_date": "20240115",
                "site_id": "site-002",
                "site_title": "Finance Workspace",
                "site_template": "STS#3",
                "site_privacy": "Private",
                "sensitivity_label_name": "Highly Confidential",
                "is_hub_site": False,
                "is_teams_connected_site": False,
            },
        ]
    )

    sharepoint_groups = pd.DataFrame(
        [
            {
                "snapshot_date": "20240115",
                "sp_group_id": "spg-hr-owners",
                "site_id": "site-001",
                "sp_group_display_name": "HR Owners",
                "sp_group_login_name": "HR Owners",
                "group_key": "hr owners",
            },
        ]
    )

    sharepoint_group_members = pd.DataFrame(
        [
            {
                "snapshot_date": "20240115",
                "sp_group_id": "spg-hr-owners",
                "member_aad_object_id": "user-nestor",
                "member_display_name": "Nestor Wilke",
                "member_email": "nestor.wilke@contoso.com",
                "member_login_name": "i:0#.f|membership|nestor.wilke@contoso.com",
                "member_upn": "nestor.wilke@contoso.com",
            },
            {
                "snapshot_date": "20240115",
                "sp_group_id": "spg-hr-owners",
                "member_aad_object_id": "user-isaiah",
                "member_display_name": "Isaiah Langer",
                "member_email": "isaiah.langer@contoso.com",
                "member_login_name": "i:0#.f|membership|isaiah.langer@contoso.com",
                "member_upn": "isaiah.langer@contoso.com",
            },
        ]
    )

    aad_groups = pd.DataFrame(
        [
            {
                "snapshot_date": "20240115",
                "aad_group_id": "aad-fin-analysts",
                "aad_group_display_name": "Finance Analysts",
                "security_enabled": True,
            },
        ]
    )

    aad_group_members = pd.DataFrame(
        [
            {
                "snapshot_date": "20240115",
                "aad_group_id": "aad-fin-analysts",
                "member_aad_object_id": "user-julia",
                "member_user_principal_name": "julia.cole@contoso.com",
                "member_display_name": "Julia Cole",
            },
            {
                "snapshot_date": "20240115",
                "aad_group_id": "aad-fin-analysts",
                "member_aad_object_id": "user-lidia",
                "member_user_principal_name": "lidia.holloway@contoso.com",
                "member_display_name": "Lidia Holloway",
            },
        ]
    )

    graph_users = pd.DataFrame(
        [
            {
                "snapshot_date": "20240115",
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
                "snapshot_date": "20240115",
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
                "snapshot_date": "20240115",
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
                "snapshot_date": "20240115",
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
                "snapshot_date": "20240115",
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
                "snapshot_date": "20240115",
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
        ]
    )

    return {
        "permissions": sharepoint_permissions,
        "sites": sharepoint_sites,
        "sp_groups": sharepoint_groups,
        "sp_group_members": sharepoint_group_members,
        "aad_groups": aad_groups,
        "aad_group_members": aad_group_members,
        "graph_users": graph_users,
    }


def build_user_dimension(users_df: pd.DataFrame) -> pd.DataFrame:
    dim = users_df.copy()
    dim["snapshot_date"] = dim["snapshot_date"].astype(str)
    dim = dim.rename(
        columns={
            "id": "user_aad_object_id",
            "userPrincipalName": "user_principal_name",
            "displayName": "user_display_name",
            "mail": "user_mail",
            "jobTitle": "user_job_title",
            "department": "user_department",
            "officeLocation": "user_office_location",
            "accountEnabled": "user_account_enabled",
            "companyName": "user_company_name",
            "country": "user_country",
            "city": "user_city",
            "state": "user_state",
            "createdDateTime": "user_created_datetime",
            "lastPasswordChangeDateTime": "user_last_password_change",
        }
    )
    columns = [
        "snapshot_date",
        "user_aad_object_id",
        "user_principal_name",
        "user_display_name",
        "user_mail",
        "user_job_title",
        "user_department",
        "user_office_location",
        "user_account_enabled",
        "user_company_name",
        "user_country",
        "user_city",
        "user_state",
        "user_created_datetime",
        "user_last_password_change",
    ]
    dim = dim[columns].drop_duplicates(["user_aad_object_id", "snapshot_date"])
    return dim


def annotate_permissions(perms_df: pd.DataFrame, sites_df: pd.DataFrame) -> pd.DataFrame:
    merged = perms_df.merge(
        sites_df,
        how="left",
        on=["snapshot_date", "site_id"],
        suffixes=("", "_site"),
    )
    merged["site_url_resolved"] = merged["site_url"]
    merged["site_title_resolved"] = merged["site_title"].fillna(merged["site_id"])
    merged["sharepoint_object_type"] = merged.apply(
        lambda row: "ListItem"
        if pd.notna(row["list_item_id"])
        else "List"
        if pd.notna(row["list_id"])
        else "Site",
        axis=1,
    )
    merged["sharepoint_object_id"] = merged.apply(
        lambda row: row["unique_item_id"]
        if pd.notna(row["unique_item_id"])
        else row["list_id"]
        if pd.notna(row["list_id"])
        else row["site_id"],
        axis=1,
    )
    merged["sharepoint_object_url"] = merged.apply(
        lambda row: row["item_url"]
        if pd.notna(row["item_url"])
        else row["site_url"],
        axis=1,
    )
    return merged


def build_direct_entitlements(perms_df: pd.DataFrame) -> pd.DataFrame:
    user_types = {"internaluser", "externaluser", "b2buser"}
    direct = perms_df[perms_df["principal_type_v2"].str.lower().isin(user_types)].copy()
    direct["entitlement_source"] = "Direct"
    direct["entitlement_source_id"] = direct[
        ["principal_aad_object_id", "principal_upn", "principal_login_name", "principal_email"]
    ].bfill(axis=1).iloc[:, 0]
    direct["entitlement_source_display_name"] = direct[
        ["principal_display_name", "principal_email", "principal_upn"]
    ].bfill(axis=1).iloc[:, 0]
    direct["user_aad_object_id"] = direct["principal_aad_object_id"]
    direct["user_principal_name"] = direct[
        ["principal_upn", "principal_login_name", "principal_email"]
    ].bfill(axis=1).iloc[:, 0]
    direct["user_display_name"] = direct["principal_display_name"]
    direct["user_email"] = direct["principal_email"].combine_first(direct["principal_upn"])
    return direct


def build_sharepoint_group_entitlements(
    perms_df: pd.DataFrame, groups_df: pd.DataFrame, members_df: pd.DataFrame
) -> pd.DataFrame:
    group_types = {"sharepointgroup"}
    sp_perms = perms_df[perms_df["principal_type_v2"].str.lower().isin(group_types)].copy()

    groups_prepared = groups_df[["snapshot_date", "site_id", "sp_group_id", "sp_group_display_name", "group_key"]]
    members_prepared = members_df[["snapshot_date", "sp_group_id", "member_aad_object_id", "member_display_name", "member_email", "member_upn"]]

    joined = sp_perms.merge(
        groups_prepared,
        left_on=["snapshot_date", "site_id", "principal_group_key"],
        right_on=["snapshot_date", "site_id", "group_key"],
        how="inner",
    ).merge(
        members_prepared,
        on=["snapshot_date", "sp_group_id"],
        how="inner",
    )

    joined["entitlement_source"] = "SharePointGroup"
    joined["entitlement_source_id"] = joined["sp_group_id"].combine_first(joined["principal_login_name"])
    joined["entitlement_source_display_name"] = joined["sp_group_display_name"].combine_first(joined["principal_display_name"])
    joined["user_aad_object_id"] = joined["member_aad_object_id"]
    joined["user_principal_name"] = joined["member_upn"]
    joined["user_display_name"] = joined["member_display_name"]
    joined["user_email"] = joined["member_email"]
    return joined


def build_aad_group_entitlements(
    perms_df: pd.DataFrame, aad_groups_df: pd.DataFrame, aad_members_df: pd.DataFrame
) -> pd.DataFrame:
    aad_types = {"group", "securitygroup", "azureadgroup"}
    aad_perms = perms_df[perms_df["principal_type_v2"].str.lower().isin(aad_types)].copy()

    groups_prepared = aad_groups_df[["snapshot_date", "aad_group_id", "aad_group_display_name"]]
    members_prepared = aad_members_df[
        ["snapshot_date", "aad_group_id", "member_aad_object_id", "member_user_principal_name", "member_display_name"]
    ]

    joined = aad_perms.merge(
        groups_prepared,
        left_on=["snapshot_date", "principal_aad_object_id"],
        right_on=["snapshot_date", "aad_group_id"],
        how="inner",
    ).merge(
        members_prepared,
        on=["snapshot_date", "aad_group_id"],
        how="inner",
    )

    joined["entitlement_source"] = "AzureADGroup"
    joined["entitlement_source_id"] = joined["aad_group_id"]
    joined["entitlement_source_display_name"] = joined["aad_group_display_name"].combine_first(
        joined["principal_display_name"]
    )
    joined["user_aad_object_id"] = joined["member_aad_object_id"]
    joined["user_principal_name"] = joined["member_user_principal_name"]
    joined["user_display_name"] = joined["member_display_name"]
    joined["user_email"] = joined["member_user_principal_name"]
    return joined


def append_common_columns(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "snapshot_date",
        "entitlement_source",
        "entitlement_source_id",
        "entitlement_source_display_name",
        "user_aad_object_id",
        "user_principal_name",
        "user_display_name",
        "user_email",
        "user_job_title",
        "user_department",
        "user_office_location",
        "user_account_enabled",
        "user_company_name",
        "user_country",
        "user_city",
        "user_state",
        "site_id",
        "site_url_resolved",
        "site_title_resolved",
        "sharepoint_object_type",
        "sharepoint_object_id",
        "sharepoint_object_url",
        "role_definition",
    ]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = None
    df = df[keep_cols]
    df = df.rename(
        columns={
            "snapshot_date": "snapshot_date_key",
            "site_url_resolved": "site_url",
            "site_title_resolved": "site_display_name",
            "role_definition": "permission_role_name",
        }
    )
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date_key"], format="%Y%m%d").dt.date
    return df


def add_lineage_hash(df: pd.DataFrame) -> pd.DataFrame:
    def make_hash(row):
        components = [
            str(row["snapshot_date_key"]),
            row["user_aad_object_id"] or "NA",
            row["user_principal_name"] or "NA",
            row["entitlement_source"] or "NA",
            row["entitlement_source_id"] or "NA",
            row["sharepoint_object_id"] or "NA",
            row["permission_role_name"] or "NA",
        ]
        return hashlib.sha256("||".join(components).encode("utf-8")).hexdigest()

    df["entitlement_hash"] = df.apply(make_hash, axis=1)
    return df


def build_gold_from_samples():
    data = build_sample_data()
    permissions_enriched = annotate_permissions(data["permissions"], data["sites"])
    user_dim = build_user_dimension(data["graph_users"])

    direct = build_direct_entitlements(permissions_enriched)
    sp_group = build_sharepoint_group_entitlements(
        permissions_enriched, data["sp_groups"], data["sp_group_members"]
    )
    aad_group = build_aad_group_entitlements(
        permissions_enriched, data["aad_groups"], data["aad_group_members"]
    )

    combined = pd.concat([direct, sp_group, aad_group], ignore_index=True, sort=False)
    combined = combined.merge(
        user_dim[
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
    combined = append_common_columns(combined)
    combined = add_lineage_hash(combined)
    combined = combined.sort_values(["entitlement_source", "user_principal_name"]).reset_index(drop=True)
    return combined


if __name__ == "__main__":
    gold_df = build_gold_from_samples()
    print("Gold entitlement sample output:")
    print(gold_df)
