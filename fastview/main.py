import json
import os
import pprint
from time import sleep

import boto3
import click

sts = boto3.client("sts")

aws_region = boto3.session.Session().region_name
aws_account_id = sts.get_caller_identity()["Account"]
aws_user = sts.get_caller_identity()["Arn"].split("/")[-1]

qs_client = boto3.client("quicksight")


@click.group()
def cli_entry_point():
    pass


@cli_entry_point.command()
def list_groups():
    group_list = qs_client.list_groups(AwsAccountId=aws_account_id, Namespace="default")
    for x in group_list["GroupList"]:
        member_list = qs_client.list_group_memberships(
            GroupName=x["GroupName"], AwsAccountId=aws_account_id, Namespace="default",
        )["GroupMemberList"]
        print("\nName: ", x["GroupName"])
        print("Description: ", x["Description"])
        print("Members:")
        for member in member_list:
            print("> ", member["MemberName"])


@cli_entry_point.command()
def list_users():
    user_list = qs_client.list_users(AwsAccountId=aws_account_id, Namespace="default")
    print("\nNames of all the users in this AWS account:\n")
    for x in user_list["UserList"]:
        print("UserName: ", x["UserName"])
        print("Arn: ", x["Arn"])
        print()


@cli_entry_point.command()
def list_data_sources():
    data_source_list = qs_client.list_data_sources(AwsAccountId=aws_account_id)
    print(
        "\n[Name  :  ID] for each data source in this AWS account \
(excluding AWS sample data sources and any of type AWS_IOT_ANALYTICS):\n"
    )
    exclusion_list = [
        "Sales Pipeline",
        "Web and Social Media Analytics",
        "Business Review",
        "People Overview",
    ]
    relevant = sorted(
        [
            x
            for x in data_source_list["DataSources"]
            if x["Name"] not in exclusion_list and x["Type"] != "AWS_IOT_ANALYTICS"
        ],
        key=lambda x: x["Name"],
    )
    max_len = max([len(x["Name"]) for x in relevant])
    for x in relevant:
        name = x["Name"]
        ds_id = x["DataSourceId"]
        print(f"{name:<{max_len}} : {ds_id:<{max_len}}")


@cli_entry_point.command()
def list_datasets():
    dataset_list = qs_client.list_data_sets(AwsAccountId=aws_account_id)
    print(
        "\n[Names : ID] for each dataset in this AWS account (excluding AWS sample datasets):\n"
    )
    exclusion_list = [
        "Sales Pipeline",
        "Web and Social Media Analytics",
        "Business Review",
        "People Overview",
    ]
    relevant = sorted(
        [
            x
            for x in dataset_list["DataSetSummaries"]
            if x["Name"] not in exclusion_list
        ],
        key=lambda x: x["Name"],
    )
    max_len = max([len(x["Name"]) for x in relevant])
    for x in relevant:
        name = x["Name"]
        ds_id = x["DataSetId"]
        print(f"{name:<{max_len}} : {ds_id:<{max_len}}")


@cli_entry_point.command()
def list_templates():
    template_list = qs_client.list_templates(AwsAccountId=aws_account_id)
    print("\nAll templates in this AWS account:\n")
    for x in template_list["TemplateSummaryList"]:
        print("\n\n>>> ", x["Name"])
        pprint.pp(x)


@cli_entry_point.command()
def list_dashboards():
    dashboard_list = qs_client.list_dashboards(AwsAccountId=aws_account_id)
    print("\nAll dashboards in this AWS account:\n")
    for x in dashboard_list["DashboardSummaryList"]:
        print("\n\n>>> ", x["Name"])
        pprint.pp(x)


@cli_entry_point.command()
@click.argument("template_name")
def list_template_versions(template_name):
    template_list = qs_client.list_templates(AwsAccountId=aws_account_id)
    matches = [
        temp
        for temp in template_list["TemplateSummaryList"]
        if temp["Name"] == template_name
    ]

    if len(matches) > 1:
        print(f"\nThere are multiple templates with name {template_name}")
        print("This function can handle one at most.")
        return

    elif len(matches) == 0:
        print(f"\nThere are no templates with name {template_name}")
        return

    template_id = matches[0]["TemplateId"]
    response = qs_client.list_template_versions(
        AwsAccountId=aws_account_id, TemplateId=template_id,
    )
    print()
    for x in response["TemplateVersionSummaryList"]:
        print()
        pprint.pp(x)


@cli_entry_point.command()
@click.argument("name")
@click.option("-id", "--data_source_id", type=str)
def describe_data_source(name, data_source_id):
    """Describe data source, by name or (optionally) by ID.
    This function ignores the name if the -id flag is used.

    Arguments:
        name {str} -- Must be unique
        data_source_id {str}
    """
    if data_source_id is None:
        description = _get_data_source_description(name)
    else:
        description = qs_client.describe_data_source(
            AwsAccountId=aws_account_id, DataSourceId=data_source_id,
        )["DataSource"]

    print()
    pprint.pp(description)

    print("\n\n>> Permissions <<")
    response = qs_client.describe_data_source_permissions(
        AwsAccountId=aws_account_id, DataSourceId=description["DataSourceId"]
    )
    for perms in response["Permissions"]:
        print("\nPrincipal: ", perms["Principal"])
        print("\nActions: ")
        pprint.pp(perms["Actions"])


@cli_entry_point.command()
@click.argument("name")
@click.option("-id", "--dataset_id", type=str)
def describe_dataset(name, dataset_id):
    """Describe dataset, by name or (optionally) by ID.
    This function ignores the name if the -id flag is used.

    Arguments:
        name {str} -- Must be unique
        dataset_id {str}
    """
    if dataset_id is None:
        description = _get_dataset_description(name)
    else:
        description = qs_client.describe_data_set(
            AwsAccountId=aws_account_id, DataSetId=dataset_id
        )["DataSet"]

    print()
    pprint.pp(
        {
            k: v
            for k, v in description.items()
            if k not in ["PhysicalTableMap", "LogicalTableMap"]
        }
    )
    print("\nPhysicalTableMap:")
    print("'", json.dumps(description["PhysicalTableMap"]), "'")
    print("\nLogicalTableMap:")
    print("'", json.dumps(description["LogicalTableMap"]), "'")

    print("\n\n>> Permissions <<")
    response = qs_client.describe_data_set_permissions(
        AwsAccountId=aws_account_id, DataSetId=description["DataSetId"]
    )
    for perms in response["Permissions"]:
        print("\nPrincipal: ", perms["Principal"])
        print("\nActions: ")
        pprint.pp(perms["Actions"])


@cli_entry_point.command()
@click.argument("name")
def describe_dashboard(name):
    description = _get_dashboard_description(name)
    print()
    pprint.pp(description)

    print("\n\n>> Permissions <<")
    response = qs_client.describe_dashboard_permissions(
        AwsAccountId=aws_account_id, DashboardId=description["DashboardId"]
    )
    for perms in response["Permissions"]:
        print("\nPrincipal: ", perms["Principal"])
        print("\nActions: ")
        pprint.pp(perms["Actions"])


@cli_entry_point.command()
@click.argument("template_name")
@click.option("-v", "--version", type=int)
def describe_template(template_name, version):
    description = _get_template_description(template_name, version)
    if version is None:
        latest_version = description["Version"]["VersionNumber"]
        print(f"\nLatest version ({latest_version}) of template {template_name}:\n")
    else:
        print(f"\nVersion {version} of template {template_name}:\n")

    pprint.pp(description)


@cli_entry_point.command()
@click.argument("group_name")
@click.argument("description")
def create_group(group_name, description):
    print(f"\nCreating group {group_name}...\n")
    response = qs_client.create_group(
        GroupName=group_name,
        Description=description,
        AwsAccountId=aws_account_id,
        Namespace="default",
    )
    pprint.pp(response)


@cli_entry_point.command()
@click.argument("group_name")
def create_group_of_all_users(group_name):
    """Creates (or updates) a Quicksight group that contains all users in the AWS account.
    Will fail if that group already exists.

    Arguments:
        group_name {str}
    """
    user_list = [
        x["UserName"]
        for x in qs_client.list_users(AwsAccountId=aws_account_id, Namespace="default")[
            "UserList"
        ]
    ]

    print(f"\nCreating group {group_name}...\n")
    response = qs_client.create_group(
        GroupName=group_name,
        Description="All the Quicksight users in this AWS account",
        AwsAccountId=aws_account_id,
        Namespace="default",
    )
    pprint.pp(response)
    print()

    for name in user_list:
        print(f"Adding: {name}")
        response = qs_client.create_group_membership(
            MemberName=name,
            GroupName=group_name,
            AwsAccountId=aws_account_id,
            Namespace="default",
        )
    print("Done!")


@cli_entry_point.command()
@click.argument("data_source_name")
@click.argument("owner_group_name")
@click.argument("redshift_host")
@click.argument("redshift_port")
@click.argument("redshift_database_name")
@click.argument("redshift_database_username")
@click.argument("redshift_database_password")
@click.argument("redshift_vpc_connection_arn")
def create_redshift_data_source(
    data_source_name,
    owner_group_name,
    redshift_host,
    redshift_port,
    redshift_database_name,
    redshift_database_username,
    redshift_database_password,
    redshift_vpc_connection_arn,
):

    owner_group_arn = _get_group_arn(owner_group_name)

    response = qs_client.create_data_source(
        AwsAccountId=aws_account_id,
        DataSourceId=data_source_name,
        Name=data_source_name,
        Type="REDSHIFT",
        DataSourceParameters={
            "RedshiftParameters": {
                "Host": redshift_host,
                "Port": int(redshift_port),
                "Database": redshift_database_name,
            }
        },
        Credentials={
            "CredentialPair": {
                "Username": redshift_database_username,
                "Password": redshift_database_password,
            }
        },
        VpcConnectionProperties={"VpcConnectionArn": redshift_vpc_connection_arn},
        Permissions=[
            {
                "Principal": owner_group_arn,
                "Actions": [
                    "quicksight:UpdateDataSourcePermissions",
                    "quicksight:DescribeDataSource",
                    "quicksight:DescribeDataSourcePermissions",
                    "quicksight:PassDataSource",
                    "quicksight:UpdateDataSource",
                    "quicksight:DeleteDataSource",
                ],
            },
        ],
    )

    pprint.pp(response)


@cli_entry_point.command()
@click.argument("dataset_name")
@click.argument("owner_group_name")
@click.argument("import_mode")
@click.argument("physical_table_map")
@click.argument("logical_table_map")
def create_dataset(
    dataset_name, owner_group_name, import_mode, physical_table_map, logical_table_map,
):
    """Creates dataset

    Arguments:
        dataset_name {str}
        owner_group_name {str}
        import_mode {'SPICE' | 'DIRECT_QUERY'} -- Indicates whether you want to import the data into SPICE or not.
        physical_table_map {str} -- Output from the description of another dataset.
        logical_table_map {str} -- Output from the description of another dataset.
    """

    owner_group_arn = _get_group_arn(owner_group_name)

    response = qs_client.create_data_set(
        AwsAccountId=aws_account_id,
        DataSetId=dataset_name,
        Name=dataset_name,
        PhysicalTableMap=json.loads(physical_table_map),
        LogicalTableMap=json.loads(logical_table_map),
        ImportMode=import_mode,
        Permissions=[
            {
                "Principal": owner_group_arn,
                "Actions": [
                    "quicksight:UpdateDataSetPermissions",
                    "quicksight:DescribeDataSet",
                    "quicksight:DescribeDataSetPermissions",
                    "quicksight:PassDataSet",
                    "quicksight:DescribeIngestion",
                    "quicksight:ListIngestions",
                    "quicksight:UpdateDataSet",
                    "quicksight:DeleteDataSet",
                    "quicksight:CreateIngestion",
                    "quicksight:CancelIngestion",
                ],
            },
        ],
    )

    pprint.pp(response)


@cli_entry_point.command()
@click.argument("user_name")
@click.argument("group_name")
def add_member_to_group(user_name, group_name):
    response = qs_client.create_group_membership(
        MemberName=user_name,
        GroupName=group_name,
        AwsAccountId=aws_account_id,
        Namespace="default",
    )
    print(f"\nAdding user {user_name} to group {group_name}...\n")
    pprint.pp(response)


@cli_entry_point.command()
@click.argument("template_name")
@click.argument("analysis_id")
@click.argument("dataset_name_list", nargs=-1)
@click.argument("version_description")
def create_or_update_template(
    template_name, analysis_id, dataset_name_list, version_description
):
    """Creates a new template, or updates an existing one if it already exists.

    Arguments:
        template_name {str} -- A name for the template.
        analysis_id {str} -- You can get this from the URL of an analysis, after the last slash.
        dataset_name_list {str} -- Name(s) of the dataset(s) that this analysis draws on.
                                        This argument takes an unlimited number of names.
        version_description {str} -- A description of what is new in this version.
    """

    template_list = qs_client.list_templates(AwsAccountId=aws_account_id)
    matches = [
        temp
        for temp in template_list["TemplateSummaryList"]
        if temp["Name"] == template_name
    ]
    dataset_arn_list = [
        _get_dataset_description(name)["Arn"] for name in dataset_name_list
    ]

    dataset_references = [
        {"DataSetPlaceholder": f"{name}_placeholder", "DataSetArn": arn,}
        for name, arn in zip(dataset_name_list, dataset_arn_list)
    ]

    if len(matches) > 1:
        print(f"There are multiple templates with name {template_name}")
        print("This function can handle one at most.")
        return

    elif len(matches) == 0:
        print(f"\nCreating template {template_name}\n")
        response = qs_client.create_template(
            AwsAccountId=aws_account_id,
            TemplateId=template_name,
            Name=template_name,
            SourceEntity={
                "SourceAnalysis": {
                    "Arn": f"arn:aws:quicksight:{aws_region}:{aws_account_id}:analysis/{analysis_id}",
                    "DataSetReferences": dataset_references,
                }
            },
            VersionDescription=version_description,
        )
        pprint.pp(response)
        print(f"\nSuccessfully created {template_name}, Version 1")

    else:
        description = _get_template_description(template_name, None)
        previous_version = description["Version"]["VersionNumber"]

        template_id = matches[0]["TemplateId"]
        print(f"\nUpdating template {template_name}\n")
        response = qs_client.update_template(
            AwsAccountId=aws_account_id,
            TemplateId=template_id,
            Name=template_name,
            SourceEntity={
                "SourceAnalysis": {
                    "Arn": f"arn:aws:quicksight:{aws_region}:{aws_account_id}:analysis/{analysis_id}",
                    "DataSetReferences": dataset_references,
                }
            },
            VersionDescription=version_description,
        )
        pprint.pp(response)
        print(f"\nSuccessfully created {template_name}, Version {previous_version + 1}")


@cli_entry_point.command()
@click.argument("dashboard_name")
@click.argument("template_name")
@click.argument("template_version")
@click.argument("owner_group_name")
@click.argument("viewer_group_name")
def create_or_update_dashboard(
    dashboard_name,
    template_name,
    template_version,
    owner_group_name,
    viewer_group_name,
):
    """Create a dashboard from a template, inferring the name of its dataset.  Will only work
    for templates where the dataset placeholder name is the dataset name + "_placeholder", as
    is the case for any templates generated with this CLI.

    Arguments:
        dashboard_name {str}
        template_name {str}
        template_version {str}
        owner_group_name {str} -- The name of a Quicksight user or group that will be granted
                                full read/write access to this dashboard.
        viewer_group_name {str} -- The name of a Quicksight user or group that will be granted
                                read-only access to this dashboard.
    """

    description = _get_template_description(template_name, int(template_version))
    template_arn = description["Arn"]
    owner_group_arn = _get_group_arn(owner_group_name)
    viewer_group_arn = _get_group_arn(viewer_group_name)
    dataset_name_list = [
        dsc["Placeholder"].replace("_placeholder", "")
        for dsc in description["Version"]["DataSetConfigurations"]
    ]
    dashboard_list = qs_client.list_dashboards(AwsAccountId=aws_account_id)
    matches = [
        ds
        for ds in dashboard_list["DashboardSummaryList"]
        if ds["Name"] == dashboard_name
    ]
    if len(matches) > 1:
        print(f"\nThere are multiple dashboards with name {dashboard_name}:\n")
        for x in matches:
            print(x["Name"])
            pprint.pp(x)
            print()
        return
    elif len(matches) == 0:
        print("\nCreating new dashboard...\n")
        response = _create_dashboard(
            dashboard_name,
            dataset_name_list,
            template_arn,
            owner_group_arn,
            viewer_group_arn,
        )
        pprint.pp(response)
        print(f"\n\nSuccessfully created dashboard {dashboard_name}!\n")
        print(">> Dashboard Permissions <<")
        permission_response = qs_client.describe_dashboard_permissions(
            AwsAccountId=aws_account_id, DashboardId=response["DashboardId"]
        )
        for perms in permission_response["Permissions"]:
            print("\nPrincipal: ", perms["Principal"])
            print("\nActions: ")
            pprint.pp(perms["Actions"])
    else:
        dashboard_id = matches[0]["DashboardId"]
        print("\nDeleting old dashboard...\n")
        response = qs_client.delete_dashboard(
            AwsAccountId=aws_account_id, DashboardId=dashboard_id,
        )
        pprint.pp(response)

        print("\nCreating new dashboard...\n")
        response = _create_dashboard(
            dashboard_name,
            dataset_name_list,
            template_arn,
            owner_group_arn,
            viewer_group_arn,
        )
        pprint.pp(response)
        print(f"\n\nSuccessfully created dashboard {dashboard_name}!\n")
        print(">> Dashboard Permissions <<")
        permission_response = qs_client.describe_dashboard_permissions(
            AwsAccountId=aws_account_id, DashboardId=response["DashboardId"]
        )
        for perms in permission_response["Permissions"]:
            print("\nPrincipal: ", perms["Principal"])
            print("\nActions: ")
            pprint.pp(perms["Actions"])


@cli_entry_point.command()
@click.argument("name")
@click.argument("owner_group_name")
def update_data_source_permissions(name, owner_group_name):
    """Will grant full permissions for a data source to a given user/group,
    without altering any pre-existing permissions.

    Arguments:
        name {str} -- Name of the data source to have its permissions updated
        owner_group_name {str} -- Name of a Quicksight user or group
    """
    data_source_id = _get_data_source_description(name)["DataSourceId"]
    owner_group_arn = _get_group_arn(owner_group_name)

    response = qs_client.update_data_source_permissions(
        AwsAccountId=aws_account_id,
        DataSourceId=data_source_id,
        GrantPermissions=[
            {
                "Principal": owner_group_arn,
                "Actions": [
                    "quicksight:UpdateDataSourcePermissions",
                    "quicksight:DescribeDataSource",
                    "quicksight:DescribeDataSourcePermissions",
                    "quicksight:PassDataSource",
                    "quicksight:UpdateDataSource",
                    "quicksight:DeleteDataSource",
                ],
            },
        ],
    )

    pprint.pp(response)

    print(f"\n\n>> New permissions for {name} <<")
    response = qs_client.describe_data_source_permissions(
        AwsAccountId=aws_account_id, DataSourceId=data_source_id
    )
    for perms in response["Permissions"]:
        print("\nPrincipal: ", perms["Principal"])
        print("\nActions: ")
        pprint.pp(perms["Actions"])


@cli_entry_point.command()
@click.argument("name")
@click.argument("owner_group_name")
def update_dataset_permissions(name, owner_group_name):
    """Will grant full permissions for a dataset to a given user/group,
    without altering any pre-existing permissions.

    Arguments:
        name {str} -- Name of the dataset to have its permissions updated
        owner_group_name {str} -- Name of a Quicksight user or group
    """
    dataset_id = _get_dataset_description(name)["DataSetId"]
    owner_group_arn = _get_group_arn(owner_group_name)

    response = qs_client.update_data_set_permissions(
        AwsAccountId=aws_account_id,
        DataSetId=dataset_id,
        GrantPermissions=[
            {
                "Principal": owner_group_arn,
                "Actions": [
                    "quicksight:UpdateDataSetPermissions",
                    "quicksight:DescribeDataSet",
                    "quicksight:DescribeDataSetPermissions",
                    "quicksight:PassDataSet",
                    "quicksight:DescribeIngestion",
                    "quicksight:ListIngestions",
                    "quicksight:UpdateDataSet",
                    "quicksight:DeleteDataSet",
                    "quicksight:CreateIngestion",
                    "quicksight:CancelIngestion",
                ],
            },
        ],
    )

    pprint.pp(response)

    print(f"\n\n>> New permissions for {name} <<")
    response = qs_client.describe_data_set_permissions(
        AwsAccountId=aws_account_id, DataSetId=dataset_id
    )
    for perms in response["Permissions"]:
        print("\nPrincipal: ", perms["Principal"])
        print("\nActions: ")
        pprint.pp(perms["Actions"])


@cli_entry_point.command()
@click.argument("group_name")
def delete_group(group_name):
    print(f"\nDeleting group {group_name}...\n")
    response = qs_client.delete_group(
        GroupName=group_name, AwsAccountId=aws_account_id, Namespace="default"
    )
    pprint.pp(response)


@cli_entry_point.command()
@click.argument("data_source_id")
@click.confirmation_option(prompt="Are you sure you want to delete this data source?")
def delete_data_source(data_source_id):
    print(f"\nDeleting data source with ID: {data_source_id}\n")
    response = qs_client.delete_data_source(
        AwsAccountId=aws_account_id, DataSourceId=data_source_id,
    )
    pprint.pp(response)


@cli_entry_point.command()
@click.argument("dashboard_name")
@click.confirmation_option(prompt="Are you sure you want to delete this dashboard?")
def delete_dashboard(dashboard_name):
    dashboard_list = qs_client.list_dashboards(AwsAccountId=aws_account_id)
    matches = [
        ds
        for ds in dashboard_list["DashboardSummaryList"]
        if ds["Name"] == dashboard_name
    ]
    if len(matches) > 1:
        print(f"\nThere are multiple dashboards with name {dashboard_name}")
        print("This function can handle one at most.")
        return
    elif len(matches) == 0:
        print(f"\nThere are no dashboards with name {dashboard_name}")
        return

    dashboard_id = matches[0]["DashboardId"]

    print(f"\nDeleting Dashboard with ID: {dashboard_id}\n")
    response = qs_client.delete_dashboard(
        AwsAccountId=aws_account_id, DashboardId=dashboard_id,
    )
    pprint.pp(response)


@cli_entry_point.command()
@click.argument("template_name")
@click.confirmation_option(prompt="Are you sure you want to delete this template?")
def delete_template(template_name):
    template_list = qs_client.list_templates(AwsAccountId=aws_account_id)
    matches = [
        temp
        for temp in template_list["TemplateSummaryList"]
        if temp["Name"] == template_name
    ]

    if len(matches) > 1:
        print(f"\nThere are multiple templates with name {template_name}")
        print("This function can handle one at most.")
        return

    elif len(matches) == 0:
        print(f"\nThere are no templates with name {template_name}")
        return

    template_id = matches[0]["TemplateId"]

    print(f"\nDeleting template with ID: {template_id}\n")
    response = qs_client.delete_template(
        AwsAccountId=aws_account_id, TemplateId=template_id,
    )
    pprint.pp(response)


def _create_dashboard(
    dashboard_name, dataset_name_list, template_arn, owner_group_arn, viewer_group_arn,
):
    dataset_arn_list = [
        _get_dataset_description(name)["Arn"] for name in dataset_name_list
    ]
    dataset_references = [
        {"DataSetPlaceholder": f"{name}_placeholder", "DataSetArn": arn,}
        for name, arn in zip(dataset_name_list, dataset_arn_list)
    ]
    read_write_permission = [
        {
            "Principal": owner_group_arn,
            "Actions": [
                "quicksight:DescribeDashboard",
                "quicksight:ListDashboardVersions",
                "quicksight:UpdateDashboardPermissions",
                "quicksight:QueryDashboard",
                "quicksight:UpdateDashboard",
                "quicksight:DeleteDashboard",
                "quicksight:DescribeDashboardPermissions",
                "quicksight:UpdateDashboardPublishedVersion",
            ],
        },
    ]

    read_only_permission = [
        {
            "Principal": viewer_group_arn,
            "Actions": [
                "quicksight:DescribeDashboard",
                "quicksight:ListDashboardVersions",
                "quicksight:QueryDashboard",
            ],
        },
    ]

    if owner_group_arn == viewer_group_arn:
        permissions = read_write_permission
    else:
        permissions = read_write_permission + read_only_permission

    response = qs_client.create_dashboard(
        AwsAccountId=aws_account_id,
        DashboardId=dashboard_name,
        Name=dashboard_name,
        Permissions=permissions,
        SourceEntity={
            "SourceTemplate": {
                "DataSetReferences": dataset_references,
                "Arn": template_arn,
            },
        },
        DashboardPublishOptions={
            "AdHocFilteringOption": {"AvailabilityStatus": "ENABLED"},
            "ExportToCSVOption": {"AvailabilityStatus": "ENABLED"},
            "SheetControlsOption": {"VisibilityState": "EXPANDED"},
        },
    )
    return response


def _get_dashboard_description(name):
    dashboard_list = qs_client.list_dashboards(AwsAccountId=aws_account_id)
    matches = [
        ds for ds in dashboard_list["DashboardSummaryList"] if ds["Name"] == name
    ]
    if len(matches) <= 0:
        print(f"\nNo dashboards have the name {name}")
        raise Exception("No dashboards with that name.")
    elif len(matches) > 1:
        print(f"\nMultiple dashboards have the name {name}:\n")
        for x in matches:
            print(x["Name"])
            print(x)
            print()
        raise Exception("Multiple dashboards with the same name (see list above).")
    else:
        dashboard_id = matches[0]["DashboardId"]

    response = qs_client.describe_dashboard(
        AwsAccountId=aws_account_id, DashboardId=dashboard_id,
    )
    return response["Dashboard"]


def _get_data_source_description(name):
    data_source_list = qs_client.list_data_sources(AwsAccountId=aws_account_id)
    matches = [ds for ds in data_source_list["DataSources"] if ds["Name"] == name]
    if len(matches) <= 0:
        print(f"\nNo data sources have the name {name}")
        raise Exception("No data sources with that name.")
    elif len(matches) > 1:
        print(f"\nMultiple data sources have the name {name}\n")
        for x in matches:
            print(x["Name"])
            pprint.pp(x)
            print()
        raise Exception("Multiple data sources with the same name (see list above).")
    else:
        data_source_id = matches[0]["DataSourceId"]

    response = qs_client.describe_data_source(
        AwsAccountId=aws_account_id, DataSourceId=data_source_id,
    )
    return response["DataSource"]


def _get_dataset_description(dataset_name):
    dataset_list = qs_client.list_data_sets(AwsAccountId=aws_account_id)
    matches = [
        ds for ds in dataset_list["DataSetSummaries"] if ds["Name"] == dataset_name
    ]
    if len(matches) <= 0:
        print(f"\nNo datasets have the name {dataset_name}\n")
        raise Exception("No datasets with that name.")
    elif len(matches) > 1:
        print(f"\nMultiple datasets have the name {dataset_name}\n")
        for x in matches:
            print(x["Name"])
            pprint.pp(x)
            print()
        raise Exception("Multiple datasets with the same name (see list above).")
    else:
        dataset_id = matches[0]["DataSetId"]

    response = qs_client.describe_data_set(
        AwsAccountId=aws_account_id, DataSetId=dataset_id
    )
    return response["DataSet"]


def _get_template_description(template_name, version):
    template_list = qs_client.list_templates(AwsAccountId=aws_account_id)
    matches = [
        temp
        for temp in template_list["TemplateSummaryList"]
        if temp["Name"] == template_name
    ]

    if len(matches) > 1:
        print(f"\nMultiple templates have the name {template_name}\n")
        for x in matches:
            print(x["Name"])
            pprint.pp(x)
            print()
        raise Exception("Multiple templates with the same name (see list above).")

    elif len(matches) == 0:
        print(f"\nThere are no templates with name {template_name}")
        raise Exception("No templates with that name.")
    else:
        template_id = matches[0]["TemplateId"]

    if version is None:
        response = qs_client.describe_template(
            AwsAccountId=aws_account_id, TemplateId=template_id,
        )
    else:
        response = qs_client.describe_template(
            AwsAccountId=aws_account_id, TemplateId=template_id, VersionNumber=version,
        )

    return response["Template"]


def _get_group_arn(group_name):
    return qs_client.describe_group(
        GroupName=group_name, AwsAccountId=aws_account_id, Namespace="default",
    )["Group"]["Arn"]


if __name__ == "__main__":
    cli_entry_point()
