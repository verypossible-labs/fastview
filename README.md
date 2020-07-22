# FastView

## This project is part of [Very labs](https://github.com/verypossible-labs/docs/blob/master/README.md).

---
FastView is a handy wrapper for the AWS QuickSight CLI.

[QuickSight](https://aws.amazon.com/quicksight/) (QS) is an AWS tool for creating dashboards tied to data from AWS resources.  You build up to a dashboard through the following progression:

data source -> dataset -> analysis -> template (optional) -> dashboard

You can make all these resources (except templates) in the QS console, or you can make them all (except analyses) through the native AWS CLI.  For better reproducibility, and some amount of version control, we recommend doing as much as possible through the CLI.  FastView is designed to help with that, by filling in several defaults and providing faster access to common commands.

We recommend using the console to create data sources, datasets, and analyses, and using FastView to create and manage user groups, templates, and dashboards.  We've added limited commands for managing and re-creating data sources and datasets. 

In QS, resources must have a unique ID but can have repeated names.  In order to make everything simpler, FastView commands will always create resources with the same name and ID.


## General Guidelines
1. Every analysis should be saved as a template. Templates are the only way to preserve analysis history in QuickSight. Keep a single analysis for `stage` and `prod`, but different templates and dashboards for each.
2. Avoid deleting templates; make new versions instead.
3. Avoid deleting analyses even more.  It's easy to recreate everything downstream of an analysis, but we can't recover analyses.
4. The URL for a dashboard will remain immutable throughout updates, so long as it has the same ID.
5. For any new dataset, the scheduled refreshing must be set in the console.
6. After making any new data sources or datasets, whether through the CLI or the console, update their permissions so that other QS users/groups have access to them too.
7. Before using FastView, make sure that your `.envrc` file is exporting the credentials for the AWS account you want to use.


## How to run commands
Run all these functions with the command `fv`.  Note that underscores in function names must be replaced with dashes because of how Click handles names. So for example, the function `describe_dashboard` has this definition:

```python
@cli_entry_point.command()
@click.argument("name")
def describe_dashboard(name):
    ...
```

... and is run like this:
```
$ fv describe-dashboard KPI1
```

## How to create a QS user group
Create user groups in order to manage access to QS resources for the users of this AWS account.
1. Use `list_groups` and `list_users`.
2. If there isn't one already, use `create_group_of_all_users` and call it `everyone`.
3. If needed, use `create_group` to make a new empty group, and `add_member_to_group` to populate it. One good practice is to have a group called `admins`, to receive read/write permissions for all resources.


## How to make or modify a data source
1. From the QS console home, click `Manage data` (top right).
2. Click `New Dataset`.
3. For making a new data source, select an AWS service (Redshift, etc) and follow the wizard.
4. For modifying a data source, scroll to the header `FROM EXISTING DATA SOURCES` at the bottom, select an existing data source, and click `Edit data source`.
5. After making a data source, use `update_data_source_permissions` to give other groups/users access to it.


## How to publish a new dashboard to stage and prod
1. Use the console to create a dataset.  Make different ones for `stage` and `prod`, and then use the `update_dataset_permissions` to give other QS users/groups access to the new datasets.
2. Use the console to create an analysis for `stage`, and copy its unique ID.  It's the long number after `/analyses/` in the URL (e.g., `9504cub3-yr62-4f34-5e90-76c6827e070d`).  Note that if you're looking at a particular sheet, the analysis ID will appear before the string `/sheets/<sheetID>`.
3. Take stock of current QS resources with the `list` and `describe` commands.
4. Run `publish_analysis`.
5. Inspect the new `stage` dashboard in the console.
6. Go back to the analysis, click on the pencil next to `Data set`, and replace the `stage` dataset with the `prod` one.  This shouldn't affect the analysis, provided that both datasets have the same variables.  QS will warn you of loss of undo/redo history, and might show an error screen.  Refresh the page and verify that the dataset was replaced with `prod`.
7. Run `publish_analysis` for the `prod` dataset.


## How to update a dashboard
1. Use the console to make any necessary updates to datasets or analyss.
2. Run `publish_analysis`.


## How to duplicate an analysis/dashboard
1. In the dashboard, click `Share > Share dashboard > Manage dashboard access` and make sure that the `Save as` checkbox is checked for your user.
2. Back in the dashboard, click `Save as` to create a new analysis from this dashboard.
3. Edit the analysis or change the dataset as needed, then publish a dashboard from it.

