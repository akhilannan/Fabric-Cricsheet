# # Import Libraries


from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from urllib.request import urlretrieve
from notebookutils import mssparkutils
from sempy import fabric
from sempy.fabric.exceptions import FabricHTTPException
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import pathlib
from zipfile import ZipFile
import os
import datetime
import dateutil
import pytz
import time
import uuid
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import json
import base64
import math


# # Initialize Spark Session


spark = SparkSession.builder.getOrCreate()


# # Get Fabric Items


def get_fabric_items(item_name=None, item_type=None, workspace_id=fabric.get_workspace_id()):
    """
    Retrieve fabric items based on optional filters for name and type within a workspace.

    Args:
        item_name: Optional; the display name of the item to filter by.
        item_type: Optional; the type of the item to filter by.
        workspace_id: Optional; the workspace ID to search within. Defaults to the current workspace.
    
    Returns: A pandas dataframe of fabric items matching the criteria.
    """
    # Build the query conditions
    conditions = []
    if item_name:
        conditions.append(f"`Display Name` == '{item_name}'")
    if item_type:
        conditions.append(f"`Type` == '{item_type}'")

    # Construct the query string
    query = " and ".join(conditions)

    # Perform the query if conditions exist, otherwise list all items
    items = fabric.list_items(workspace=workspace_id)
    return items.query(query) if conditions else items


# # Get Lakehouse ID


def get_lakehouse_id(lakehouse_name: str) -> int:
    """Returns the Id of a Lakehouse item given its display name.

    Args:
        lakehouse_name: The display name of the Lakehouse item.

    Returns:
        The Id of the Lakehouse item as an integer.

    Raises:
        ValueError: If no Lakehouse item with the given display name is found.
    """

    # Filter the list by matching the display name with the given argument
    query_result = get_fabric_items(item_name= lakehouse_name, item_type = 'Lakehouse')

    # Check if the query result is empty
    if query_result.empty:
        # If yes, raise an exception with an informative message
        raise ValueError(f"No Lakehouse item with display name '{lakehouse_name}' found.")
        
    # If no, return the first value of the Id column as an integer
    return query_result.Id.values[0]


# # Create Lakehouse


def create_lakehouse_if_not_exists(lh_name: str) -> str:
    # Use a docstring to explain the function's purpose and parameters
    """Creates a lakehouse with the given name if it does not exist already.

    Args:
        lh_name (str): The name of the lakehouse to create.

    Returns:
        str: The ID of the lakehouse.
    """
    try:
        # Use the fabric API to create a lakehouse and return its ID
        return fabric.create_lakehouse(lh_name)
    except FabricHTTPException as exc:
        # If the lakehouse already exists, return its ID
        return get_lakehouse_id(lh_name)


# # Create Mount Point


def create_mount_point(abfss_path: str, mount_point: str = "/lakehouse/default") -> str:
    """Creates a mount point for an Azure Blob Storage path and returns the local path.

    Args:
        abfss_path (str): The Azure Blob Storage path to mount.
        mount_point (str, optional): The mount point to use. Defaults to "/lakehouse/default".

    Returns:
        str: The local path of the mount point.

    Raises:
        ValueError: If the mount point is already in use or invalid.
    """
    
    # Check if the mount point exists in fs.mounts
    if any(m.mountPoint == mount_point for m in mssparkutils.fs.mounts()):
        # Return the local path of the existing mount point
        return next(m.localPath for m in mssparkutils.fs.mounts() if m.mountPoint == mount_point)
    else:
        # Mount the One Lake path
        mssparkutils.fs.mount(abfss_path, mount_point)

        # Return the local path of the new mount point
        return next(m.localPath for m in mssparkutils.fs.mounts() if m.mountPoint == mount_point)


# # Get Lakehouse path


def get_lakehouse_path(lakehouse_name: str, path_type: str = "spark", folder_type: str = "Tables") -> str:
    """Returns the path to a lakehouse folder based on the lakehouse name, path type, and folder type.

    Parameters:
    lakehouse_name (str): The name of the lakehouse.
    path_type (str): The type of the path, either "spark" or "local". Defaults to "spark".
    folder_type (str): The type of the folder, either "Tables" or "Files". Defaults to "Tables".

    Returns:
    str: The path to the lakehouse folder.

    Raises:
    ValueError: If the lakehouse name, path type, or folder type is invalid.
    """
    # Validate the parameters
    if not lakehouse_name:
        raise ValueError("Lakehouse name cannot be empty.")
    if path_type not in ["spark", "local"]:
        raise ValueError(f"Invalid path type: {path_type}.")
    if folder_type not in ["Tables", "Files"]:
        raise ValueError(f"Invalid folder type: {folder_type}.")

    # Create the lakehouse if it does not exist
    lakehouse_id = create_lakehouse_if_not_exists(lakehouse_name)

    # Get the workspace id
    workspace_id = fabric.get_workspace_id()
    abfss_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com"
    abfss_lakehouse_path = os.path.join(abfss_path, lakehouse_id)

    # Construct the path based on the path type
    if path_type == "spark":
        return os.path.join(abfss_lakehouse_path, folder_type)
    elif path_type == "local":
        local_path = create_mount_point(abfss_lakehouse_path, f"/lakehouse/{lakehouse_name}")
        return os.path.join(local_path, folder_type)


# # Delete a Lakehouse item


def delete_path(lakehouse, item, folder_type= "Tables"):
    """Deletes the folder or file if it exists.

    Args:
        path (str): The path of the folder or file to be deleted.
    """
    path = get_lakehouse_path(lakehouse, path_type = "spark", folder_type = folder_type)
    path_item = os.path.join(path, item)
    if mssparkutils.fs.exists(path_item):
        mssparkutils.fs.rm(path_item, True)
    else:
        print(f"Path does not exist: {path_item}")


# # Download Data from URL


def download_data(url, lakehouse, path):
    """
    Downloads a zip file from the base URL and extracts it to the lake path.

    Parameters
    ----------
    path : str
        The path of the directory where the data will be stored.
    url : str
        The URL of the zip file to be downloaded.
    lakehouse: str
        Name of Lakehouse

    Returns
    -------
    None
    """
    # Create a lake path
    lake_path = os.path.join(get_lakehouse_path(lakehouse, "local", "Files"), path)

    # Create a file name from the base URL
    file_path = os.path.join(lake_path, os.path.basename(url))

    # Create a directory for the lake path if it does not exist
    os.makedirs(lake_path, exist_ok=True)

    # Download the data from the base URL and save the file in the path
    urlretrieve(url, file_path)


# # Check if Delta Table Exists


def delta_table_exists(lakehouse_name: str, tbl: str) -> bool:
  """Check if a delta table exists at the given path.

  Parameters:
  path (str): The directory where the delta table is stored.
  tbl (str): The name of the delta table.

  Returns:
  bool: True if the delta table exists, False otherwise.
  """
  try:
    # Use the DeltaTable class to access the delta table
    path = get_lakehouse_path(lakehouse_name)
    DeltaTable.forPath(spark, os.path.join(path, tbl))
    # If no exception is raised, the delta table exists
    return True
  except Exception as e:
    # If an exception is raised, the delta table does not exist
    return False


# # Read a Delta Table in Lakehouse


def read_delta_table(lakehouse_name: str, table_name: str) -> DataFrame:
    """Reads a delta table from a given path and table name.

    Args:
        lakehouse_name (str): Name of the Lakehouse where the table exists.
        table_name (str): The name of the delta table.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with the delta table data.
    """
    path = get_lakehouse_path(lakehouse_name)
    return (
        spark
        .read
        .format('delta')
        .load(f"{path}/{table_name}")
    )


# # Create or Replace Delta Table


def create_or_replace_delta_table(df: DataFrame, lakehouse_name: str, table_name: str, mode_type: str = "overwrite") -> None:
    """Create or replace a delta table from a dataframe.

    Args:
        df (pyspark.sql.DataFrame): The dataframe to write to the delta table.
        lakehouse_name (str): Lakehouse where delta table is stored.
        table_name (str): The name of the delta table.
        mode_type (str, optional): The mode for writing the delta table. Defaults to "overwrite".

    Returns:
        None
    """
    path = get_lakehouse_path(lakehouse_name)
    (
        df
        .write
        .mode(mode_type)
        .option("mergeSchema", "true")
        .format("delta")
        .save(f'{path}/{table_name}')
    )


# # Upsert Delta table


def upsert_delta_table(lakehouse_name: str, table_name: str, df: DataFrame, merge_condition: str, update_condition: dict = None) -> None:
    """Updates or inserts rows into a delta table with the given data frame and conditions.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        table_name (str): The name of the delta table.
        df (pyspark.sql.DataFrame): The data frame to merge with the delta table.
        merge_condition (str): The condition to match the rows for merging.
        update_condition (dict, optional): The dictionary of column names and values to update when matched. Defaults to None.

    Returns:
        None
    """
    table_path = get_lakehouse_path(lakehouse_name)
    delta_table = DeltaTable.forPath(spark, os.path.join(table_path, table_name))
    if update_condition is None:
        # If update_condition is None, just insert new rows when not matched
        (
            delta_table.alias('t')
            .merge(df.alias('s'), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # Otherwise, update existing rows when matched and insert new rows when not matched
        (
            delta_table.alias('t')
            .merge(df.alias('s'), merge_condition)
            .whenMatchedUpdate(set = update_condition)
            .execute()
        )


# # Create or Upsert data to Delta table


def create_or_insert_table(df: DataFrame, lakehouse_name: str, table_name: str, primary_key: str, merge_key: str) -> None:
    """Create or insert a delta table from a dataframe.

    Args:
        df (DataFrame): The dataframe to be inserted or used to create the table.
        lakehouse_name (str): The name of the lakehouse where the table is located.
        table_name (str): The name of the table to be created or inserted.
        primary_key (str): The name of the column that serves as the primary key of the table.
        merge_key (str): The name of the column that serves as the merge key of the table.
    """
    if delta_table_exists(lakehouse_name, table_name):
        # Get the maximum value of the primary key from the existing table
        max_primary_key = read_delta_table(lakehouse_name, table_name).agg(F.max(primary_key)).collect()[0][0] + 1
        # Increment the primary key of the dataframe by the maximum value
        df = df.withColumn(primary_key, F.col(primary_key) + F.lit(max_primary_key))
        # Format the merge condition with the merge key
        merge_condition = f"t.{merge_key} = s.{merge_key}"
        # Upsert the dataframe into the existing table
        upsert_delta_table(lakehouse_name, table_name, df, merge_condition)
    else:
        # Create a new table from the dataframe
        create_or_replace_delta_table(df, lakehouse_name, table_name)


# # Get row count of a table


def get_row_count(lakehouse_or_link: str, table_name: str = None) -> int:
    """Get the row count from a delta table or a web page.

    Args:
        table_path: The path of the table or web page.
        table_name: The name of the table. Defaults to None.

    Returns:
        The row count as an integer.
    """
    # If the table name is None, assume the table path is a web page
    if table_name is None:
        # Extract the row count from the HTML table
        try:
            row_count = int(
                pd
                .read_html(lakehouse_or_link)[1]
                .All
                .str
                .extract("([\\d,]+)")
                .iloc[0, 0]
                .replace(",", "")
            )
        except ValueError:
            print("Invalid row count in the web page")
            return
    else:
        # Get the row count from the delta table
        row_count = read_delta_table(lakehouse_or_link, table_name).count()
    
    return row_count


# # Compare row count


def compare_row_count(table1_lakehouse: str, table1_name: str, table2_lakehouse: str, table2_name: str = None) -> None:
    """Compare the row count of two tables and exit or print the difference.

    This function compares the row count of two delta tables or a delta table and a web page.
    It exits the notebook with message "No new data" if the row counts are equal, or prints the difference otherwise.

    Args:
        table1_lakehouse: The lakehouse of the first table.
        table1_name: The name of the first table.
        table2_lakehouse: The lakehouse of the second table or web page.
        table2_name: The name of the second table. Defaults to None.

    Returns:
        None
    """
    
    # Check if the first table exists as a delta table
    if delta_table_exists(table1_lakehouse, table1_name):
        # Get the row count and the match IDs from the first table
        row_count_1 = get_row_count(table1_lakehouse, table1_name)
        
        # Get the row count from the second table or web page
        row_count_2 = get_row_count(table2_lakehouse, table2_name)
        
        # Compare the row counts and exit or print accordingly
        if row_count_1 == row_count_2:
            # If the row counts are equal, exit the notebook with message "No new data"
            mssparkutils.notebook.exit("No new data")
        else:
            print(f"Cricsheet has {row_count_2 - row_count_1} more matches added")


# # Unzip files from an archive


def unzip_files(zip_filename: str, filenames: list[str], path: str) -> None:
    """Unzip a batch of files from a zip file to a given path.

    Args:
        zip_filename (str): The name of the zip file.
        filenames (list[str]): The list of filenames to unzip.
        path (str): The destination path for the unzipped files.
    """
    # Open the zip file
    with ZipFile(zip_filename, 'r') as handle:
        # Unzip a batch of files
        handle.extractall(path=path, members=filenames)


# # Unzip a large number of files in parallel


def unzip_parallel(lakehouse: str, path: str, filename: str, file_type: str = None) -> None:
    """Unzip all files from a zip file to a given path in parallel.

    Args:
        lakehouse (str): Name of the Lakehouse
        path (str): The destination path for the unzipped files.
        filename (str): The name of the zip file without the path.
        file_type (str): The file type to extract. Defaults to None.
    """
    # Create a lake path
    lake_path = os.path.join(get_lakehouse_path(lakehouse, "local", "Files"), path)
    # join the path and the filename to get the full zip file path
    zip_filename = os.path.join(lake_path, filename)
    # check if the zip file exists
    if os.path.exists(zip_filename):
        # open the zip file
        with ZipFile(zip_filename, 'r') as handle:
            # list of all files to unzip
            files = handle.namelist()
        # filter the files by file type if not None
        if file_type is not None:
            files = [f for f in files if f.endswith(file_type)]
        # determine chunksize
        n_workers = 80
        chunksize = round(len(files) / n_workers)
        # start the thread pool
        with ProcessPoolExecutor(n_workers) as exe:
            # split the copy operations into chunks
            for i in range(0, len(files), chunksize):
                # select a chunk of filenames
                filenames = files[i:(i + chunksize)]
                # submit the batch copy task
                _ = exe.submit(unzip_files, zip_filename, filenames, lake_path)
        # remove the zip file
        os.remove(zip_filename)
    else:
        # print an error message
        print(f"The zip file {zip_filename} does not exist.")


# # Get first team to Bat or Field


def first_team(toss_decision, team_player_schema):
  """
  Returns the name of the first team to play based on the toss decision.

  Parameters:
  toss_decision (str): The decision of the toss winner to bat or field.

  Returns:
  str: The name of the first team to play.
  """
  # Extract the team names from the JSON column "team_players" using the schema "team_player_schema"
  teams = F.map_keys(F.from_json(F.col("team_players"), team_player_schema))
  # Assign the first and second team names to variables
  first_team_name = teams[0]
  second_team_name =  teams[1]
  # Return the name of the first team to play based on the toss decision and the toss winner
  return (F.when(F.col("toss_decision") == toss_decision, F.col("toss_winner"))
          .when((F.col("toss_winner") == first_team_name), second_team_name)
          .otherwise(first_team_name))


# # Convert a string of refresh items to json


def convert_to_json(refresh_objects: str) -> list:
    """Converts a string of refresh objects to a list of dictionaries.

    Args:
        refresh_objects (str): A string of refresh objects, separated by "|".
            Each refresh object consists of a table name and optional partitions, separated by ":".
            Partitions are comma-separated. 
            eg. "Table1:Partition1,Partition2|Table2"

    Returns:
        list: A list of dictionaries, each with a "table" key and an optional "partition" key.
    """
    result = [] # Initialize an empty list to store the converted dictionaries
    if refresh_objects is None or refresh_objects == "All":
        return result # Return an empty list if the input is None or "All"
    for item in refresh_objects.split("|"): # Loop through each refresh object, separated by "|"
        table, *partitions = item.split(":") # Split the item by ":" and assign the first element to table and the rest to partitions
        if partitions: # If there are any partitions
            # Extend the result list with a list comprehension that creates a dictionary for each partition
            # The dictionary has the table name and the partition name as keys
            # The partition name is stripped of any leading or trailing whitespace
            result.extend([{"table": table, "partition": partition.strip()} for partition in ",".join(partitions).split(",")])
        else: # If there are no partitions
            # Append a dictionary with only the table name as a key to the result list
            result.append({"table": table})
    return result # Return the final list of dictionaries


# # Call Enhanced Refresh API


def start_enhanced_refresh(
    dataset_name: str,
    workspace_name: str = fabric.get_workspace_id(),
    refresh_objects: str = "All",
    refresh_type: str = "full",
    commit_mode: str = "transactional",
    max_parallelism: int = 10,
    retry_count: int = 0,
    apply_refresh_policy: bool = False,
    effective_date: datetime.date = datetime.date.today(),
) -> str:
    """Starts an enhanced refresh of a dataset.

    Args:
        dataset_name: The name of the dataset to refresh.
        workspace_name: The name of the workspace where the dataset is located. Defaults to the current workspace.
        refresh_objects: The objects to refresh in the dataset. Can be "All" or a list of object names. Defaults to "All".
        refresh_type: The type of refresh to perform. Can be "full" or "incremental". Defaults to "full".
        commit_mode: The commit mode to use for the refresh. Can be "transactional" or "streaming". Defaults to "transactional".
        max_parallelism: The maximum number of parallel threads to use for the refresh. Defaults to 10.
        retry_count: The number of times to retry the refresh in case of failure. Defaults to 0.
        apply_refresh_policy: Whether to apply the refresh policy defined in the dataset. Defaults to False.
        effective_date: The date to use for the refresh. Defaults to today.

    Returns:
        The refresh request id.

    Raises:
        FabricException: If the refresh fails or encounters an error.
    """
    objects_to_refresh = convert_to_json(refresh_objects)
    return fabric.refresh_dataset(
        workspace=workspace_name,
        dataset=dataset_name,
        objects=objects_to_refresh,
        refresh_type=refresh_type,
        max_parallelism=max_parallelism,
        commit_mode=commit_mode,
        retry_count=retry_count,
        apply_refresh_policy=apply_refresh_policy,
        effective_date=effective_date,
    )


# # Get Enhanced Refresh Details


def get_enhanced_refresh_details(dataset_name: str, refresh_request_id: str, workspace_name: str = fabric.resolve_workspace_name(fabric.get_workspace_id())) -> pd.DataFrame:
    """Get enhanced refresh details for a given dataset and refresh request ID.

    Args:
        dataset_name (str): The name of the dataset.
        refresh_request_id (str): The ID of the refresh request.
        workspace_name (str, optional): The name of the workspace. Defaults to name workspace where Notebook reside.

    Returns:
        pd.DataFrame: A dataframe with the refresh details, messages, and time taken in seconds.
    """
    # Get the refresh execution details from fabric
    refresh_details = fabric.get_refresh_execution_details(workspace=workspace_name, dataset=dataset_name, refresh_request_id=refresh_request_id)
    
    # Create a dataframe with the refresh details
    df = pd.DataFrame({
        'workspace': [workspace_name],
        'dataset': [dataset_name],
        'start_time': [refresh_details.start_time],
        'end_time': [refresh_details.end_time if refresh_details.end_time is not None else None],
        'status': [refresh_details.status],
        'extended_status': [refresh_details.extended_status],
        'number_of_attempts': [refresh_details.number_of_attempts]
    })

    # Calculate the time taken in seconds and add it as a new column
    df['duration_in_sec'] = (df['end_time'].fillna(df['start_time']) - df['start_time']).dt.total_seconds()
    df['duration_in_sec'] = df['duration_in_sec'].astype(int)
    
    # Convert the start_time and end_time columns to strings with the date format
    df['start_time'] = df['start_time'].astype(str).str.slice(0, 16)
    df['end_time'] = df['end_time'].astype(str).str.slice(0, 16)
    
    # Create a dataframe with the refresh messages
    df_msg = pd.DataFrame(refresh_details.messages)
    
    # Add the dataset column to df_msg
    df_msg = df_msg.assign(dataset=dataset_name)
    
    # Merge the dataframes on the dataset column and return the result
    return df.merge(df_msg, how='outer', on='dataset')


# # Cancel Enhanced Refresh


def cancel_enhanced_refresh(request_id: str, dataset_id: str, workspace_id: str = fabric.get_workspace_id()) -> dict:
    """Cancel an enhanced refresh request for a Power BI dataset.

    Args:
        request_id (str): The ID of the refresh request to cancel.
        dataset_id (str): The ID of the dataset to cancel the refresh for.
        workspace_id (str, optional): The ID of the workspace containing the dataset. Defaults to the current workspace.

    Returns:
        dict: The JSON response from the Power BI REST API.

    Raises:
        FabricHTTPException: If the request fails with a non-200 status code.
    """
    
    # Create a Power BI REST client object
    client = fabric.PowerBIRestClient()

    # Construct the endpoint URL for the delete request
    endpoint = f"v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{request_id}"

    # Send the delete request and get the response
    response = client.delete(endpoint)

    # Check the status code and raise an exception if not 200
    if response.status_code != 200:
        raise FabricHTTPException(response)

    # Return the JSON response as a dictionary
    return response.json()


# # Check if a Semantic Model exists in the workspace


def is_dataset_exists(dataset: str, workspace: str = fabric.get_workspace_id()) -> bool:
    """Check if a dataset exists in a given workspace.

    Args:
        dataset (str): The name of the dataset to check.
        workspace (str, optional): The ID or Name of the workspace to search in. Defaults to the current workspace.

    Returns:
        bool: True if the dataset exists, False otherwise.
    """
    return not fabric.list_datasets(workspace).query(f"`Dataset Name` == '{dataset}'").empty


# # Synchronous Enhanced refresh of datasets


def refresh_and_wait(
    dataset_list: list[str],
    workspace: str = fabric.get_workspace_id(),
    logging_lakehouse: str = None,
    parent_job_name: str = None,
    job_category: str = "Adhoc",
) -> None:
    """
    Waits for enhanced refresh of given datasets.

    Args:
      dataset_list: List of datasets to refresh.
      workspace: The workspace ID where the datasets are located. Defaults to the current workspace.
      logging_lakehouse: The name of the lakehouse where the job information will be logged. Defaults to None.
      parent_job_name: The name of the parent job that triggered the refresh. Defaults to None.

    Returns:
      None

    Raises:
      Exception: If any of the datasets failed to refresh.
    """

    # Filter out the datasets that do not exist
    valid_datasets = [
                dataset
                for dataset in dataset_list
                if is_dataset_exists(dataset, workspace)
            ]

    # Start the enhanced refresh for the valid datasets
    request_ids = {
        dataset: start_enhanced_refresh(dataset, workspace)
        for dataset in valid_datasets
    }

    # Check if logging_lakehouse has value
    if logging_lakehouse:
        # Loop through the request_ids dictionary
        for dataset, request_id in request_ids.items():
            # Log entries in logging table
            insert_or_update_job_table(
                lakehouse_name=logging_lakehouse,
                job_name=dataset,
                parent_job_name=parent_job_name,
                request_id=request_id,
                job_category=job_category,
            )

    # Print the datasets that do not exist
    invalid_datasets = set(dataset_list) - set(valid_datasets)
    if invalid_datasets:
        print(f"The following datasets do not exist: {', '.join(invalid_datasets)}")

    # Loop until all the requests are completed
    request_status_dict = {} # Initialize an empty dictionary to store the request status of each dataset
    while True:
        for dataset, request_id in request_ids.copy().items():
            # Get the status and details of the current request
            request_status_df = get_enhanced_refresh_details(dataset, request_id, workspace)
            request_status = request_status_df["status"].iloc[0]

            # If the request is not unknown, print the details, store the status in the dictionary, and remove it from the request_ids
            if request_status != "Unknown":
                if logging_lakehouse:
                    duration = request_status_df["duration_in_sec"].iloc[0]
                    msg = request_status_df["Message"].iloc[0]
                    msg = None if pd.isna(msg) else msg
                    insert_or_update_job_table(
                        lakehouse_name=logging_lakehouse,
                        job_name=dataset,
                        parent_job_name=parent_job_name,
                        request_id=request_id,
                        status=request_status,
                        duration=duration,
                        job_category=job_category,
                        message=msg,
                    )
                print(request_status_df.to_markdown())
                request_status_dict[dataset] = request_status  # Store the status in the dictionary
                del request_ids[dataset]

        # If there are no more requests, exit the loop
        if not request_ids:
            # Check if any of the datasets failed to refresh
            failed_datasets = [
                dataset
                for dataset, status in request_status_dict.items()
                if status == "Failed"
            ]
            # If there are any failed datasets, raise an exception with the list of them
            if failed_datasets:
                raise Exception(f"The following datasets failed to refresh: {', '.join(failed_datasets)}")
            break  # Exit the loop
        # Otherwise, wait for 30 seconds before checking again
        else:
            time.sleep(30)


# # Get Job ID


def get_job_id(lakehouse = None, table = None, job_name = None) -> str:
    """Returns a job id based on the type and the delta table.

    Args:
        lakehouse (str, optional): The lakehouse of the delta table. Defaults to None.
        table (str, optional): The name of the delta table. Defaults to None.
        job_name (str, optional): The name of the job. Defaults to None.
        type (str, optional): The type of the job id. Can be "Random" or "Latest" or "Latest Parent". Defaults to "Random".

    Returns:
        str: The job id as a string.
    """
    job_id = str(uuid.uuid4()) # default to random job id
    if lakehouse and table and job_name:
        try:
            job_id = read_delta_table(lakehouse, table).filter(F.col("job_name") == job_name).orderBy(F.desc("start_time")).first()["job_id"]
        except Exception as e:
            print(f"Returning a random id because of this error: {e}")
            pass # ignore any errors
    return job_id


# # Insert or Update Log table


def insert_or_update_job_table(
    lakehouse_name: str,
    job_name: str,
    job_category: str = "Adhoc",
    status: str = "InProgress",
    parent_job_name: str = None,
    request_id: str = None,
    message: str = None,
    duration: int = None,
) -> None:
    """
    Inserts or updates a row in the job table with the given parameters.

    Args:
        lakehouse_name: The name of Lakehouse where job table has to be stored.
        job_name: The name of the job.
        job_category: Category of a job. Defaults to Adhoc
        parent_job_name: Name of the parent job. Defaults to None.
        request_id: The request ID for the job. If None, a random job ID will be generated. Defaults to None.
        status: The status of the job. If "InProgress", the job will be inserted as a new row in the table, otherwise updated. Defaults to "InProgress".
        message: Captures error message or other messaages if any
    """
    # Check if lakehouse_name is None and return None if it is
    if lakehouse_name is None:
        return None

    table_name = "t_job"
    # Get the job ID and the parent job ID
    job_id = request_id or (
        get_job_id()
        if status == "InProgress"
        else get_job_id(lakehouse_name, table_name, job_name)
    )
    parent_job_id = (
        get_job_id(lakehouse_name, table_name, parent_job_name)
        if parent_job_name
        else None
    )

    # Create a dataframe with the job details
    job_df = spark.createDataFrame(
        [
            (
                job_id,
                parent_job_id,
                job_name,
                job_category,
                datetime.datetime.today(),
                None,
                status,
                message,
            )
        ],
        schema="job_id string, parent_job_id string, job_name string, job_category string, start_time timestamp, end_time timestamp, status string, message string",
    )

    # Insert or update the job table based on the status column
    if status == "InProgress":
        create_or_replace_delta_table(job_df, lakehouse_name, table_name, "append")
    else:
        job_df = job_df.withColumn("end_time", F.lit(datetime.datetime.today()))
        end_time = f"t.start_time + INTERVAL {duration} SECONDS" if duration else "s.end_time"
        merge_condition = "s.job_id = t.job_id"
        update_condition = {
            "end_time": end_time,
            "status": "s.status",
            "message": "s.message",
        }
        upsert_delta_table(lakehouse_name, table_name, job_df, merge_condition, update_condition)


# # Execute DAG


def execute_dag(dag):
    """Run multiple notebooks in parallel and return the results or raise an exception."""
    results = mssparkutils.notebook.runMultiple(dag)
    errors = [] # A list to store the errors
    for job, data in results.items(): # Loop through the dictionary
        if data["exception"]: # Check if there is an exception for the job
            errors.append(f"{job}: {data['exception']}") # Add the error message to the list
    if errors: # If the list is not empty
        raise Exception("\n".join(errors)) # Raise an exception with the comma-separated errors
    else:
        return results


# # Execute and Log


def execute_and_log(function: callable, log_lakehouse: str = None, job_name: str = None, job_category: str = None, parent_job_name: str = None, request_id: str = None, **kwargs) -> any:
    """Executes a given function and logs the result to a lakehouse table.

    Args:
        function: The function to execute.
        log_lakehouse: The name of the lakehouse to log to.
        job_name: The name of the job to log.
        job_category: Category of a job
        parent_job_name: The name of the parent job, if any.
        request_id: The request ID for the job, if any.
        **kwargs: The keyword arguments to pass to the function.

    Returns:
        The result of the function execution.

    Raises:
        Exception: If the function execution fails.
    """
    result = None
    # check if log_lakehouse is None
    if log_lakehouse is not None:
        # call the insert_or_update_job_table function
        insert_or_update_job_table(lakehouse_name=log_lakehouse, job_name=job_name, job_category = job_category, parent_job_name=parent_job_name, request_id = request_id)
    try:
        result = function(**kwargs) # assign the result of the function call to a variable
        # check if log_lakehouse is None
        if log_lakehouse is not None:
            # call the insert_or_update_job_table function
            insert_or_update_job_table(lakehouse_name=log_lakehouse, job_name=job_name, status="Completed")
    except Exception as e:
        msg = str(e)
        status = 'Completed' if msg == "No new data" else "Failed"
        # check if log_lakehouse is None
        if log_lakehouse is not None:
            # call the insert_or_update_job_table function
            insert_or_update_job_table(lakehouse_name=log_lakehouse, job_name=job_name, status=status, message=msg)
        raise e
    return result


# # Get Tables in a Lakehouse


def get_tables_in_lakehouse(lakehouse_name):
    """Returns a list of tables in the given lakehouse.

    Args:
        lakehouse_name (str): The name of the lakehouse.

    Returns:
        list: A list of table names, or an empty list if the lakehouse does not exist or is empty.
    """
    try:
        table_list = [file.name for file in mssparkutils.fs.ls(get_lakehouse_path(lakehouse_name))]
    except Exception as e:
        print(e)
        table_list = []
    return table_list


# # Optimize and Vacuum Table using Table Maintenance API


def optimize_and_vacuum_table_api(lakehouse_name: str, table_name: str, workspace_id: str = fabric.get_workspace_id()) -> str:
    """Optimize and vacuum a table in a lakehouse.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        table_name (str): The name of the table.
        workspace_id (str, optional): The ID of the workspace. Defaults to fabric.get_workspace_id().

    Returns:
        Optional[str]: The operation ID of the job instance, or None if an error occurs.
    """
    client = fabric.FabricRestClient()
    lakehouse_id = get_lakehouse_id(lakehouse_name)
    payload = {
        "executionData": {
            "tableName": table_name,
            "optimizeSettings": {
                "vOrder": True
            },
            "vacuumSettings": {
                "retentionPeriod": "7.01:00:00"
            }
        }
    }
    try:
        response = client.post(f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/jobs/instances?jobType=TableMaintenance", json= payload)
        if response.status_code != 202:
            raise FabricHTTPException(response)
        operation_id = response.headers['Location'].split('/')[-1]
        return operation_id
    except FabricHTTPException as e:
        print(e)
        return None


# # Optimize and Vacuum Table


def optimize_and_vacuum_table(lakehouse_name: str, table_name: str, retain_hours: int = None) -> bool:
  """
  Optimizes and vacuums a Delta table in a lakehouse.

  Args:
      lakehouse_name: Name of the lakehouse containing the table.
      table_name: Name of the table to optimize and vacuum.
      retain_hours: Optional number of hours to retain deleted data files.

  Returns:
      True on success, False on failure.
  """

  full_table_name = f"{lakehouse_name}.{table_name}"
  retain_syntax = ""
  if retain_hours is not None:
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
    retain_syntax = f" RETAIN {retain_hours} HOURS"

  try:
    spark.sql(f"OPTIMIZE {full_table_name}")
    spark.sql(f"VACUUM {full_table_name}{retain_syntax}")
  except Exception as e:
    raise Exception(f"Error optimizing and vacuuming {full_table_name}: {e}")


# # Get Lakehouse Job Status


def get_lakehouse_job_status(operation_id: str, lakehouse_name: str, workspace_id: str = fabric.get_workspace_id()) -> dict:
    """Returns the status of a lakehouse job given its operation ID and lakehouse name.

    Args:
        operation_id (str): The ID of the lakehouse job operation.
        lakehouse_name (str): The name of the lakehouse.
        workspace_id (str, optional): The ID of the workspace. Defaults to None.

    Returns:
        dict: A dictionary containing the operation status.

    Raises:
        FabricHTTPException: If the response status code is not 200.
    """
    client = fabric.FabricRestClient()
    lakehouse_id = get_lakehouse_id(lakehouse_name)
    try:
        response = client.get(f"/v1/workspaces/{workspace_id}/items/{lakehouse_id}/jobs/instances/{operation_id}")
        if response.status_code in (200, 403):
            return response.json()
        else:
            raise FabricHTTPException(response)
    except FabricHTTPException as e:
            print(e)


# # Parse items to Optimize/Vacuum


def prepare_optimization_items(items_to_optimize_vacuum, retain_hours=None):
    """
    Prepares a list of items for optimizing and vacuuming tables in a lakehouse.

    Parameters:
    items_to_optimize_vacuum (dict or str): A dictionary with lakehouse names as keys and lists of table names as values. If a string is provided, it's evaluated to a dictionary or converted into one with a None value.
    retain_hours (int, optional): The number of hours to retain the data during the vacuum process. Defaults to None.

    Returns:
    list: A list of tuples, each containing the lakehouse name, table name, and retain_hours for the optimization task.
    """
    # Evaluate string input to a dictionary or encapsulate it in a dictionary
    if isinstance(items_to_optimize_vacuum, str):
        try:
            items_to_optimize_vacuum = ast.literal_eval(items_to_optimize_vacuum)
        except ValueError:
            items_to_optimize_vacuum = {items_to_optimize_vacuum: None}
    
    items = []
    # Iterate over the lakehouses and their respective tables
    for lakehouse_name, table_list in items_to_optimize_vacuum.items():
        # If no table list is provided, retrieve all tables in the lakehouse
        table_list = table_list or get_tables_in_lakehouse(lakehouse_name)
        # Ensure the table_list is a list even if a single table name is provided
        table_list = [table_list] if isinstance(table_list, str) else table_list
        # Create a task for each table
        for table_name in table_list:
            items.append((lakehouse_name, table_name, retain_hours))
    
    return items


# # Optimize and Vacuum multiple items using Table Maintenance API


def optimize_and_vacuum_items_api(items_to_optimize_vacuum: str | dict, log_lakehouse: str = None, job_category: str = None, parent_job_name: str = None, parallelism: int = 3) -> None:
    """Optimize and vacuum tables in lakehouses.

    Args:
        items_to_optimize_vacuum: A dictionary of lakehouse names and table lists, or a string that can be evaluated to a dictionary, or a single lakehouse name.
        log_lakehouse: The name of the lakehouse where the job details will be logged, if any.
        job_category: The category of the job, if any.
        parent_job_name: The name of the parent job, if any.
        parallelism: The number of tables to optimize and vacuum concurrently, default is 3.
    """
    # Create a queue to store the pending tables
    queue = [(item[0], item[1]) for item in prepare_optimization_items(items_to_optimize_vacuum)]
    
    # Optimize and vacuum each table in each lakehouse and store the operation ids
    job_details = {}
    # Create a list to store the running tables
    running = []
    operation_status_dict = {}
    # Loop until the queue is empty and the running list is empty
    while queue or running:
        # If the running list is not full and the queue is not empty, pop a table from the queue and start the operation
        while len(running) < parallelism and queue:
            lakehouse_name, table_name = queue.pop(0)
            operation_id = optimize_and_vacuum_table_api(lakehouse_name, table_name)
            if log_lakehouse:
                insert_or_update_job_table(
                    lakehouse_name=log_lakehouse,
                    job_name=f"{lakehouse_name}.{table_name}",
                    parent_job_name=parent_job_name,
                    request_id=operation_id,
                    job_category=job_category,
                )
            job_details[lakehouse_name, table_name] = operation_id
            running.append((lakehouse_name, table_name, operation_id))
        # Check the status of the running tables and remove the ones that are finished
        for (lakehouse_name, table_name, operation_id) in running.copy():
            operation_details = get_lakehouse_job_status(operation_id, lakehouse_name)
            operation_status = operation_details['status']
            if operation_status not in ["NotStarted", "InProgress"]:
                running.remove((lakehouse_name, table_name, operation_id))
                operation_status_dict[lakehouse_name, table_name] = operation_status
                if log_lakehouse:
                    start_time = dateutil.parser.isoparse(operation_details['startTimeUtc'])
                    end_time = dateutil.parser.isoparse(operation_details['endTimeUtc'])
                    duration = (end_time - start_time).total_seconds()
                    msg = operation_details['failureReason']
                    insert_or_update_job_table(
                        lakehouse_name=log_lakehouse,
                        job_name=f"{lakehouse_name}.{table_name}",
                        parent_job_name=parent_job_name,
                        request_id=operation_id,
                        status=operation_status,
                        duration=duration,
                        job_category=job_category,
                        message=msg
                    )
    
    # Raise an exception if any table failed to optimize
    failed_tables = [f"{lakehouse_name}.{table_name}" for (lakehouse_name, table_name), status in operation_status_dict.items() if status == 'Failed']
    if failed_tables:
        raise Exception(f"The following tables failed to optimize: {', '.join(failed_tables)}")


# # Optimize and Vacuum multiple items


def optimize_and_vacuum_items(items_to_optimize_vacuum: str | dict, retain_hours: int = None, parallelism: int = 4) -> None:
    """
    Optimizes and vacuums Delta tables in parallel across multiple lakehouses.

    Args:
        items_to_optimize_vacuum: A string representing a single item (lakehouse.table)
                                    or a dictionary where keys are lakehouse names
                                    and values are lists of tables (str) or None
                                    to get all tables from that lakehouse.
        retain_hours: Optional number of hours to retain deleted data files.
        parallelism: Number of parallel threads to use for optimization/vacuum tasks.
    """

    # Prepare tasks
    tasks = prepare_optimization_items(items_to_optimize_vacuum)

    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = []
        for task in tasks:
            futures.append(executor.submit(optimize_and_vacuum_table, *task))
        for future in as_completed(futures):
            # No explicit return value handling, potential errors handled in worker function
            pass

    print("Optimization and vacuum completed.")


# # Get SQL Analytics Server and DB Details of a Lakehouse


def get_server_db(lakehouse_name: str, workspace_id: str = fabric.get_workspace_id()) -> tuple:
    """
    Retrieves the server and database details for a given lakehouse.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        workspace_id (str, optional): The workspace ID. Defaults to Current workspace id.

    Returns:
        tuple: A tuple containing the SQL Analytics server connection string and database ID.
    """

    client = fabric.FabricRestClient()
    lakehouse_id = get_lakehouse_id(lakehouse_name)
    response = client.get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}")
    
    if response.status_code == 200:
        response_json = response.json()
        sql_end_point = response_json['properties']['sqlEndpointProperties']
        server = sql_end_point['connectionString']
        db = sql_end_point['id']
        return server, db
    else:
        raise Exception(f"Failed to get lakehouse details: {response.status_code}")


# # Build a Shared Expression M code


def get_shared_expression(lakehouse_name: str, workspace_id: str = fabric.get_workspace_id()) -> str:
    """
    This function generates the shared expression statement for a given lakehouse and its SQL endpoint.

    Args:
        lakehouse_name (str): An optional parameter to set the lakehouse. This defaults to the lakehouse attached to the notebook.        
        workspace_id (str, optional): An optional parameter to set the workspace in which the lakehouse resides. This defaults to the workspace in which the notebook resides.

    Returns:
        This function returns an M statement which can be used as the expression in the shared expression for a Direct Lake semantic model.
    """

    server, db = get_server_db(lakehouse_name, workspace_id)
    m_statement = 'let\n\tdatabase = Sql.Database("' + server + '", "' + db + '")\nin\n\tdatabase'
    return m_statement


# # Repoint Semantic Model to a Lakehouse


def update_model_expression(dataset_name: str, lakehouse_name: str, workspace_id: str = fabric.get_workspace_id()) -> None:
    """
    Update the expression in the semantic model to point to the specified lakehouse.

    Args:
    - dataset_name (str): The name of the dataset.
    - lakehouse_name (str): The name of the lakehouse.
    - workspace_id (str, optional): An optional parameter to set the workspace in which the lakehouse resides. This defaults to the workspace in which the notebook resides.
    """
    tom_server = fabric.create_tom_server(readonly = False, workspace = workspace_id)
    tom_database = tom_server.Databases.GetByName(dataset_name)
    shared_expression = get_shared_expression(lakehouse_name)

    try:
        model = tom_database.Model
        model.Expressions['DatabaseQuery'].Expression = shared_expression
        model.SaveChanges()
        print(f"The expression in the '{dataset_name}' semantic model has been updated to point to the '{lakehouse_name}' lakehouse.")
    except Exception as e:
        print(f"ERROR: The expression in the '{dataset_name}' semantic model was not updated. Error: {e}")


# # Encode file content to base64


def encode_to_base64(file):
    return base64.b64encode(json.dumps(file).encode('utf-8')).decode('utf-8')


# # Decode Base64 encoded data to JSON format


def decode_from_base64(encoded_data):
    # Decode the Base64 data
    decoded_bytes = base64.b64decode(encoded_data)
    # Convert bytes to string
    decoded_str = decoded_bytes.decode('utf-8')
    # Convert string to JSON
    decoded_json = json.loads(decoded_str)
    return decoded_json


# # Check the status of the Fabric Operation


def check_operation_status(operation_id, client=fabric.FabricRestClient()):
    operation_response = client.get(f"/v1/operations/{operation_id}").json()
    while operation_response['status'] != 'Succeeded':
        if operation_response['status'] == 'Failed':
            error = operation_response['error']
            error_code = error['errorCode']
            error_message = error['message']
            raise Exception(f"Operation failed with error code {error_code}: {error_message}")
        time.sleep(3)
        operation_response = client.get(f"/v1/operations/{operation_id}").json()
    print('Operation succeeded')


# # Create or Replace Semantic Model from bim


def create_or_replace_semantic_model_from_bim(dataset_name, bim_file_json, workspace_id=fabric.get_workspace_id()):
    """
    This function deploys a Model.bim file to create a new semantic model in a workspace. If it already exists it will overwrite it.

    Parameters:

        dataset_name: The name of the semantic model to be created.
        bim_file_json: model.bim file loaded as a json dict
        workspace_id: An optional parameter to set the workspace in which the lakehouse resides. This defaults to the workspace in which the notebook resides.

    Returns:

        This function returns a printout stating the success/failure of the operation.

    """

    # Define the object type for the semantic model
    object_type = 'SemanticModel'

    # Default Power BI dataset definition
    def_pbi_dataset = {"version": "1.0", "settings": {}}

    # Prepare the request body with the dataset name and encoded BIM file content
    request_body = {
        'displayName': dataset_name,
        'type': object_type,
        'definition': {
            "parts": [
                {
                    "path": "model.bim", 
                    "payload": encode_to_base64(bim_file_json), 
                    "payloadType": "InlineBase64"
                },
                {
                    "path": "definition.pbidataset", 
                    "payload": encode_to_base64(def_pbi_dataset), 
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }

    # Call the function to create or replace the fabric item
    create_or_replace_fabric_item(workspace_id, dataset_name, object_type, request_body)


# # Create or replace report from report json


def create_or_replace_report_from_reportjson(report_name, dataset_name, report_json, theme_json=None, workspace_id=fabric.get_workspace_id()):
    """
    Create or replace a report from a report JSON definition.

    Args:
        report_name (str): The name of the report.
        dataset_name (str): The name of the dataset.
        report_json (dict): The JSON definition of the report.
        theme_json (dict, optional): The JSON definition of the theme. Defaults to None.
        workspace_id (str, optional): An optional parameter to set the workspace in which the lakehouse resides. This defaults to the workspace in which the notebook resides.

    Returns:
        None
    """

    # Define the object type for the report
    object_type = 'Report'

    # Retrieve the semantic model associated with the dataset name
    dataset_df = get_fabric_items(item_name=dataset_name, item_type='SemanticModel')
    if dataset_df.empty:
        print(f"ERROR: The '{dataset_name}' semantic model does not exist.")
        return

    # Extract the ID of the dataset
    dataset_id = dataset_df['Id'].iloc[0]

    # Prepare the Power BI report definition
    pbir_def = {
        "version": "1.0",
        "datasetReference": {
            "byPath": None,
            "byConnection": {
                "connectionString": None,
                "pbiServiceModelId": None,
                "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                "pbiModelDatabaseName": dataset_id,
                "name": "EntityDataSource",
                "connectionType": "pbiServiceXmlaStyleLive"
            }
        }
    }

    # Initialize the parts list with the report definition
    parts = [
        {
            "path": "report.json",
            "payload": encode_to_base64(report_json),
            "payloadType": "InlineBase64"
        },
        {
            "path": "definition.pbir",
            "payload": encode_to_base64(pbir_def),
            "payloadType": "InlineBase64"
        }
    ]

    # If a theme is provided, add it to the parts list
    if theme_json:
        theme_id = theme_json['payload']['blob']['displayName']
        theme_path = f'StaticResources/SharedResources/BaseThemes/{theme_id}.json'
        parts.append({
            "path": theme_path,
            "payload": encode_to_base64(theme_json),
            "payloadType": "InlineBase64"
        })

    # Prepare the request body with the report name, type, and definition
    request_body = {
        'displayName': report_name,
        'type': object_type,
        'definition': {"parts": parts}
    }

    # Call the function to create or replace the fabric item
    create_or_replace_fabric_item(workspace_id, report_name, object_type, request_body)


# # Create or Replace Notebook


def create_or_replace_notebook_from_ipynb(notebook_name, notebook_json, default_lakehouse_name=None, replacements=None, workspace_id=fabric.get_workspace_id()):
    """
    Create or replace a notebook in the Fabric workspace.

    This function takes a notebook name, its JSON content, an optional workspace ID, and a dictionary of replacements. It encodes the notebook JSON content to Base64 and sends a request to create or replace the notebook item in the specified Fabric workspace.

    Parameters:
    - notebook_name (str): The display name of the notebook.
    - notebook_json (str): The JSON content of the notebook to be encoded to Base64.
    - default_lakehouse_name (str): An optional parameter to set the default lakehouse name.
    - replacements (dict): An optional dictionary where each key-value pair represents a string to find and a string to replace it with in the code cells of the notebook.
    - workspace_id (str): An optional parameter to set the workspace in which the lakehouse resides. This defaults to the workspace in which the notebook resides.

    Returns:
    None
    """
    # Define the object type for the notebook.
    object_type = 'Notebook'

    # Apply replacements if provided
    if replacements:
        for cell in notebook_json['cells']:
            if cell['cell_type'] == 'code':
                new_source = []
                for line in cell['source']:
                    for key, value in replacements.items():
                        if key in line:
                            line = line.replace(key, value)
                    new_source.append(line)
                cell['source'] = new_source

    # Apply default lakehouse if provided
    if default_lakehouse_name:
        default_lakehouse_id = get_lakehouse_id(default_lakehouse_name)
        new_lakehouse_data = {
            "lakehouse": {
                "default_lakehouse": default_lakehouse_id,
                "default_lakehouse_name": default_lakehouse_name,
                "default_lakehouse_workspace_id": workspace_id
            }
        }
        notebook_json['metadata']['dependencies'] = new_lakehouse_data

    # Construct the request body with the notebook details.
    request_body = {
        "displayName": notebook_name,
        "type": object_type,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "artifact.content.ipynb",
                    "payload": encode_to_base64(notebook_json),
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }

    # Call the function to create or replace the notebook item in the Power BI workspace.
    create_or_replace_fabric_item(workspace_id, notebook_name, object_type, request_body)


# # Create or Replace Fabric Item


def create_or_replace_fabric_item(workspace_id, item_name, object_type, request_body):
    """
    Create or replace a fabric item within a given workspace.

    This function checks if an item with the given name and type already exists within the workspace.
    If it does not exist, it creates a new item. If it does exist, it replaces the existing item with the new definition.

    Parameters:
    - workspace_id (str): The ID of the workspace where the item is to be created or replaced.
    - item_name (str): The name of the item to be created or replaced.
    - object_type (str): The type of the item (e.g., 'dataset', 'report').
    - request_body (dict): The definition of the item in JSON format.

    Returns:
    - None
    """

    # Initialize the REST client for the fabric service
    client = fabric.FabricRestClient()

    # Retrieve existing items of the same name and type
    df = get_fabric_items(item_name=item_name, item_type=object_type)

    # If no existing item, create a new one
    if df.empty:
        response = client.post(f"/v1/workspaces/{workspace_id}/items", json=request_body)
        print(f"'{item_name}' created as a new {object_type}.")
    else:
        # If item exists, replace it with the new definition
        item_id = df.Id.values[0]
        print(f"'{item_name}' already exists as a {object_type} in the workspace. Replacing it...")
        response = client.post(f"/v1/workspaces/{workspace_id}/items/{item_id}/updateDefinition", json=request_body)

    # Check the response status code to determine the outcome
    status_code = response.status_code
    if status_code == 200:
        print("Operation succeeded")
    elif status_code in (201, 202):
        # If status code indicates a pending operation, check its status
        check_operation_status(response.headers['x-ms-operation-id'], client)
    else:
        # If operation failed, print the status code
        print(f"Operation failed with status code: {status_code}")


# # Execute a function with retries


def execute_with_retries(func: callable, *args: any, max_retries: int = 5, delay: int = 10, **kwargs: any) -> None:
    """
    Executes a function with a specified number of retries and delay between attempts.

    Parameters:
    func (Callable): The function to be executed.
    *args (Any): Positional arguments to pass to the function.
    max_retries (int): Maximum number of retries. Default is 5.
    delay (int): Delay in seconds between retries. Default is 10.
    **kwargs (Any): Keyword arguments to pass to the function.

    Returns:
    None
    """
    for attempt in range(max_retries):
        try:
            func(*args, **kwargs)
            print("Function succeeded")
            return
        except Exception as e:
            print(f"An error occurred: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Maximum retries reached. Function failed.")
                return
