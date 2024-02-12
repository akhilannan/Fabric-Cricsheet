#!/usr/bin/env python
# coding: utf-8

# ## Common Functions
# 
# New notebook

# # Install Semantic Link

# In[ ]:


get_ipython().system('pip install --upgrade semantic-link --q')


# # Import Libraries

# In[ ]:


from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
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
import pytz
import time


# # Function to get Lakehouse ID

# In[ ]:


def get_lakehouse_id(lakehouse_name: str) -> int:
    """Returns the Id of a Lakehouse item given its display name.

    Args:
        lakehouse_name: The display name of the Lakehouse item.

    Returns:
        The Id of the Lakehouse item as an integer.

    Raises:
        ValueError: If no Lakehouse item with the given display name is found.
    """
    # Get the list of all Lakehouse items
    lakehouse_items = fabric.list_items("Lakehouse")

    # Filter the list by matching the display name with the given argument
    query_result = lakehouse_items.query(f"`Display Name` == '{lakehouse_name}'")

    # Check if the query result is empty
    if query_result.empty:
        # If yes, raise an exception with an informative message
        raise ValueError(f"No Lakehouse item with display name '{lakehouse_name}' found.")
        
    # If no, return the first value of the Id column as an integer
    return query_result.Id.values[0]


# # Function to create Lakehouse

# In[ ]:


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


# # Function to Create Mount Point

# In[ ]:


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
    
    # Mount the One Lake path
    mssparkutils.fs.mount(abfss_path, mount_point)

    # Return the local path of the mount point
    return next(m.localPath for m in mssparkutils.fs.mounts() if m.mountPoint == mount_point)


# # Function to return Lakehouse path

# In[ ]:


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
        local_path = create_mount_point(abfss_lakehouse_path)
        return os.path.join(local_path, folder_type)


# # Function to check if Delta Table Exists

# In[ ]:


def delta_table_exists(path: str, tbl: str) -> bool:
  """Check if a delta table exists at the given path.

  Parameters:
  path (str): The directory where the delta table is stored.
  tbl (str): The name of the delta table.

  Returns:
  bool: True if the delta table exists, False otherwise.
  """
  try:
    # Use the DeltaTable class to access the delta table
    DeltaTable.forPath(spark, os.path.join(path, tbl))
    # If no exception is raised, the delta table exists
    return True
  except Exception as e:
    # If an exception is raised, the delta table does not exist
    return False


# # Function for Reading a Delta Table

# In[ ]:


def read_delta_table(path: str, table_name: str) -> pyspark.sql.DataFrame:
    """Reads a delta table from a given path and table name.

    Args:
        path (str): The path to the delta table directory.
        table_name (str): The name of the delta table.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with the delta table data.
    """
    return (
        spark
        .read
        .format('delta')
        .load(f"{path}/{table_name}")
    )


# # Function for Creating or Replacing a Table

# In[ ]:


def create_or_replace_delta_table(df: pyspark.sql.DataFrame, path: str, tbl: str, mode_type: str = "overwrite") -> None:
    """Create or replace a delta table from a dataframe.

    Args:
        df (pyspark.sql.DataFrame): The dataframe to write to the delta table.
        path (str): The path where the delta table is stored.
        tbl (str): The name of the delta table.
        mode_type (str, optional): The mode for writing the delta table. Defaults to "overwrite".

    Returns:
        None
    """
    (
        df
        .write
        .mode(mode_type)
        .option("mergeSchema", "true")
        .format("delta")
        .save(f'{path}/{tbl}')
    )


# # Function to get row count of a table

# In[ ]:


def get_row_count(table_path: str, table_name: str = None) -> int:
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
                .read_html(table_path)[1]
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
        row_count = read_delta_table(table_path, table_name).count()
    
    return row_count


# # Function to compare row count

# In[ ]:


def compare_row_count(table1_path: str, table1_name: str, table2_path: str, table2_name: str = None) -> tuple[int, int, str]:
    """Compare the row count of two tables and exit or print the difference.

    This function compares the row count of two delta tables or a delta table and a web page.
    It exits the notebook with code -1 if the row counts are equal, or prints the difference otherwise.
    It also returns the maximum and minimum match IDs from the first table, and the write mode.

    Args:
        table_path1: The path of the first table.
        table_name1: The name of the first table.
        table_path2: The path of the second table or web page.
        table_name2: The name of the second table. Defaults to None.

    Returns:
        A tuple of (max_match_id, min_match_id, write_mode)
    """
    # Initialize the default values
    max_match_id, min_match_id, write_mode = 0, 0, "overwrite"
    
    # Check if the first table exists as a delta table
    if delta_table_exists(table1_path, table1_name):
        # Get the row count and the match IDs from the first table
        row_count_1 = get_row_count(table1_path, table1_name)
        
        # Get the row count from the second table or web page
        row_count_2 = get_row_count(table2_path, table2_name)
        
        # Compare the row counts and exit or print accordingly
        if row_count_1 == row_count_2:
            # If the row counts are equal, exit the notebook with code -1
            mssparkutils.notebook.exit(-1)
        else:
            print(f"Cricsheet has {row_count_2 - row_count_1} more matches added")
            max_match_id, min_match_id = read_delta_table(table1_path, table1_name).agg(F.max("match_id"), F.min("match_id")).collect()[0]
            write_mode = "append"
    
    return (max_match_id, min_match_id, write_mode)


# # Function to unzip files from an archive

# In[ ]:


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


# # Function to parallel unzip a large number of files

# In[ ]:


def unzip_parallel(path: str, zip_filename: str) -> None:
    """Unzip all files from a zip file to a given path in parallel.

    Args:
        path (str): The destination path for the unzipped files.
        zip_filename (str): The name of the zip file.
    """
    # open the zip file
    with ZipFile(zip_filename, 'r') as handle:
        # list of all files to unzip
        files = handle.namelist()
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
            _ = exe.submit(unzip_files, zip_filename, filenames, path)


# # Define Function for getting first team to Bat or Field

# In[ ]:


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


# # Function to convert a string of refresh items to json

# In[ ]:


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


# # Function to call Enhanced Refresh API

# In[ ]:


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


# # Function to get Refresh Details

# In[ ]:


def get_enhanced_refresh_details(dataset_name: str, refresh_request_id: str, workspace_name: str = fabric.resolve_workspace_name(fabric.get_workspace_id())) -> pd.DataFrame:
    """Get enhanced refresh details for a given dataset and refresh request ID.

    Args:
        dataset_name (str): The name of the dataset.
        refresh_request_id (str): The ID of the refresh request.
        workspace_name (str, optional): The name of the workspace. Defaults to name workspace where Notebook reside.

    Returns:
        pd.DataFrame: A dataframe with the refresh details, objects, messages, and time taken in seconds.
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
    
    # Convert the start_time and end_time columns to strings with the date format
    df['start_time'] = df['start_time'].astype(str).str.slice(0, 16)
    df['end_time'] = df['end_time'].astype(str).str.slice(0, 16)
    
    # Create a dataframe with the refresh objects
    df_object = pd.DataFrame(refresh_details.objects)
    
    # Add the dataset column to df_object
    df_object = df_object.assign(dataset=dataset_name)
    
    # Create a dataframe with the refresh messages
    df_msg = pd.DataFrame(refresh_details.messages)
    
    # Add the dataset column to df_msg
    df_msg = df_msg.assign(dataset=dataset_name)
    
    # Merge the dataframes on the dataset column and return the result
    return df.merge(df_object, how='outer', on='dataset').merge(df_msg, how='outer', on='dataset')


# # Check if a Semantic Model exists in the workspace

# In[ ]:


def is_dataset_exists(dataset: str, workspace: str = fabric.get_workspace_id()) -> bool:
    """Check if a dataset exists in a given workspace.

    Args:
        dataset (str): The name of the dataset to check.
        workspace (str, optional): The ID or Name of the workspace to search in. Defaults to the current workspace.

    Returns:
        bool: True if the dataset exists, False otherwise.
    """
    return not fabric.list_datasets(workspace).query(f"`Dataset Name` == '{dataset}'").empty


# # Function for synchronous refresh of datasets

# In[ ]:


def refresh_and_wait(dataset_list: list[str], workspace: str = fabric.get_workspace_id()) -> None:
  """
  Waits for enhanced refresh of given datasets.

  Args:
    dataset_list: List of datasets to refresh.
    workspace: The workspace ID where the datasets are located. Defaults to the current workspace.

  Returns:
    None
  """

  # Filter out the datasets that do not exist
  valid_datasets = [dataset for dataset in dataset_list if is_dataset_exists(dataset, workspace)]

  # Start the enhanced refresh for the valid datasets
  request_ids = {dataset: start_enhanced_refresh(dataset, workspace) for dataset in valid_datasets}

  # Print the datasets that do not exist
  invalid_datasets = set(dataset_list) - set(valid_datasets)
  if invalid_datasets:
    print(f"The following datasets do not exist: {', '.join(invalid_datasets)}")

  # Loop until all the requests are completed
  while True:
    for dataset, request_id in request_ids.copy().items():
      # Get the status and details of the current request
      request_status_df = get_enhanced_refresh_details(dataset, request_id, workspace)
      request_status = request_status_df['status'].iloc[0]

      # If the request is not unknown, print the details and remove it from the dictionary
      if request_status != "Unknown":
        print(request_status_df.to_markdown())
        del request_ids[dataset]

    # If there are no more requests, exit the loop
    if not request_ids:
      break
    # Otherwise, wait for 30 seconds before checking again
    else:
      time.sleep(30)

