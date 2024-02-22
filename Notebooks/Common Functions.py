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
from pyspark.sql import DataFrame
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
import uuid


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
    
    # Check if the mount point exists in fs.mounts
    if any(m.mountPoint == mount_point for m in mssparkutils.fs.mounts()):
        # Return the local path of the existing mount point
        return next(m.localPath for m in mssparkutils.fs.mounts() if m.mountPoint == mount_point)
    else:
        # Mount the One Lake path
        mssparkutils.fs.mount(abfss_path, mount_point)

        # Return the local path of the new mount point
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


# # Delete a Lakehouse item

# In[ ]:


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

# In[ ]:


# Define a function that takes the lake path and the base URL as parameters
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


# # Function to check if Delta Table Exists

# In[ ]:


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


# # Function for Reading a Delta Table

# In[ ]:


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


# # Function for Creating or Replacing a Table

# In[ ]:


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


# # Function for updating Delta table

# In[ ]:


def update_delta_table(lakehouse_name: str, table_name: str, df: DataFrame, merge_condition: str, update_condition: dict) -> None:
    """Updates a delta table with the given data frame and conditions.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        table_name (str): The name of the delta table.
        df (pyspark.sql.DataFrame): The data frame to merge with the delta table.
        merge_condition (str): The condition to match the rows for merging.
        update_condition (dict): The dictionary of column names and values to update when matched.

    Returns:
        None
    """
    table_path = get_lakehouse_path(lakehouse_name)
    delta_table = DeltaTable.forPath(spark, os.path.join(table_path, table_name))
    (
        delta_table.alias('t')
        .merge(df.alias('s'), merge_condition)
        .whenMatchedUpdate(set = update_condition)
        .execute()
    )


# # Function to get row count of a table

# In[ ]:


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


# # Function to compare row count

# In[ ]:


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

# In[ ]:


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


# # Function to get Job ID

# In[ ]:


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


# # Function to Insert or Update Log table

# In[ ]:


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
        end_time = (
            f"t.start_time + INTERVAL {duration} SECONDS" if duration else "s.end_time"
        )
        merge_condition = "s.job_id = t.job_id"
        update_condition = {
            "end_time": end_time,
            "status": "s.status",
            "message": "s.message",
        }
        update_delta_table(
            lakehouse_name, table_name, job_df, merge_condition, update_condition
        )


# # Function to Execute DAG

# In[ ]:


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


# # Function to Execute and Log

# In[ ]:


def execute_and_log(function: callable, log_lakehouse: str, job_name: str, job_category: str, parent_job_name: str = None, request_id: str = None, **kwargs) -> any:
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
    insert_or_update_job_table(lakehouse_name=log_lakehouse, job_name=job_name, job_category = job_category, parent_job_name=parent_job_name, request_id = request_id)
    try:
        result = function(**kwargs) # assign the result of the function call to a variable
        insert_or_update_job_table(lakehouse_name=log_lakehouse, job_name=job_name, status="Completed")
    except Exception as e:
        msg = str(e)
        status = 'Completed' if msg == "No new data" else "Failed"
        insert_or_update_job_table(lakehouse_name=log_lakehouse, job_name=job_name, status=status, message=msg)
        raise e
    return result


# # Get Tables in a Lakehouse

# In[ ]:


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


# # Optimize and Vacuum Table

# In[ ]:


def optimize_and_vacuum_table(lakehouse_name: str, table_name: str | list | None = None, retention_hours: int = None) -> None:
    """Optimizes and vacuums one or more Delta tables in the lakehouse.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        table_name (str | list | None, optional): The name or names of the tables to optimize and vacuum. If None, all tables in the lakehouse will be processed. Defaults to None.
        retention_hours (int, optional): The retention period in hours for vacuuming. Defaults to None.
    """
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
    path = get_lakehouse_path(lakehouse_name)
    if table_name is None:
        # Get a list of all tables in the lakehouse
        table_names = get_tables_in_lakehouse(lakehouse_name)
    elif isinstance(table_name, str):
        # Convert a single table name to a list
        table_names = [table_name]
    else:
        # Assume table_name is already a list of table names
        table_names = table_name
    # Loop through each table name and optimize and vacuum it
    for table_name in table_names:
        delta_table = DeltaTable.forPath(spark, os.path.join(path, table_name))
        delta_table.optimize()
        delta_table.vacuum(retention_hours)

