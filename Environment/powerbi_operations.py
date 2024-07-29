import datetime
import json
import os
import time

import pandas as pd

from sempy import fabric

from fabric_utils import (call_api, get_lakehouse_id, get_create_or_update_fabric_item, get_item_id,
                          extract_item_name_and_type_from_path, does_semantic_model_exist)
from file_operations import get_file_content_as_base64


def get_server_db(lakehouse_name: str, workspace: str=None) -> tuple:
    """
    Retrieves the server and database details for a given lakehouse.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        workspace (str, optional): The name or ID of the workspace. Defaults to the current workspace ID.

    Returns:
        tuple: A tuple containing the SQL Analytics server connection string and database ID.

    Raises:
        Exception: If the request to get lakehouse details fails.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    lakehouse_id = get_lakehouse_id(lakehouse_name, workspace_id)
    endpoint = f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
    
    response = call_api(endpoint, 'get')
    
    if response.status_code == 200:
        response_json = response.json()
        sql_end_point = response_json['properties']['sqlEndpointProperties']
        server = sql_end_point['connectionString']
        db = sql_end_point['id']
        return server, db
    else:
        raise Exception(f"Failed to get lakehouse details: {response.status_code}")


def get_shared_expression(lakehouse_name: str, workspace: str=None) -> str:
    """
    Generates the shared expression statement for a given lakehouse and its SQL endpoint.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        workspace (str, optional): The name or ID of the workspace in which the lakehouse resides. Defaults to the current workspace ID.

    Returns:
        str: An M statement which can be used as the expression in the shared expression for a Direct Lake semantic model.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    
    # Retrieve server and database details
    server, db = get_server_db(lakehouse_name, workspace_id)
    
    # Create the M statement
    m_statement = f'let\n\tdatabase = Sql.Database("{server}", "{db}")\nin\n\tdatabase'
    
    return m_statement


def update_model_expression(dataset_name: str, lakehouse_name: str, workspace: str=None) -> None:
    """
    Update the expression in the semantic model to point to the specified lakehouse.

    Args:
        dataset_name (str): The name of the dataset.
        lakehouse_name (str): The name of the lakehouse.
        workspace (str, optional): The ID or name of the workspace where the lakehouse resides. Defaults to the current workspace if not provided.

    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    tom_server = fabric.create_tom_server(readonly=False, workspace=workspace_id)
    tom_database = tom_server.Databases.GetByName(dataset_name)
    shared_expression = get_shared_expression(lakehouse_name, workspace_id)
    
    try:
        model = tom_database.Model
        model.Expressions['DatabaseQuery'].Expression = shared_expression
        model.SaveChanges()
        print(f"The expression in the '{dataset_name}' semantic model has been updated to point to the '{lakehouse_name}' lakehouse.")
    except Exception as e:
        print(f"ERROR: The expression in the '{dataset_name}' semantic model was not updated. Error: {e}")


def update_definition_pbir(folder_path: str, dataset_id: str) -> None:
    """
    Update the 'definition.pbir' file in the specified folder with new dataset details.
    
    Args:
        folder_path (str): The path to the folder containing the 'definition.pbir' file.
        dataset_id (str): The new dataset ID to be used in the 'definition.pbir' file.
    
    Raises:
        FileNotFoundError: If the 'definition.pbir' file does not exist.
        ValueError: If the folder_path or dataset_id is invalid.
        json.JSONDecodeError: If the file content is not valid JSON.
        Exception: For any other exceptions that might occur.
    """
    # Validate input parameters
    if not os.path.isdir(folder_path):
        raise ValueError(f"The folder path '{folder_path}' does not exist or is not a directory.")
    if not isinstance(dataset_id, str) or not dataset_id.strip():
        raise ValueError("The dataset_id must be a non-empty string.")
    
    # Define the file path
    file_path = os.path.join(folder_path, 'definition.pbir')
    
    # Check if the file exists
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")
    
    try:
        # Read the existing content of the file
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Update the content
        data['datasetReference']['byPath'] = None
        data['datasetReference']['byConnection'] = {
            "connectionString": None,
            "pbiServiceModelId": None,
            "pbiModelVirtualServerName": "sobe_wowvirtualserver",
            "pbiModelDatabaseName": dataset_id,
            "name": "EntityDataSource",
            "connectionType": "pbiServiceXmlaStyleLive"
        }
        
        # Write the updated content back to the file
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)
        
        print(f"File '{file_path}' updated successfully.")
    
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error decoding JSON from the file '{file_path}': {str(e)}")
    except Exception as e:
        raise Exception(f"An error occurred while updating the file '{file_path}': {str(e)}")


def create_or_replace_semantic_model(model_path: str, workspace: str=None) -> None:
    """
    Create or replace a Power BI semantic model from a given path.

    Args:
        model_path (str): The path to the folder containing the semantic model.
        workspace (str, optional): The ID or name of the workspace to which the semantic model will be deployed. If not provided, defaults to the current workspace.

    Raises:
        ValueError: If the model_path is invalid.
        Exception: For any other exceptions that might occur during the process.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    
    # Validate input parameters
    if not os.path.isdir(model_path):
        raise ValueError(f"The model path '{model_path}' does not exist or is not a directory.")

    try:
        # Get item name and type
        item_name, item_type = extract_item_name_and_type_from_path(model_path)

        # Prepare the request body based on the model path
        model_definition = create_powerbi_item_definition(model_path)

        # Call the function to create or replace the fabric item in the workspace
        get_create_or_update_fabric_item(item_name = item_name, item_type = item_type, item_definition = model_definition, workspace = workspace_id)

    except Exception as e:
        raise Exception(f"An error occurred while creating or replacing the semantic model: {str(e)}")


def create_or_replace_report_from_pbir(report_path: str, dataset_name: str, dataset_workspace: str=None, report_workspace: str=None) -> None:
    """
    Create or replace a Power BI report in service from PBIR and point it to a dataset.

    Args:
        report_path (str): The path to the folder containing the 'definition.pbir' file.
        dataset_name (str): The name of the dataset to be used in the report.
        dataset_workspace (str, optional): The ID or name of the workspace containing the dataset. Defaults to the current workspace if not provided.
        report_workspace (str, optional): The ID or name of the workspace where the report will be deployed. Defaults to the current workspace if not provided.

    Raises:
        ValueError: If the report_path or dataset_name is invalid.
        Exception: For any other exceptions that might occur during the process.
    """
    dataset_workspace_id = fabric.resolve_workspace_id(dataset_workspace)
    report_workspace_id = fabric.resolve_workspace_id(report_workspace)
    
    # Validate input parameters
    if not os.path.isdir(report_path):
        raise ValueError(f"The report path '{report_path}' does not exist or is not a directory.")
    if not isinstance(dataset_name, str) or not dataset_name.strip():
        raise ValueError("The dataset_name must be a non-empty string.")

    try:
        # Extract the ID of the dataset
        dataset_id = get_item_id(dataset_name, 'SemanticModel', dataset_workspace_id)

        # Prepare the Power BI report definition
        update_definition_pbir(report_path, dataset_id)

        # Get item name and type
        item_name, item_type = extract_item_name_and_type_from_path(report_path)

        # Prepare the request body with the report name, type, and definition
        report_definition = create_powerbi_item_definition(report_path)

        # Call the function to create or replace the fabric item
        get_create_or_update_fabric_item(item_name = item_name, item_type = item_type, item_definition = report_definition, workspace = report_workspace_id)

    except Exception as e:
        raise Exception(f"An error occurred while creating or replacing the report: {str(e)}")


def create_powerbi_item_definition(parent_folder_path: str) -> dict:
    """
    Creates a request body for a Power BI item with Base64 encoded file contents.

    This function generates a dictionary representing the item definition for Power BI.
    It scans a specified folder and its subfolders for files to include in the definition,
    encoding their contents in Base64. The request body includes:
    - Files that start with 'definition.' and have a length greater than 11 characters
    - All files in the parent folder and its subfolders, excluding certain patterns

    Args:
        parent_folder_path (str): The path to the parent folder containing the files.

    Returns:
        dict: A dictionary with a single key "parts" that contains a list of dictionaries,
              each representing a file with its path, Base64 encoded payload, and payload type.
    """
    item_definition = {
            "parts": []
        }

    # Check for 'definition.*' file immediately inside the parent folder
    for file_name in os.listdir(parent_folder_path):
        if file_name.startswith('definition.') and len(file_name) > 11:
            definition_file_path = os.path.join(parent_folder_path, file_name)
            if os.path.isfile(definition_file_path):
                encoded_content = get_file_content_as_base64(definition_file_path)
                item_definition["parts"].append({
                    "path": file_name,
                    "payload": encoded_content,
                    "payloadType": "InlineBase64"
                })

    # Traverse through the parent folder and its subfolders
    for root, dirs, files in os.walk(parent_folder_path):
        # Filter subfolders to process
        relative_root = os.path.relpath(root, parent_folder_path)
        if not any(relative_root.startswith(prefix) for prefix in ('StaticResources', 'definition')):
            continue

        for file_name in files:
            # Exclude specific file patterns
            if file_name.endswith('.abf') or (file_name.startswith('item.') and file_name.endswith('.json')):
                continue

            # Construct the relative path excluding the parent folder
            relative_path = os.path.relpath(os.path.join(root, file_name), parent_folder_path).replace(os.sep, '/')

            # Get the base64 encoded content of the file
            file_path = os.path.join(root, file_name)
            encoded_content = get_file_content_as_base64(file_path)

            # Add the file details to the parts list in the request body
            item_definition["parts"].append({
                "path": relative_path,
                "payload": encoded_content,
                "payloadType": "InlineBase64"
            })

    return item_definition


def start_enhanced_refresh(
    dataset_name: str,
    workspace=None,
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
        dataset_name (str): The name of the dataset to refresh.
        workspace (str, optional): The ID or name of the workspace where the dataset is located. Defaults to the current workspace if not provided.
        refresh_objects (str, optional): The objects to refresh in the dataset. Can be "All" or a list of object names. Defaults to "All".
        refresh_type (str, optional): The type of refresh to perform. Can be "full" or "incremental". Defaults to "full".
        commit_mode (str, optional): The commit mode to use for the refresh. Can be "transactional" or "streaming". Defaults to "transactional".
        max_parallelism (int, optional): The maximum number of parallel threads to use for the refresh. Defaults to 10.
        retry_count (int, optional): The number of times to retry the refresh in case of failure. Defaults to 0.
        apply_refresh_policy (bool, optional): Whether to apply the refresh policy defined in the dataset. Defaults to False.
        effective_date (datetime.date, optional): The date to use for the refresh. Defaults to today.

    Returns:
        str: The refresh request ID.

    Raises:
        FabricException: If the refresh fails or encounters an error.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    
    objects_to_refresh = convert_to_json(refresh_objects)
    return fabric.refresh_dataset(
        workspace=workspace_id,
        dataset=dataset_name,
        objects=objects_to_refresh,
        refresh_type=refresh_type,
        max_parallelism=max_parallelism,
        commit_mode=commit_mode,
        retry_count=retry_count,
        apply_refresh_policy=apply_refresh_policy,
        effective_date=effective_date,
    )


def get_enhanced_refresh_details(dataset_name: str, refresh_request_id: str, workspace: str=None) -> pd.DataFrame:
    """Get enhanced refresh details for a given dataset and refresh request ID.

    Args:
        dataset_name (str): The name of the dataset.
        refresh_request_id (str): The ID of the refresh request.
        workspace (str, optional): The ID or name of the workspace. Defaults to the current workspace if not provided.

    Returns:
        pd.DataFrame: A dataframe with the refresh details, messages, and time taken in seconds.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    
    # Get the refresh execution details from fabric
    refresh_details = fabric.get_refresh_execution_details(workspace=workspace_id, dataset=dataset_name, refresh_request_id=refresh_request_id)
    
    # Create a dataframe with the refresh details
    df = pd.DataFrame({
        'workspace': [workspace_id],
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


def cancel_enhanced_refresh(request_id: str, dataset_id: str, workspace: str=None) -> dict:
    """Cancel an enhanced refresh request for a Power BI dataset.

    Args:
        request_id (str): The ID of the refresh request to cancel.
        dataset_id (str): The ID of the dataset to cancel the refresh for.
        workspace (str, optional): The ID or name of the workspace containing the dataset. Defaults to the current workspace if not provided.

    Returns:
        dict: The JSON response from the Power BI REST API.

    Raises:
        FabricHTTPException: If the request fails with a non-200 status code.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    
    # Construct the endpoint URL for the delete request
    endpoint = f"v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{request_id}"

    # Send the delete request using call_api
    response = call_api(endpoint, 'delete', client_type='powerbi')

    # Return the JSON response as a dictionary
    return response.json()


def refresh_and_wait(
    dataset_list: list[str],
    workspace=None,
    logging_lakehouse: str = None,
    parent_job_name: str = None,
    job_category: str = "Adhoc",
) -> None:
    """
    Waits for enhanced refresh of given datasets.

    Args:
      dataset_list (list[str]): List of datasets to refresh.
      workspace (str, optional): The ID or name of the workspace where the datasets are located. Defaults to the current workspace if not provided.
      logging_lakehouse (str, optional): The name of the lakehouse where the job information will be logged. Defaults to None.
      parent_job_name (str, optional): The name of the parent job that triggered the refresh. Defaults to None.
      job_category (str, optional): The category of the job. Defaults to "Adhoc".

    Returns:
      None

    Raises:
      Exception: If any of the datasets failed to refresh.
    """
    from job_operations import insert_or_update_job_table

    # Resolve the workspace_id from the workspace parameter
    workspace_id = fabric.resolve_workspace_id(workspace)

    # Filter out the datasets that do not exist
    valid_datasets = [
        dataset
        for dataset in dataset_list
        if does_semantic_model_exist(dataset, workspace_id)
    ]

    # Start the enhanced refresh for the valid datasets
    request_ids = {
        dataset: start_enhanced_refresh(dataset, workspace_id)
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
    request_status_dict = {}  # Initialize an empty dictionary to store the request status of each dataset
    while True:
        for dataset, request_id in request_ids.copy().items():
            # Get the status and details of the current request
            request_status_df = get_enhanced_refresh_details(dataset, request_id, workspace_id)
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
                print(request_status_df)
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