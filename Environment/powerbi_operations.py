import datetime
import json
import os
import time

from api_client import FabricPowerBIClient as FPC
from fabric_utils import (
    get_lakehouse_id,
    get_create_or_update_fabric_item,
    get_item_id,
    extract_item_name_and_type_from_path,
    resolve_workspace_id,
)
from file_operations import get_file_content_as_base64


def get_server_db(
    lakehouse_name: str, workspace: str = None, client: FPC = None
) -> tuple:
    """
    Retrieves the server and database details for a given lakehouse.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        workspace (str, optional): The name or ID of the workspace. Defaults to the current workspace ID.
        client (FPC, optional): An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        tuple: A tuple containing the SQL Analytics server connection string and database ID.

    Raises:
        Exception: If the request to get lakehouse details fails.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)
    lakehouse_id = get_lakehouse_id(lakehouse_name, workspace_id, client=client)

    try:
        response = FPC.request_with_client(
            "GET",
            f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
            client=client,
        )

        if response.status_code == 200:
            response_json = response.json()
            sql_end_point = response_json["properties"]["sqlEndpointProperties"]
            server = sql_end_point["connectionString"]
            db = sql_end_point["id"]
            return server, db
        else:
            raise Exception(f"Failed to get lakehouse details: {response.status_code}")
    except Exception as e:
        print(f"Error retrieving server and database details: {e}")
        raise


def get_shared_expression(
    lakehouse_name: str, workspace: str = None, client=None
) -> str:
    """
    Generates the shared expression statement for a given lakehouse and its SQL endpoint.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        workspace (str, optional): The name or ID of the workspace in which the lakehouse resides. Defaults to the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        str: An M statement which can be used as the expression in the shared expression for a Direct Lake semantic model.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)

    # Retrieve server and database details
    server, db = get_server_db(lakehouse_name, workspace_id, client=client)

    # Create the M statement
    m_statement = f'let\n\tdatabase = Sql.Database("{server}", "{db}")\nin\n\tdatabase'

    return m_statement


def update_model_expression(
    dataset_name: str, lakehouse_name: str, workspace: str = None, client=None
) -> None:
    """
    Update the expression in the semantic model to point to the specified lakehouse.

    Args:
        dataset_name (str): The name of the dataset.
        lakehouse_name (str): The name of the lakehouse.
        workspace (str, optional): The ID or name of the workspace where the lakehouse resides. Defaults to the current workspace if not provided.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    """
    from sempy import fabric

    workspace_id = resolve_workspace_id(workspace, client=client)
    tom_server = fabric.create_tom_server(readonly=False, workspace=workspace_id)
    tom_database = tom_server.Databases.GetByName(dataset_name)
    shared_expression = get_shared_expression(
        lakehouse_name, workspace_id, client=client
    )

    try:
        model = tom_database.Model
        model.Expressions["DatabaseQuery"].Expression = shared_expression
        model.SaveChanges()
        print(
            f"The expression in the '{dataset_name}' semantic model has been updated to point to the '{lakehouse_name}' lakehouse."
        )
    except Exception as e:
        print(
            f"ERROR: The expression in the '{dataset_name}' semantic model was not updated. Error: {e}"
        )


def update_model_database_expression(
    path, fabric_connection_string, sql_analytics_endpoint_id
):
    """
    Updates database connection strings in files at the specified path.

    Replaces occurrences of `Sql.Database("server", "database")` with
    `Sql.Database("{fabric_connection_string}", "{sql_analytics_endpoint_id}")` in files
    with `.tmdl`, `.bim`, or `.json` extensions.

    Args:
        path (str): File or directory path.
        fabric_connection_string (str): New connection string.
        sql_analytics_endpoint_id (str): New analytics endpoint ID.
    """
    VALID_EXTENSIONS = (".tmdl", ".bim", ".json")

    import re

    def update_file(file_path):
        with open(file_path, "r+", encoding="utf-8") as f:
            content = f.read()
            pattern = r'Sql\.Database\("([^"]+)",\s"([^"]+)"\)'
            replacement = f'Sql.Database("{fabric_connection_string}", "{sql_analytics_endpoint_id}")'
            new_content = re.sub(pattern, replacement, content)
            if new_content != content:
                f.seek(0)
                f.write(new_content)
                f.truncate()
                print(f"Updated: {os.path.basename(file_path)}")
                return True
        return False

    updated = False

    if os.path.isfile(path):
        if path.lower().endswith(VALID_EXTENSIONS):
            updated = update_file(path)
    elif os.path.isdir(path):
        for root, _, files in os.walk(path):
            for file in files:
                if file.lower().endswith(VALID_EXTENSIONS):
                    file_path = os.path.join(root, file)
                    if update_file(file_path):
                        updated = True
    else:
        print(f"Invalid path: {path}")

    if not updated:
        print("No database expressions were updated in any files.")


def update_definition_pbir(folder_path: str, dataset_id: str) -> None:
    """
    Update the 'definition.pbir' file in the specified folder with new dataset details.
    Only writes to the file if there's a change in the datasetReference content.

    Args:
        folder_path (str): The path to the folder containing the 'definition.pbir' file.
        dataset_id (str): The new dataset ID to be used in the 'definition.pbir' file.

    Raises:
        FileNotFoundError: If the 'definition.pbir' file does not exist.
        ValueError: If the folder_path or dataset_id is invalid.
        json.JSONDecodeError: If the file content is not valid JSON.
        KeyError: If 'datasetReference' key is not found in the file.
        Exception: For any other exceptions that might occur.
    """
    file_to_udpate = "definition.pbir"
    # Validate input parameters
    if not os.path.isdir(folder_path):
        raise ValueError(
            f"The folder path '{folder_path}' does not exist or is not a directory."
        )
    if not isinstance(dataset_id, str) or not dataset_id.strip():
        raise ValueError("The dataset_id must be a non-empty string.")

    # Define the file path
    sep = "/"
    file_path = folder_path + sep + file_to_udpate
    file_full_name = (sep).join(file_path.split(sep)[-2:])

    # Check if the file exists
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")

    try:
        # Read the existing content of the file
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        # Create the new datasetReference content
        new_dataset_reference = {
            "byPath": None,
            "byConnection": {
                "connectionString": None,
                "pbiServiceModelId": None,
                "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                "pbiModelDatabaseName": dataset_id,
                "name": "EntityDataSource",
                "connectionType": "pbiServiceXmlaStyleLive",
            },
        }

        # Check if there's a change in the datasetReference
        if data["datasetReference"] != new_dataset_reference:
            # Update the datasetReference
            data["datasetReference"] = new_dataset_reference

            # Write the updated content back to the file
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4)

            print(f"File '{file_full_name}' updated successfully.")
        else:
            print(f"No changes needed for file '{file_full_name}'.")

    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Error decoding JSON from the file '{file_path}': {str(e)}"
        )
    except KeyError as e:
        raise KeyError(
            f"The 'datasetReference' key was not found in the file '{file_path}'"
        )
    except Exception as e:
        raise Exception(
            f"An error occurred while updating the file '{file_path}': {str(e)}"
        )


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
    item_definition = {"parts": []}

    # Check for 'definition.*' file immediately inside the parent folder
    for file_name in os.listdir(parent_folder_path):
        if file_name.startswith("definition.") and len(file_name) > 11:
            definition_file_path = os.path.join(parent_folder_path, file_name)
            if os.path.isfile(definition_file_path):
                encoded_content = get_file_content_as_base64(definition_file_path)
                item_definition["parts"].append(
                    {
                        "path": file_name,
                        "payload": encoded_content,
                        "payloadType": "InlineBase64",
                    }
                )

    # Traverse through the parent folder and its subfolders
    for root, dirs, files in os.walk(parent_folder_path):
        # Filter subfolders to process
        relative_root = os.path.relpath(root, parent_folder_path)
        if not any(
            relative_root.startswith(prefix)
            for prefix in ("StaticResources", "definition")
        ):
            continue

        for file_name in files:
            # Exclude specific file patterns
            if file_name.endswith(".abf") or (
                file_name.startswith("item.") and file_name.endswith(".json")
            ):
                continue

            # Construct the relative path excluding the parent folder
            relative_path = os.path.relpath(
                os.path.join(root, file_name), parent_folder_path
            ).replace(os.sep, "/")

            # Get the base64 encoded content of the file
            file_path = os.path.join(root, file_name)
            encoded_content = get_file_content_as_base64(file_path)

            # Add the file details to the parts list in the request body
            item_definition["parts"].append(
                {
                    "path": relative_path,
                    "payload": encoded_content,
                    "payloadType": "InlineBase64",
                }
            )

    return item_definition


def create_or_replace_semantic_model(
    model_path: str, lakehouse_name: str = None, workspace: str = None, client=None
) -> None:
    """
    Create or replace a Power BI semantic model from a given path.

    Args:
        model_path (str): The path to the folder containing the semantic model.
            lakehouse_name (str, optional): The name of the lakehouse to get the server and database information. If not provided, the model expression will not be updated.
        workspace (str, optional): The ID or name of the workspace to which the semantic model will be deployed. If not provided, defaults to the current workspace.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Raises:
        ValueError: If the model_path is invalid.
        Exception: For any other exceptions that might occur during the process.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)

    # Validate input parameters
    if not os.path.isdir(model_path):
        raise ValueError(
            f"The model path '{model_path}' does not exist or is not a directory."
        )

    try:
        # Get item name and type
        item_name, item_type = extract_item_name_and_type_from_path(model_path)

        # If lakehouse_name is provided, update the model expression
        if lakehouse_name:
            server, db = get_server_db(lakehouse_name, workspace_id, client=client)
            update_model_database_expression(model_path, server, db)

        # Prepare the request body based on the model path
        model_definition = create_powerbi_item_definition(model_path)

        # Call the function to create or replace the fabric item in the workspace
        get_create_or_update_fabric_item(
            item_name=item_name,
            item_type=item_type,
            item_definition=model_definition,
            workspace=workspace_id,
            client=client,
        )

    except Exception as e:
        raise Exception(
            f"An error occurred while creating or replacing the semantic model: {str(e)}"
        )


def create_or_replace_report_from_pbir(
    report_path: str,
    dataset_name: str,
    dataset_workspace: str = None,
    report_workspace: str = None,
    client=None,
) -> None:
    """
    Create or replace a Power BI report in service from PBIR and point it to a dataset.

    Args:
        report_path (str): The path to the folder containing the 'definition.pbir' file.
        dataset_name (str): The name of the dataset to be used in the report.
        dataset_workspace (str, optional): The ID or name of the workspace containing the dataset. Defaults to the current workspace if not provided.
        report_workspace (str, optional): The ID or name of the workspace where the report will be deployed. Defaults to the current workspace if not provided.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Raises:
        ValueError: If the report_path or dataset_name is invalid.
        Exception: For any other exceptions that might occur during the process.
    """
    dataset_workspace_id = resolve_workspace_id(dataset_workspace, client=client)
    report_workspace_id = resolve_workspace_id(report_workspace, client=client)

    # Validate input parameters
    if not os.path.isdir(report_path):
        raise ValueError(
            f"The report path '{report_path}' does not exist or is not a directory."
        )
    if not isinstance(dataset_name, str) or not dataset_name.strip():
        raise ValueError("The dataset_name must be a non-empty string.")

    try:
        # Extract the ID of the dataset
        dataset_id = get_item_id(
            dataset_name, "SemanticModel", dataset_workspace_id, client=client
        )

        # Prepare the Power BI report definition
        update_definition_pbir(report_path, dataset_id)

        # Get item name and type
        item_name, item_type = extract_item_name_and_type_from_path(report_path)

        # Prepare the request body with the report name, type, and definition
        report_definition = create_powerbi_item_definition(report_path)

        # Call the function to create or replace the fabric item
        get_create_or_update_fabric_item(
            item_name=item_name,
            item_type=item_type,
            item_definition=report_definition,
            workspace=report_workspace_id,
            client=client,
        )

    except Exception as e:
        raise Exception(
            f"An error occurred while creating or replacing the report: {str(e)}"
        )


def start_enhanced_refresh(
    semantic_model_name: str,
    workspace: str = None,
    refresh_objects: str = "All",
    refresh_type: str = "full",
    commit_mode: str = "transactional",
    max_parallelism: int = 10,
    retry_count: int = 0,
    apply_refresh_policy: bool = False,
    effective_date: datetime.date = datetime.date.today(),
    client: FPC = None,
) -> str:
    """Starts an enhanced refresh of a semantic model.

    Args:
        semantic_model_name (str): The name of the semantic model to refresh.
        workspace (str, optional): The ID or name of the workspace where the semantic model is located. Defaults to the current workspace if not provided.
        refresh_objects (str, optional): The objects to refresh in the semantic model. Can be "All" or a list of object names. Defaults to "All".
        refresh_type (str, optional): The type of refresh to perform. Can be "full" or "incremental". Defaults to "full".
        commit_mode (str, optional): The commit mode to use for the refresh. Can be "transactional" or "streaming". Defaults to "transactional".
        max_parallelism (int, optional): The maximum number of parallel threads to use for the refresh. Defaults to 10.
        retry_count (int, optional): The number of times to retry the refresh in case of failure. Defaults to 0.
        apply_refresh_policy (bool, optional): Whether to apply the refresh policy defined in the semantic model. Defaults to False.
        effective_date (datetime.date, optional): The date to use for the refresh. Defaults to today.
        client (FPC, optional): An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        str: The refresh request ID.

    Raises:
        Exception: If the refresh fails or encounters an error.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)
    semantic_model_id = get_item_id(
        item_name=semantic_model_name,
        item_type="SemanticModel",
        workspace=workspace_id,
        client=client,
    )

    # Convert refresh objects to JSON format
    objects_to_refresh = convert_to_json(refresh_objects)

    # Prepare the request body
    request_body = {
        "type": refresh_type,
        "commitMode": commit_mode,
        "maxParallelism": max_parallelism,
        "retryCount": retry_count,
        "applyRefreshPolicy": apply_refresh_policy,
        "effectiveDate": effective_date.isoformat(),
    }

    # Add objects to refresh if specified
    if objects_to_refresh:
        request_body["objects"] = objects_to_refresh

    try:
        # Make the API call
        response = FPC.request_with_client(
            "POST",
            f"/v1.0/myorg/groups/{workspace_id}/datasets/{semantic_model_id}/refreshes",
            json=request_body,
            client=client,
        )

        # Extract the request ID from the Location header
        request_id = response.headers.get("Location").split("/")[-1]

        return request_id
    except Exception as e:
        print(f"Error starting enhanced refresh: {e}")
        raise


def get_enhanced_refresh_details(
    semantic_model_name: str,
    refresh_request_id: str,
    workspace: str = None,
    client: FPC = None,
) -> dict:
    """Gets the details of an enhanced refresh operation for a dataset.

    Args:
        semantic_model_name (str): The name of the semantic model.
        refresh_request_id (str): The ID of the refresh request.
        workspace (str, optional): The ID or name of the workspace where the dataset is located. Defaults to None.
        client (FPC, optional): An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        dict: The details of the refresh operation with an added 'duration_in_sec' key.

    Raises:
        Exception: If the operation fails or encounters an error.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)
    semantic_model_id = get_item_id(
        item_name=semantic_model_name,
        item_type="SemanticModel",
        workspace=workspace_id,
        client=client,
    )

    try:
        # Make the API call
        response = FPC.request_with_client(
            "GET",
            f"/v1.0/myorg/groups/{workspace_id}/datasets/{semantic_model_id}/refreshes/{refresh_request_id}",
            client=client,
        )

        # Parse the response
        refresh_details = response.json()

        # Convert startTime to a timezone-aware datetime object
        tz_info = datetime.timezone.utc
        start_time = datetime.datetime.fromisoformat(
            refresh_details["startTime"].replace("Z", "+00:00")
        ).replace(tzinfo=tz_info)

        # Determine end_time with a default of the current UTC time if not provided
        end_time = (
            datetime.datetime.fromisoformat(
                refresh_details["endTime"].replace("Z", "+00:00")
            ).replace(tzinfo=tz_info)
            if "endTime" in refresh_details
            else datetime.datetime.now(tz=tz_info)
        )

        # Add duration in seconds to the refresh details
        refresh_details["duration_in_sec"] = round(
            (end_time - start_time).total_seconds()
        )

        return refresh_details
    except Exception as e:
        print(f"Error fetching refresh details: {e}")
        raise


def cancel_enhanced_refresh(
    request_id: str, dataset_id: str, workspace: str = None, client: FPC = None
) -> dict:
    """Cancel an enhanced refresh request for a Power BI dataset.

    Args:
        request_id (str): The ID of the refresh request to cancel.
        dataset_id (str): The ID of the dataset to cancel the refresh for.
        workspace (str, optional): The ID or name of the workspace containing the dataset. Defaults to the current workspace if not provided.
        client (FPC, optional): An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        dict: The JSON response from the Power BI REST API.

    Raises:
        Exception: If the request fails with a non-200 status code.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)

    try:
        # Send the delete request using the FPC.request_with_client method
        response = FPC.request_with_client(
            "DELETE",
            f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{request_id}",
            client=client,
        )

        # Return the JSON response as a dictionary
        return response.json()
    except Exception as e:
        print(f"Error canceling refresh request: {e}")
        raise


def refresh_and_wait(
    dataset_list: list[str],
    workspace=None,
    logging_lakehouse: str = None,
    parent_job_name: str = None,
    job_category: str = "Adhoc",
    client=None,
) -> None:
    """
    Waits for enhanced refresh of given datasets.

    Args:
      dataset_list (list[str]): List of datasets to refresh.
      workspace (str, optional): The ID or name of the workspace where the datasets are located. Defaults to the current workspace if not provided.
      logging_lakehouse (str, optional): The name of the lakehouse where the job information will be logged. Defaults to None.
      parent_job_name (str, optional): The name of the parent job that triggered the refresh. Defaults to None.
      job_category (str, optional): The category of the job. Defaults to "Adhoc".
      client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
      None

    Raises:
      Exception: If any of the datasets failed to refresh.
    """
    from job_operations import insert_or_update_job_table

    # Resolve the workspace_id from the workspace parameter
    workspace_id = resolve_workspace_id(workspace, client=client)

    # Filter out the datasets that do not exist
    valid_datasets = [
        dataset
        for dataset in dataset_list
        if get_item_id(dataset, "SemanticModel", workspace_id, client=client)
    ]

    # Start the enhanced refresh for the valid datasets
    request_ids = {
        dataset: start_enhanced_refresh(dataset, workspace_id, client=client)
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
    request_status_dict = (
        {}
    )  # Initialize an empty dictionary to store the request status of each dataset
    while True:
        for dataset, request_id in request_ids.copy().items():
            # Get the status and details of the current request
            refresh_details = get_enhanced_refresh_details(
                dataset, request_id, workspace_id, client=client
            )
            request_status = refresh_details["status"]

            # If the request is not unknown, print the details, store the status in the dictionary, and remove it from the request_ids
            if request_status != "Unknown":
                if logging_lakehouse:
                    duration = refresh_details.get("duration_in_sec", None)
                    msg = refresh_details.get("Message", None)
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
                print(refresh_details)
                request_status_dict[dataset] = (
                    request_status  # Store the status in the dictionary
                )
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
                raise Exception(
                    f"The following datasets failed to refresh: {', '.join(failed_datasets)}"
                )
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
    result = []  # Initialize an empty list to store the converted dictionaries
    if refresh_objects is None or refresh_objects == "All":
        return result  # Return an empty list if the input is None or "All"
    for item in refresh_objects.split(
        "|"
    ):  # Loop through each refresh object, separated by "|"
        table, *partitions = item.split(
            ":"
        )  # Split the item by ":" and assign the first element to table and the rest to partitions
        if partitions:  # If there are any partitions
            # Extend the result list with a list comprehension that creates a dictionary for each partition
            # The dictionary has the table name and the partition name as keys
            # The partition name is stripped of any leading or trailing whitespace
            result.extend(
                [
                    {"table": table, "partition": partition.strip()}
                    for partition in ",".join(partitions).split(",")
                ]
            )
        else:  # If there are no partitions
            # Append a dictionary with only the table name as a key to the result list
            result.append({"table": table})
    return result  # Return the final list of dictionaries