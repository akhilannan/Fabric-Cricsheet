import os
import time

import notebookutils
from sempy import fabric


def call_api(url, method, body=None, params=None, files=None, client=None, client_type='fabric'):
    """
    Calls an API with the specified URL, method, body, and params.

    Args:
        url: The URL of the API endpoint.
        method: The HTTP method to use for the request.
        body: The JSON body to include in the request. Defaults to None.
        params: The URL parameters to include in the request. Defaults to None.
        files: Optional dictionary of files to upload. Defaults to None.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.
        client_type: The type of client to initialize ('fabric' or 'powerbi'). Defaults to 'fabric'.

    Returns:
        The response from the API call.

    Raises:
        ValueError: If an invalid method or client_type is provided.
    """    
    # Use the provided client if available
    if client is None:
        # Define a dictionary to map client types to their respective client classes
        client_classes = {
            'fabric': fabric.FabricRestClient,
            'powerbi': fabric.PowerBIRestClient
        }

        # Get the appropriate client class
        client_class = client_classes.get(client_type.lower())
        if client_class is None:
            raise ValueError(f"Invalid client_type: {client_type}. Must be 'fabric' or 'powerbi'.")

        # Initialize the client
        client = client_class()

    # Get the method from the client
    try:
        client_method = getattr(client, method.lower())
    except AttributeError:
        raise ValueError(f"Invalid method: {method}")

    # Make the API call
    try:
        response = client_method(url, json=body, params=params, files=files)
        response.raise_for_status()
    except fabric.exceptions.FabricHTTPException as e:
        raise Exception(f"An error occurred with the Fabric API: {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}")

    return response


def poll_operation_status(operation_id: str):
    """
    Polls the status of an operation until it is completed.

    This function repeatedly checks the status of an operation using its ID by making
    HTTP GET requests to the API. The status is checked every 5 seconds until
    the operation is either completed successfully or fails. If the operation fails,
    an exception is raised with details of the error.

    Parameters:
    - operation_id (str): The unique identifier of the operation to check.

    Raises:
    - Exception: If the operation status is 'Failed', an exception is raised with the
      error code and message from the response.

    Prints:
    - A message 'Operation succeeded' if the operation completes successfully.
    """
    while True:
        response = call_api(f'/v1/operations/{operation_id}', 'get').json()
        status = response['status']
        
        if status == 'Succeeded':
            print('Operation succeeded')
            break
        
        if status == 'Failed':
            error = response['error']
            error_code = error['errorCode']
            error_message = error['message']
            raise Exception(f'Operation failed with error code {error_code}: {error_message}')
        
        time.sleep(5)


def get_all_items_with_pagination(endpoint: str, params: dict=None):
    """
    Fetch all items from a paginated API endpoint.

    Args:
        endpoint (str): The API endpoint to fetch data from.
        params (dict, optional): Additional query parameters for the API request.

    Returns:
        list: A list of all items retrieved from the API.
    """
    if params is None:
        params = {}

    all_items = []

    while True:
        try:
            response = call_api(endpoint, 'get', params=params)
            data = response.json()

            items = data.get('value', data.get('data', []))
            all_items.extend(items)

            continuation_token = data.get('continuationToken')
            if not continuation_token:
                break

            params['continuationToken'] = continuation_token

        except Exception as e:
            print(f"Error fetching data: {e}")
            break

    return all_items


def get_fabric_capacities() -> list:
    """
    Retrieve fabric capacities from the API, filtering for active capacities with SKU not equal to 'PP3'.

    Returns:
        List[Dict[str, Any]]: A list of fabric capacities that are active and do not have SKU 'PP3'.
    """
    endpoint = "/v1/capacities"
    all_capacities = get_all_items_with_pagination(endpoint)
    
    # Filter the capacities
    filtered_capacities = [capacity for capacity in all_capacities 
                           if capacity.get('state') == 'Active' and capacity.get('sku') != 'PP3']
    
    return filtered_capacities


def get_fabric_items(item_name: str = None, item_type: str = None, workspace: str=None) -> list:
    """
    Retrieve fabric items from a specified workspace, optionally filtered by item name and type.

    Args:
        item_name (str, optional): The name of the item to filter by (case-insensitive partial match).
        item_type (str, optional): The type of items to retrieve.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.

    Returns:
        List[Dict[str, Any]]: A list of fabric items matching the specified criteria.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    params = {'type': item_type} if item_type else {}
    endpoint = f"/v1/workspaces/{workspace_id}/items"

    all_items = get_all_items_with_pagination(endpoint, params)

    if item_name:
        return [item for item in all_items if item_name == item.get('displayName')]
    
    return all_items


def get_item_id(item_name: str, item_type: str, workspace: str=None) -> str:
    """
    Get the ID of a specific item based on its name and type.

    Args:
        item_name (str): The name of the item to find.
        item_type (str): The type of the item to find.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.

    Returns:
        Optional[str]: The ID of the matching item, or None if no matching item is found.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    items = get_fabric_items(item_name=item_name, item_type=item_type, workspace=workspace_id)
    
    if not items:
        print(f"{item_name} doesn't exist in the workspace as {item_type}")
        return None
    
    return items[0]['id']


def get_lakehouse_id(lakehouse_name: str, workspace: str=None) -> str:
    """
    Retrieves the ID of a Fabric Lakehouse item based on its display name.

    This function calls `get_item_id` with the item type set to 'Lakehouse' to find the ID of
    a Fabric Lakehouse item given its display name and workspace ID.

    Parameters:
    - lakehouse_name (str): The display name of the Fabric Lakehouse item to search for.
    - workspace (str, optional): The ID or name of the workspace where the Lakehouse item is located.
      Defaults to the current workspace if not provided.

    Returns:
    - str: The ID of the Fabric Lakehouse item if found.
    """
    return get_item_id(item_name=lakehouse_name, item_type='Lakehouse', workspace=workspace)


def get_or_create_fabric_workspace(workspace_name: str, capacity_id: str = None) -> str:
    """
    Resolves the workspace ID using the provided workspace name. If resolution fails, checks for capacity ID
    and creates a new workspace if needed.

    Args:
        workspace_name (str): Name of the workspace.
        capacity_id (str, optional): ID of the capacity. Defaults to None.

    Returns:
        str: The resolved or newly created workspace ID.

    Raises:
        Exception: If no suitable capacity is found and capacity_id is not provided, or if workspace creation fails.
    """
    try:
        # Attempt to resolve the workspace ID using the provided workspace name
        workspace_id = fabric.resolve_workspace_id(workspace_name)
        return workspace_id
    except Exception as e:    
    # If the workspace ID resolution fails (returns None), check if capacity ID is missing
        if capacity_id is None:
            try:
                # Fetch active capacities
                capacities = get_fabric_capacities()
                
                # Check if any capacities are available
                if not capacities:
                    raise Exception("No Premium/Fabric Capacities found")
                
                # Select the first capacity's ID
                capacity_id = capacities[0]['id']
            except Exception as e:
                print(f"Error fetching or selecting capacity: {e}")
                raise Exception("No suitable capacity found and capacity_id was not provided")
    
        # Create a new workspace using the provided workspace name and capacity ID
        body = {
            "displayName": workspace_name,
            "capacityId": capacity_id
        }
        try:
            # Make the POST request to create the workspace
            response = call_api("/v1/workspaces", 'post', body=body)
            
            # Parse the response JSON
            data = response.json()
            
            # Return the ID of the newly created workspace
            return data.get('id')
        
        except Exception as e:
            print(f"Error creating workspace: {e}")
            raise Exception("Failed to create workspace")


def get_create_or_update_fabric_item(item_name: str, item_type: str, item_definition: dict=None, item_description: str=None, old_item_name: str=None, workspace: str=None):
    """
    Gets, creates, or updates a Fabric item within a given workspace.

    Parameters:
    - item_name (str): The display name of the item.
    - item_type (str): The type of the item.
    - item_definition (dict, optional): The definition of the item. Default is None.
    - item_description (str, optional): The description of the item. Default is None.
    - old_item_name (str, optional): The old display name of the item for renaming. Default is None.
    - workspace (str, optional): The name or ID of the workspace where the item is to be created or updated.
                                 If not provided, it uses the default workspace ID.

    Returns:
    - str: The ID of the item, whether it was newly created, updated, or already existed.
           Returns `None` if the operation fails.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)

    # Build the request_body
    request_body = {
        "displayName": item_name,
        "type": item_type
    }

    if item_description:
        request_body["description"] = item_description

    if item_definition:
        request_body["definition"] = item_definition

    # Initialize URL and method
    url = f"/v1/workspaces/{workspace_id}/items"
    method = "post"
    
    # Determine the item to work with based on old_item_name or item_name
    item_id = get_item_id(item_name=old_item_name or item_name, item_type=item_type, workspace=workspace_id)

    # Determine the appropriate action based on the item's existence and type
    if item_id is None:
        action = "created"
    elif item_definition:
        url = f"{url}/{item_id}/updateDefinition"
        action = "definition updated"
    elif (old_item_name or item_description):
        url = f"{url}/{item_id}"
        action = "updated"
        method = "patch"
    else:
        return item_id  # Item exists and doesn't need updating, so just return its ID

    # Perform the API request based on the method
    response = call_api(url, method, request_body)
    status_code = response.status_code


    # Check the response status code to determine the outcome
    if status_code in (200, 201):
        print(f"Operation succeeded: '{item_name}' {action} as a {item_type}.")
    elif status_code == 202:
        # If status code indicates a pending operation, check its status
        try:
            poll_operation_status(response.headers['x-ms-operation-id'])
        except Exception as e:
            print(f"Operation failed: {str(e)}")
            return None
    else:
        # If operation failed, print the status code
        print(f"Operation failed with status code: {status_code}")
        return None

    # Ensure item_id is set (in case of new item creation)
    if not item_id:
        item_id = get_item_id(item_name=item_name, item_type=item_type, workspace=workspace_id)

    return item_id


def extract_item_name_and_type_from_path(parent_folder_path: str):
    """
    Extracts item name and object type from the parent folder name.
    The folder name should be in the format 'item_name.object_type'.
    """
    if not os.path.exists(parent_folder_path):
        raise FileNotFoundError(f"The path '{parent_folder_path}' does not exist.")
    
    parent_folder_name = os.path.basename(parent_folder_path)
    if '.' not in parent_folder_name:
        raise ValueError("Expectation is to have the parent folder in the format 'item_name.object_type'")
    
    item_name, object_type = parent_folder_name.split('.', 1)
    return item_name, object_type


def create_lakehouse_if_not_exists(lh_name: str, workspace: str=None) -> str:
    """
    Creates a lakehouse with the given name if it does not exist already.

    Args:
        lh_name (str): The name of the lakehouse to create.
        workspace (str, optional): The name or ID of the workspace where the lakehouse is to be created.
                                   If not provided, it uses the default workspace ID.

    Returns:
        str: The ID of the lakehouse.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)

    return get_create_or_update_fabric_item(item_name = lh_name, item_type = "Lakehouse", workspace=workspace_id)


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
    if any(m.mountPoint == mount_point for m in notebookutils.fs.mounts()):
        # Return the local path of the existing mount point
        return next(m.localPath for m in notebookutils.fs.mounts() if m.mountPoint == mount_point)
    else:
        # Mount the One Lake path
        notebookutils.fs.mount(abfss_path, mount_point)

        # Return the local path of the new mount point
        return next(m.localPath for m in notebookutils.fs.mounts() if m.mountPoint == mount_point)


def get_lakehouse_path(lakehouse_name: str, path_type: str = "spark", folder_type: str = "Tables", workspace=None) -> str:
    """
    Returns the path to a lakehouse folder based on the lakehouse name, path type, and folder type.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        path_type (str): The type of the path, either "spark" or "local". Defaults to "spark".
        folder_type (str): The type of the folder, either "Tables" or "Files". Defaults to "Tables".
        workspace (str, optional): The name or ID of the workspace where the lakehouse is located.
                                   If not provided, it uses the current workspace ID.

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

    workspace_id = fabric.resolve_workspace_id(workspace)
    lakehouse_id = create_lakehouse_if_not_exists(lakehouse_name, workspace_id)

    # Construct the path to the lakehouse
    abfss_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com"
    abfss_lakehouse_path = os.path.join(abfss_path, lakehouse_id)

    # Construct the path based on the path type
    if path_type == "spark":
        return os.path.join(abfss_lakehouse_path, folder_type)
    elif path_type == "local":
        local_path = create_mount_point(abfss_lakehouse_path, f"/lakehouse/{lakehouse_name}")
        return os.path.join(local_path, folder_type)


def delete_path(lakehouse, item, folder_type='Tables'):
    """Deletes the folder or file if it exists.

    Args:
        path (str): The path of the folder or file to be deleted.
    """
    path = get_lakehouse_path(lakehouse, path_type='spark', folder_type=folder_type)
    path_item = os.path.join(path, item)
    if notebookutils.fs.exists(path_item):
        notebookutils.fs.rm(path_item, True)
    else:
        print(f'Path does not exist: {path_item}')


def get_delta_tables_in_lakehouse(lakehouse_name: str, workspace=None) -> list:
    """
    Retrieve names of tables with format 'delta' from a specified lakehouse within a workspace.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        workspace (str): The name or ID of the workspace. If not provided, it uses the current workspace ID.

    Returns:
        List[str]: A list of table names with format 'delta' in the specified lakehouse.
    """
    try:
        workspace_id = fabric.resolve_workspace_id(workspace)
        lakehouse_id = get_lakehouse_id(lakehouse_name, workspace_id)
        tables = get_all_items_with_pagination(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables")
        
        delta_tables = [table['name'] for table in tables if table.get('format') == 'delta']
    except Exception as e:
        print(e)
        delta_tables = []

    return delta_tables