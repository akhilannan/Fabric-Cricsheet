import os
import time

from api_client import FabricPowerBIClient


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
        ValueError: If neither 'sempy fabric' is available nor 'client' is provided.
    """    
    # Use the provided client if available
    if client is None:
        client = FabricPowerBIClient(client_type=client_type)

    # Make the API call using the client's request method
    response = client.request(method, url, json=body, params=params, files=files)

    return response


def poll_operation_status(operation_id: str, message: str=None, client=None):
    """
    Polls the status of an operation until it is completed.

    This function repeatedly checks the status of an operation using its ID by making
    HTTP GET requests to the API. The status is checked every 5 seconds until
    the operation is either completed successfully or fails. If the operation fails,
    an exception is raised with details of the error.

    Parameters:
    - operation_id (str): The unique identifier of the operation to check.
    - message (str, optional): A message to print upon success.
    - client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Raises:
    - Exception: If the operation status is 'Failed', an exception is raised with the
      error code and message from the response.

    Prints:
    - 'Operation succeeded: {message}' if successful
    """
    while True:
        response = call_api(f'/v1/operations/{operation_id}', 'get', client=client).json()
        status = response['status']
        
        if status == 'Succeeded':
            print(f'Operation succeeded{": " + message if message else ""}')
            break
        
        if status == 'Failed':
            error = response['error']
            error_code = error['errorCode']
            error_message = error['message']
            raise Exception(f'Operation failed with error code {error_code}: {error_message}')
        
        time.sleep(5)


def get_all_items_with_pagination(endpoint: str, params: dict=None, client=None):
    """
    Fetch all items from a paginated API endpoint.

    Args:
        endpoint (str): The API endpoint to fetch data from.
        params (dict, optional): Additional query parameters for the API request.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        list: A list of all items retrieved from the API.
    """
    if params is None:
        params = {}

    all_items = []

    while True:
        try:
            response = call_api(endpoint, 'get', params=params, client=client)
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


def resolve_workspace_id(workspace: str, client=None) -> str:
    """
    Resolves a workspace name or UUID to the workspace UUID.

    Args:
        workspace (str): The name or UUID of the workspace.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        str: The workspace UUID.

    Raises:
        ValueError: If the workspace is not found.
    """
    try:
        from sempy import fabric
        return fabric.resolve_workspace_id(workspace)
    except (ImportError, AttributeError):
        # Fallback logic if sempy or the method isn't available
        workspaces = get_all_items_with_pagination("/v1/workspaces", client=client)

        for workspace_item in workspaces:
            if workspace in (workspace_item["id"], workspace_item["displayName"]):
                return workspace_item["id"]

        raise ValueError(f"Workspace '{workspace}' not found.")
    

def get_fabric_capacities(client=None) -> list:
    """
    Retrieve fabric capacities from the API, filtering for active capacities with SKU not equal to 'PP3'.

    Args:
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        List[Dict[str, Any]]: A list of fabric capacities that are active and do not have SKU 'PP3'.
    """
    endpoint = "/v1/capacities"
    all_capacities = get_all_items_with_pagination(endpoint, client=client)
    
    # Filter the capacities
    filtered_capacities = [capacity for capacity in all_capacities 
                           if capacity.get('state') == 'Active' and capacity.get('sku') != 'PP3']
    
    return filtered_capacities


def get_fabric_items(item_name: str = None, item_type: str = None, workspace: str=None, client=None) -> list:
    """
    Retrieve fabric items from a specified workspace, optionally filtered by item name and type.

    Args:
        item_name (str, optional): The name of the item to filter by (case-insensitive partial match).
        item_type (str, optional): The type of items to retrieve.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        List[Dict[str, Any]]: A list of fabric items matching the specified criteria.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)
    params = {'type': item_type} if item_type else {}
    endpoint = f"/v1/workspaces/{workspace_id}/items"

    all_items = get_all_items_with_pagination(endpoint, params, client=client)

    if item_name:
        return [item for item in all_items if item_name == item.get('displayName')]
    
    return all_items


def get_item_id(item_name: str, item_type: str, workspace: str=None, client=None) -> str:
    """
    Get the ID of a specific item based on its name and type.

    Args:
        item_name (str): The name of the item to find.
        item_type (str): The type of the item to find.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        Optional[str]: The ID of the matching item, or None if no matching item is found.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)
    items = get_fabric_items(item_name=item_name, item_type=item_type, workspace=workspace_id, client=client) 
    
    if not items:
        print(f"{item_name} doesn't exist in the workspace as {item_type}")
        return None
    
    return items[0]['id']


def get_lakehouse_id(lakehouse_name: str, workspace: str=None, client=None) -> str:
    """
    Retrieves the ID of a Fabric Lakehouse item based on its display name.

    This function calls `get_item_id` with the item type set to 'Lakehouse' to find the ID of
    a Fabric Lakehouse item given its display name and workspace ID.

    Parameters:
    - lakehouse_name (str): The display name of the Fabric Lakehouse item to search for.
    - workspace (str, optional): The ID or name of the workspace where the Lakehouse item is located.
      Defaults to the current workspace if not provided.
    - client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
    - str: The ID of the Fabric Lakehouse item if found.
    """
    return get_item_id(item_name=lakehouse_name, item_type='Lakehouse', workspace=workspace, client=client)


def get_or_create_fabric_workspace(workspace_name: str, capacity_id: str = None, client=None) -> str:
    """
    Resolves the workspace ID using the provided workspace name. If resolution fails, checks for capacity ID
    and creates a new workspace if needed.

    Args:
        workspace_name (str): Name of the workspace.
        capacity_id (str, optional): ID of the capacity. Defaults to None.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        str: The resolved or newly created workspace ID.

    Raises:
        Exception: If no suitable capacity is found and capacity_id is not provided, or if workspace creation fails.
    """
    try:
        # Attempt to resolve the workspace ID using the provided workspace name
        workspace_id = resolve_workspace_id(workspace_name, client=client)
        return workspace_id
    except Exception as e:    
    # If the workspace ID resolution fails (returns None), check if capacity ID is missing
        if capacity_id is None:
            try:
                # Fetch active capacities
                capacities = get_fabric_capacities(client=client)
                
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
            response = call_api("/v1/workspaces", 'post', body=body, client=client)
            
            # Parse the response JSON
            data = response.json()
            
            # Return the ID of the newly created workspace
            return data.get('id')
        
        except Exception as e:
            print(f"Error creating workspace: {e}")
            raise Exception("Failed to create workspace")


def get_create_or_update_fabric_item(item_name: str, item_type: str, item_definition: dict=None, item_description: str=None, old_item_name: str=None, workspace: str=None, client=None):
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
    - client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
    - str: The ID of the item, whether it was newly created, updated, or already existed.
           Returns `None` if the operation fails.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)

    # Build the request_body
    request_body = {
        "displayName": item_name,
        "type": item_type,
        **({"description": item_description} if item_description else {}),
        **({"definition": item_definition} if item_definition else {})
    }

    # Initialize URL and method
    url = f"/v1/workspaces/{workspace_id}/items"
    method = "post"
    
    # Determine the item to work with based on old_item_name or item_name
    item_id = get_item_id(item_name=old_item_name or item_name, item_type=item_type, workspace=workspace_id, client=client)

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
    response = call_api(url, method, request_body, client=client)
    status_code = response.status_code
    msg = f"'{item_name}' {item_type} {action}."

    # Check the response status code to determine the outcome
    if status_code in (200, 201):
        print(f"Operation succeeded: {msg}.")
    elif status_code == 202:
        # If status code indicates a pending operation, check its status
        try:
            poll_operation_status(response.headers['x-ms-operation-id'], msg, client=client)
        except Exception as e:
            print(f"Operation failed: {str(e)}")
            return None
    else:
        # If operation failed, print the status code
        print(f"Operation failed with status code: {status_code}")
        return None

    return item_id or get_item_id(item_name, item_type, workspace_id, client)


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


def create_lakehouse_if_not_exists(lh_name: str, workspace: str=None, client=None) -> str:
    """
    Creates a lakehouse with the given name if it does not exist already.

    Args:
        lh_name (str): The name of the lakehouse to create.
        workspace (str, optional): The name or ID of the workspace where the lakehouse is to be created.
                                   If not provided, it uses the default workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.


    Returns:
        str: The ID of the lakehouse.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)

    return get_create_or_update_fabric_item(item_name=lh_name, item_type="Lakehouse", workspace=workspace_id, client=client)


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
    import notebookutils
    # Check if the mount point exists in fs.mounts
    if any(m.mountPoint == mount_point for m in notebookutils.fs.mounts()):
        # Return the local path of the existing mount point
        return next(m.localPath for m in notebookutils.fs.mounts() if m.mountPoint == mount_point)
    else:
        # Mount the One Lake path
        notebookutils.fs.mount(abfss_path, mount_point)

        # Return the local path of the new mount point
        return next(m.localPath for m in notebookutils.fs.mounts() if m.mountPoint == mount_point)


def get_lakehouse_path(lakehouse_name: str, path_type: str = "spark", folder_type: str = "Tables", workspace=None, client=None) -> str:
    """
    Returns the path to a lakehouse folder based on the lakehouse name, path type, and folder type.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        path_type (str): The type of the path, either "spark" or "local". Defaults to "spark".
        folder_type (str): The type of the folder, either "Tables" or "Files". Defaults to "Tables".
        workspace (str, optional): The name or ID of the workspace where the lakehouse is located.
                                   If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.


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

    workspace_id = resolve_workspace_id(workspace, client=client)
    lakehouse_id = create_lakehouse_if_not_exists(lakehouse_name, workspace_id, client=client)

    # Construct the path to the lakehouse
    abfss_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com"
    abfss_lakehouse_path = os.path.join(abfss_path, lakehouse_id)

    # Construct the path based on the path type
    if path_type == "spark":
        return os.path.join(abfss_lakehouse_path, folder_type)
    elif path_type == "local":
        local_path = create_mount_point(abfss_lakehouse_path, f"/lakehouse/{lakehouse_name}")
        return os.path.join(local_path, folder_type)


def delete_path(lakehouse, item, folder_type='Tables', client=None):
    """Deletes the folder or file if it exists.

    Args:
        lakehouse (str): The name of the lakehouse.
        item (str): The name of the item (folder or file) to delete.
        folder_type (str): The type of folder ('Tables' or 'Files'). Defaults to 'Tables'.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    """
    import notebookutils
    path = get_lakehouse_path(lakehouse, path_type='spark', folder_type=folder_type, client=client)
    path_item = os.path.join(path, item)
    if notebookutils.fs.exists(path_item):
        notebookutils.fs.rm(path_item, True)
    else:
        print(f'Path does not exist: {path_item}')


def get_delta_tables_in_lakehouse(lakehouse_name: str, workspace=None, client=None) -> list:
    """
    Retrieve names of tables with format 'delta' from a specified lakehouse within a workspace.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        workspace (str): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        List[str]: A list of table names with format 'delta' in the specified lakehouse.
    """
    try:
        workspace_id = resolve_workspace_id(workspace, client=client)
        lakehouse_id = get_lakehouse_id(lakehouse_name, workspace_id, client=client)
        tables = get_all_items_with_pagination(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables", client=client)
        
        delta_tables = [table['name'] for table in tables if table.get('format') == 'delta']
    except Exception as e:
        print(e)
        delta_tables = []

    return delta_tables