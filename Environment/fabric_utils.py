import os

import notebookutils
from sempy import fabric
from sempy.fabric.exceptions import FabricHTTPException


def get_all_items_with_pagination(endpoint: str, params: dict = None) -> list:
    """
    Fetch all items from a paginated API endpoint.

    Args:
        endpoint (str): The API endpoint to fetch data from.
        params (dict, optional): Additional query parameters for the API request.

    Returns:
        List[Dict[str, Any]]: A list of all items retrieved from the API.
    """
    if params is None:
        params = {}

    client = fabric.FabricRestClient()
    all_items = []

    while True:
        try:
            response = client.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            items = data.get('value', [])
            all_items.extend(items)
            
            if 'continuationToken' not in data:
                break
            
            params['continuationToken'] = data['continuationToken']
        
        except FabricHTTPException as e:
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


def get_fabric_items(item_name: str = None, item_type: str = None, workspace=None) -> list:
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


def get_item_id(item_name: str, item_type: str, workspace=None) -> str:
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


def get_lakehouse_id(lakehouse_name: str, workspace=None) -> str:
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
        client = fabric.FabricRestClient()
        body = {
            "displayName": workspace_name,
            "capacityId": capacity_id
        }
        try:
            # Make the POST request to create the workspace
            response = client.post("/v1/workspaces", json=body)
            response.raise_for_status()
            
            # Parse the response JSON
            data = response.json()
            
            # Return the ID of the newly created workspace
            return data.get('id')
        
        except FabricHTTPException as e:
            print(f"Error creating workspace: {e}")
            raise Exception("Failed to create workspace")


def create_or_replace_fabric_item(request_body, workspace=None):
    """
    Create or replace a fabric item within a given workspace.

    This function checks if an item with the given name and type already exists within the workspace.
    If it does not exist, it creates a new item. If it does exist, it replaces the existing item with the new definition.

    Parameters:
    - workspace (str, optional): The name or ID of the workspace where the item is to be created or replaced.
                                 If not provided, it uses the default workspace ID.
    - request_body (dict): The definition of the item in JSON format. Should contain "displayName" and "type".

    Returns:
    - str: The ID of the item if the operation is successful. Returns `None` if the operation fails.
    """
    from job_operations import check_operation_status

    # Resolve the workspace_id from the workspace parameter
    workspace_id = fabric.resolve_workspace_id(workspace)

    # Extract item_name and item_type from request_body
    item_name = request_body.get("displayName")
    item_type = request_body.get("type")

    # Check if 'item_name' and 'item_type' are provided; if not, print an error and exit function
    if not item_name or not item_type:
        print("Error: 'displayName' and 'type' must be present in request_body.")
        return None

    # Initialize the REST client for the fabric service
    client = fabric.FabricRestClient()
    url = f"/v1/workspaces/{workspace_id}/items"

    # Retrieve the existing item ID if it exists
    item_id = get_item_id(item_name=item_name, item_type=item_type, workspace=workspace_id)

    # Determine the appropriate URL and HTTP method based on the presence of item_id
    if item_id is None:
        action = "created"
    else:
        if item_type in ["Notebook", "Report", "SemanticModel", "SparkJobDefinition", "DataPipeline"]:
            url = f"{url}/{item_id}/updateDefinition"
            action = "replaced"
        else:
            print(f"'{item_name}' already exists as a {item_type} in the workspace")
            return item_id

    # Perform the API request
    response = client.post(url, json=request_body)
    status_code = response.status_code

    # Check the response status code to determine the outcome
    if status_code in (200, 201):
        print(f"Operation succeeded: '{item_name}' {action} as a {item_type}.")
    elif status_code == 202:
        # If status code indicates a pending operation, check its status
        try:
            check_operation_status(response.headers['x-ms-operation-id'], client)
        except Exception as e:
            print(f"Operation failed: {str(e)}")
            return None
    else:
        # If operation failed, print the status code
        print(f"Operation failed with status code: {status_code}")
        return None

    # Ensure item_id is set
    if not item_id:
        item_id = get_item_id(item_name=item_name, item_type=item_type, workspace=workspace_id)

    return item_id


def extract_item_name_and_type_from_path(parent_folder_path):
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


def create_lakehouse_if_not_exists(lh_name: str, workspace=None) -> str:
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
    request_body = {
        "displayName": lh_name,
        "type": "Lakehouse"
    }

    return create_or_replace_fabric_item(request_body, workspace_id)


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

    # Resolve the workspace ID
    workspace_id = fabric.resolve_workspace_id(workspace)

    # Create the lakehouse if it does not exist
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


def get_tables_in_lakehouse(lakehouse_name):
    """Returns a list of tables in the given lakehouse.

    Args:
        lakehouse_name (str): The name of the lakehouse.

    Returns:
        list: A list of table names, or an empty list if the lakehouse does not exist or is empty.
    """
    try:
        table_list = [file.name for file in notebookutils.fs.ls(get_lakehouse_path(lakehouse_name))]
    except Exception as e:
        print(e)
        table_list = []
    return table_list


def is_dataset_exists(dataset: str, workspace=None) -> bool:
    """
    Check if a dataset exists in a given workspace.

    Args:
        dataset (str): The name of the dataset to check.
        workspace (str, optional): The name or ID of the workspace to search in.
                                   If not provided, it uses the current workspace ID.

    Returns:
        bool: True if the dataset exists, False otherwise.
    """
    workspace_id = fabric.resolve_workspace_id(workspace)
    return not fabric.list_datasets(workspace_id).query(f"`Dataset Name` == '{dataset}'").empty