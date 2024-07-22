import os

from notebookutils import mssparkutils
from sempy import fabric
from sempy.fabric.exceptions import FabricHTTPException


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


def get_item_id(item_name, item_type, workspace_id=fabric.get_workspace_id()):
    """
    Retrieves the ID of a Fabric item based on its display name.

    This function searches for a Fabric item by its display name and type within a specific
    workspace and returns the ID of the item if found. If no matching item is found, it raises
    a ValueError with a descriptive message.

    Parameters:
    - item_name (str): The display name of the Fabric item to search for.
    - item_type (str): The type of the Fabric item (e.g., 'Lakehouse', 'Table', etc.).
    - workspace_id (str, optional): The ID of the workspace where the item is located. Defaults
      to the current workspace ID obtained from `fabric.get_workspace_id()`.

    Returns:
    - str: The ID of the Fabric item if found.

    Raises:
    - ValueError: If no Fabric item with the given display name and type is found in the workspace.
    """
    
    # Filter the list by matching the display name with the given argument
    df = get_fabric_items(item_name=item_name, item_type=item_type, workspace_id=workspace_id)

     # Check if the dataframe is empty
    if df.empty:
        # If yes, raise an exception with an informative message
        raise ValueError(f"{item_name} doesn't exist in the workspace as {item_type}")
    else:
        return df.Id.values[0]


def get_lakehouse_id(lakehouse_name: str, workspace_id: str = fabric.get_workspace_id()):
    """
    Retrieves the ID of a Fabric Lakehouse item based on its display name.

    This function calls `get_item_id` with the item type set to 'Lakehouse' to find the ID of
    a Fabric Lakehouse item given its display name and workspace ID.

    Parameters:
    - lakehouse_name (str): The display name of the Fabric Lakehouse item to search for.
    - workspace_id (str, optional): The ID of the workspace where the Lakehouse item is located.
      Defaults to the current workspace ID obtained from `fabric.get_workspace_id()`.

    Returns:
    - str: The ID of the Fabric Lakehouse item if found.
    """
    return get_item_id(item_name = lakehouse_name, item_type = 'Lakehouse', workspace_id = workspace_id)


def get_or_create_workspace(workspace_name, capacity_id=None):
    """
    Resolves the workspace ID using the provided workspace name. If resolution fails, checks for capacity ID and creates a new workspace if needed.

    Args:
        workspace_name (str): Name of the workspace.
        capacity_id (str, optional): ID of the capacity. Defaults to None.

    Returns:
        str: The resolved or newly created workspace ID.
        
    Raises:
        Exception: If no suitable capacity is found and capacity_id is not provided.
    """
    try:
        # Attempt to resolve the workspace ID using the provided workspace name
        workspace_id = fabric.resolve_workspace_id(workspace_name)
    except:
        # If the workspace ID resolution fails, check if capacity ID is missing
        if capacity_id is None:
            try:
                # List active capacities and select the first one that is not PPU
                capacity_id = fabric.list_capacities().query("State == 'Active' and Sku != 'PP3'")["Id"].iloc[0]
            except:
                # If no suitable capacity is found, raise an exception
                raise Exception("No Premium/Fabric Capacities found")
        # Create a new workspace using the provided workspace name and capacity ID
        workspace_id = fabric.create_workspace(workspace_name, capacity_id)
    
    return workspace_id


def create_or_replace_fabric_item(request_body, workspace_id=fabric.get_workspace_id()):
    """
    Create or replace a fabric item within a given workspace.

    This function checks if an item with the given name and type already exists within the workspace.
    If it does not exist, it creates a new item. If it does exist, it replaces the existing item with the new definition.

    Parameters:
    - workspace_id (str): The ID of the workspace where the item is to be created or replaced.
    - request_body (dict): The definition of the item in JSON format. Should contain "displayName" and "type".

    Returns:
    - None
    """
    from job_operations import check_operation_status
    
    # Extract item_name and object_type from request_body
    item_name = request_body.get("displayName")
    object_type = request_body.get("type")
    
    if not item_name or not object_type:
        print("Error: 'displayName' and 'type' must be present in request_body.")
        return
    
    # Initialize the REST client for the fabric service
    client = fabric.FabricRestClient()

    # Retrieve existing items of the same name and type
    df = get_fabric_items(item_name=item_name, item_type=object_type, workspace_id=workspace_id)

    # If no existing item, create a new one
    if df.empty:
        response = client.post(f"/v1/workspaces/{workspace_id}/items", json=request_body)
        print(f"'{item_name}' created as a new {object_type}.")
    elif df.Type.values[0] == "Environment":
        print(f"'{item_name}' already exists as a {object_type} in the workspace")
        return
    else:
        # If item exists, replace it with the new definition
        item_id = df.Id.values[0]
        print(f"'{item_name}' already exists as a {object_type} in the workspace. Replacing it...")
        response = client.post(f"/v1/workspaces/{workspace_id}/items/{item_id}/updateDefinition", json=request_body)

    # Check the response status code to determine the outcome
    status_code = response.status_code
    if status_code in (200, 201):
        print("Operation succeeded")
    elif status_code == 202:
        # If status code indicates a pending operation, check its status
        try:
            check_operation_status(response.headers['x-ms-operation-id'], client)
        except Exception as e:
            print(f"Operation failed: {str(e)}")
    else:
        # If operation failed, print the status code
        print(f"Operation failed with status code: {status_code}")


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


def create_lakehouse_if_not_exists(lh_name: str, workspace_id: str = fabric.get_workspace_id()) -> str:
    # Use a docstring to explain the function's purpose and parameters
    """Creates a lakehouse with the given name if it does not exist already.

    Args:
        lh_name (str): The name of the lakehouse to create.

    Returns:
        str: The ID of the lakehouse.
    """
    try:
        # Use the fabric API to create a lakehouse and return its ID
        return fabric.create_lakehouse(display_name = lh_name, workspace = workspace_id)
    except FabricHTTPException as exc:
        # If the lakehouse already exists, return its ID
        return get_lakehouse_id(lh_name, workspace_id)


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


def get_lakehouse_path(lakehouse_name: str, path_type: str = "spark", folder_type: str = "Tables", workspace_id: str = fabric.get_workspace_id()) -> str:
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
    lakehouse_id = create_lakehouse_if_not_exists(lakehouse_name, workspace_id)

    # Get the workspace id
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
    if mssparkutils.fs.exists(path_item):
        mssparkutils.fs.rm(path_item, True)
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
        table_list = [file.name for file in mssparkutils.fs.ls(get_lakehouse_path(lakehouse_name))]
    except Exception as e:
        print(e)
        table_list = []
    return table_list


def is_dataset_exists(dataset: str, workspace: str = fabric.get_workspace_id()) -> bool:
    """Check if a dataset exists in a given workspace.

    Args:
        dataset (str): The name of the dataset to check.
        workspace (str, optional): The ID or Name of the workspace to search in. Defaults to the current workspace.

    Returns:
        bool: True if the dataset exists, False otherwise.
    """
    return not fabric.list_datasets(workspace).query(f"`Dataset Name` == '{dataset}'").empty