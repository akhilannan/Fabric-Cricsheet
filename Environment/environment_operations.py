import json
import os
import time

from fabric_utils import call_api, create_lakehouse_if_not_exists, get_create_or_update_fabric_item, get_item_id, resolve_workspace_id
from file_operations import encode_to_base64


def update_sparkcompute(environment_id: str, file_path: str, workspace: str=None, client=None) -> str:
    """
    Updates the SparkCompute configuration for the specified environment.

    Args:
        environment_id (str): ID of the target environment.
        file_path (str): Path to the JSON configuration file.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        str: Success message or error details.
    """
    try:
        workspace_id = resolve_workspace_id(workspace, client=client)
        if environment_id:
            with open(file_path, 'r') as json_file:
                request_body = json.load(json_file)
            endpoint = f"v1/workspaces/{workspace_id}/environments/{environment_id}/staging/sparkcompute"
            response = call_api(endpoint, 'patch', body=request_body, client=client)
            if response.status_code == 200:
                return "SparkCompute updated successfully."
            else:
                return f"Error updating SparkCompute: {response.text}"
        else:
            return "Environment ID not provided."
    except Exception as e:
        return f"Error: {str(e)}"


def publish_staging_environment(environment_id: str, workspace: str=None, client=None) -> str:
    """
    Initiates the publishing process for the specified environment.

    Args:
        environment_id (str): ID of the target environment.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        str: Publish status message.
    """
    try:
        workspace_id = resolve_workspace_id(workspace, client=client)
        if environment_id:
            endpoint = f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/publish"
            response = call_api(endpoint, 'post', client=client)
            if response.status_code == 200:
                print("Publish started...")
                while True:
                    publish_state = get_publish_state(environment_id, workspace_id, client=client)
                    if publish_state != "running":
                        return f"Publish {publish_state}"
                    time.sleep(30)
            else:
                return f"Error starting publish: {response.text}"
        else:
            return "Environment ID not provided."
    except Exception as e:
        return f"Error: {str(e)}"


def get_publish_state(environment_id: str, workspace: str=None, client=None) -> str:
    """
    Retrieves the current publish state of the specified environment.

    Args:
        environment_id (str): ID of the target environment.
        workspace: The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        Current publish state (e.g., "running", "success", "failed", etc).
    """
    try:
        workspace_id = resolve_workspace_id(workspace, client=client)
        if environment_id:
            endpoint = f"/v1/workspaces/{workspace_id}/environments/{environment_id}"
            response = call_api(endpoint, 'get', client=client)
            publish_details = response.json().get('properties', {}).get('publishDetails', {})
            return publish_details.get('state', "Unknown")
        else:
            return "Environment ID not provided."
    except Exception as e:
        return f"Error: {str(e)}"


def upload_files_to_environment(environment_id: str, file_paths: str, workspace: str=None, client=None) -> dict:
    """
    Uploads one or more library files to the specified environment.

    Args:
        environment_id (str): ID of the target environment.
        file_paths (str or list of str): Path(s) to the library file(s) on the local system.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        dict: Dictionary with file paths as keys and success messages or error details as values.
    """
    # Ensure file_paths is a list
    if isinstance(file_paths, str):
        file_paths = [file_paths]

    results = {}
    try:
        workspace_id = resolve_workspace_id(workspace, client=client)
        if environment_id:
            endpoint = f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/libraries"
            for file_path in file_paths:
                file_name = os.path.basename(file_path)
                try:
                    with open(file_path, 'rb') as file:
                        files = {'file': file}
                        response = call_api(endpoint, method='post', files=files, client=client)
                        if response.status_code == 200:
                            results[file_name] = "Library uploaded successfully."
                        else:
                            results[file_name] = f"Error uploading library: {response.text}"
                except Exception as e:
                    results[file_name] = f"Error: {str(e)}"
        else:
            return {"error": "Environment ID not provided."}
    except Exception as e:
        return {"error": f"Error: {str(e)}"}
    return results


def delete_existing_libraries_and_publish(environment_id: str, workspace: str = None, client = None) -> None:
    """
    Checks for existing custom libraries in the environment, deletes them, and publishes the environment.

    Args:
        environment_id (str): ID of the target environment.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)

    try:
        # Check for existing custom libraries
        response = call_api(f"/v1/workspaces/{workspace_id}/environments/{environment_id}/libraries", 'Get', client=client).json()
        custom_libraries = response.get("customLibraries", {})

        # Extract list of files to delete
        files_to_delete = [file for file_type in ["wheelFiles", "pyFiles", "jarFiles", "rTarFiles"]
                           for file in custom_libraries.get(file_type, [])]

        if files_to_delete:
            print("Deleting existing libraries...")
            # Delete existing libraries
            for file in files_to_delete:
                call_api(f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/libraries?libraryToDelete={file}", 'Delete', client=client)
            
            # Publish after deleting
            print(publish_staging_environment(environment_id, workspace_id, client=client))

    except Exception as e:
        if "404" in str(e):  # If the error message contains "404"
            return  # Exit the function
        raise
        

def create_and_publish_spark_environment(environment_name: str, json_path: str, py_path: str, workspace: str=None, client=None):
    """
    Creates or updates a Spark environment using the specified JSON and Python files.

    Args:
        environment_name (str): Name of the Spark environment.
        yml_path (str): Path to the Spark JSON configuration file.
        py_path (str): Path to the fabric_utils.py file.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)
    environment_id = get_create_or_update_fabric_item(item_name=environment_name, item_type="Environment", workspace=workspace_id, client=client)
    delete_existing_libraries_and_publish(environment_id, workspace_id, client=client)
    print(upload_files_to_environment(environment_id, py_path, workspace_id, client=client))
    print(update_sparkcompute(environment_id, json_path, workspace_id, client=client))
    print(publish_staging_environment(environment_id, workspace_id, client=client))


def create_or_replace_notebook_from_ipynb(notebook_path: str, default_lakehouse_name: str = None, environment_name: str = None, replacements: dict = None, workspace: str = None, client = None) -> str:
    """
    Create or replace a notebook in the Fabric workspace.

    Args:
        notebook_path (str): Path to the notebook .ipynb file.
        default_lakehouse_name (str, optional): Default lakehouse of the Notebook.
        environment_name (str, optional): Environment name.
        replacements (dict, optional): Dictionary of string replacements in code cells.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        str: ID of the created/replaced Notebook.
    """
    workspace_id = resolve_workspace_id(workspace, client=client)
    # Extract the notebook name from the path (excluding the .ipynb extension)
    notebook_name = os.path.splitext(os.path.basename(notebook_path))[0]

    # Load notebook JSON content
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook_json = json.load(f)

    # Apply replacements
    if replacements:
        for cell in notebook_json.get('cells', []):
            if cell['cell_type'] == 'code':
                cell['source'] = [
                    line.replace(key, value) 
                    for line in cell['source'] 
                    for key, value in replacements.items()
                ]

    # Apply default lakehouse
    if default_lakehouse_name:
        default_lakehouse_id = create_lakehouse_if_not_exists(default_lakehouse_name, workspace_id, client=client)
        notebook_json.setdefault('metadata', {}).setdefault('dependencies', {}).update({
            "lakehouse": {
                "default_lakehouse": default_lakehouse_id,
                "default_lakehouse_name": default_lakehouse_name,
                "default_lakehouse_workspace_id": workspace_id
            }
        })

    # Append environment details
    if environment_name:
        environment_id = get_item_id(environment_name, "Environment", workspace_id, client=client)
        notebook_json['metadata']['dependencies'].update({
            "environment": {
                "environmentId": environment_id,
                "workspaceId": workspace_id
            }
        })

    # Construct the request body with the notebook details
    notebook_definition = {
        "format": "ipynb",
        "parts": [{
            "path": "artifact.content.ipynb",
            "payload": encode_to_base64(notebook_json),
            "payloadType": "InlineBase64"
        }]
    }

    return get_create_or_update_fabric_item(
        item_name=notebook_name, 
        item_type='Notebook', 
        item_definition=notebook_definition, 
        workspace=workspace_id, 
        client=client
    )