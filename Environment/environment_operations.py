import json
import os
import time
import yaml

from sempy import fabric

from fabric_utils import call_api, get_create_or_update_fabric_item, get_item_id, get_lakehouse_id
from file_operations import encode_to_base64


def update_sparkcompute(environment_name: str, file_path: str, workspace: str=None) -> str:
    """
    Updates the SparkCompute configuration for the specified environment.

    Args:
        environment_name (str): Name of the target environment.
        file_path (str): Path to the YAML configuration file.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.

    Returns:
        str: Success message or error details.
    """
    try:
        workspace_id = fabric.resolve_workspace_id(workspace)
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        if environment_id:
            with open(file_path, 'r') as yaml_file:
                yaml_content = yaml.safe_load(yaml_file)
            json_content = json.dumps(yaml_content, indent=2)
            request_body = json.loads(json_content)
            endpoint = f"v1/workspaces/{workspace_id}/environments/{environment_id}/staging/sparkcompute"
            response = call_api(endpoint, 'patch', body=request_body)
            if response.status_code == 200:
                return "SparkCompute updated successfully."
            else:
                return f"Error updating SparkCompute: {response.text}"
        else:
            return "Environment not found."
    except Exception as e:
        return f"Error: {str(e)}"


def publish_staging_environment(environment_name: str, workspace: str=None) -> str:
    """
    Initiates the publishing process for the specified environment.

    Args:
        environment_name (str): Name of the target environment.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.

    Returns:
        str: Publish status message.
    """
    try:
        workspace_id = fabric.resolve_workspace_id(workspace)
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        if environment_id:
            endpoint = f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/publish"
            response = call_api(endpoint, 'post')
            if response.status_code == 200:
                print("Publish started...")
                while get_publish_state(environment_name, workspace_id) == "running":
                    time.sleep(30)
                return f"Publish {get_publish_state(environment_name, workspace_id)}"
            else:
                return f"Error starting publish: {response.text}"
        else:
            return "Environment not found."
    except Exception as e:
        return f"Error: {str(e)}"


def get_publish_state(environment_name: str, workspace: str=None) -> str:
    """
    Retrieves the current publish state of the specified environment.

    Args:
        environment_name: Name of the target environment.
        workspace: The name or ID of the workspace. If not provided, it uses the current workspace ID.

    Returns:
        Current publish state (e.g., "running", "success", "failed", etc).
    """
    try:
        workspace_id = fabric.resolve_workspace_id(workspace)
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        if environment_id:
            endpoint = f"/v1/workspaces/{workspace_id}/environments/{environment_id}"
            response = call_api(endpoint, 'get')
            publish_details = response.json().get('properties', {}).get('publishDetails', {})
            return publish_details.get('state', "Unknown")
        else:
            return "Environment not found."
    except Exception as e:
        return f"Error: {str(e)}"
    

def upload_files_to_environment(environment_name: str, file_paths: str, workspace: str=None) -> dict:
    """
    Uploads one or more library files to the specified environment.

    Args:
        environment_name (str): Name of the target environment.
        file_paths (str or list of str): Path(s) to the library file(s) on the local system.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.

    Returns:
        dict: Dictionary with file paths as keys and success messages or error details as values.
    """
    # Ensure file_paths is a list
    if isinstance(file_paths, str):
        file_paths = [file_paths]

    results = {}
    try:
        workspace_id = fabric.resolve_workspace_id(workspace)
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        if environment_id:
            endpoint = f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/libraries"
            for file_path in file_paths:
                try:
                    with open(file_path, 'rb') as file:
                        files = {'file': file}
                        response = call_api(endpoint, method='post', files=files)
                        if response.status_code == 200:
                            results[file_path] = "Library uploaded successfully."
                        else:
                            results[file_path] = f"Error uploading library: {response.text}"
                except Exception as e:
                    results[file_path] = f"Error: {str(e)}"
        else:
            return {"error": "Environment not found."}
    except Exception as e:
        return {"error": f"Error: {str(e)}"}
    return results


def create_and_publish_spark_environment(environment_name: str, yml_path: str, py_path: str, workspace: str=None):
    """
    Creates or replaces a Spark environment using the specified YAML and Python files.

    Args:
        environment_name (str): Name of the Spark environment.
        yml_path (str): Path to the Spark YAML configuration file.
        py_path (str): Path to the fabric_utils.py file.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
    """
    # Resolve workspace ID
    workspace_id = fabric.resolve_workspace_id(workspace)
    
    # Create or replace the fabric item
    get_create_or_update_fabric_item(item_name = environment_name, item_type = "Environment", workspace=workspace_id)
    
    # Upload files to the environment
    print(upload_files_to_environment(environment_name, py_path, workspace_id))
    
    # Update SparkCompute configuration
    print(update_sparkcompute(environment_name, yml_path, workspace_id))
    
    # Publish the environment
    print(publish_staging_environment(environment_name, workspace_id))


def create_or_replace_notebook_from_ipynb(notebook_path: str, default_lakehouse_name: str=None, environment_name: str=None, replacements: dict=None, workspace: str=None) -> str:
    """
    Create or replace a notebook in the Fabric workspace.

    This function takes the path to the notebook file, an optional workspace name or ID, and a dictionary of replacements. It reads the notebook file, encodes the notebook JSON content to Base64, and sends a request to create or replace the notebook item in the specified Fabric workspace.

    Args:
        notebook_path (str): The path to the notebook .ipynb file.
        default_lakehouse_name (str, optional): An optional parameter to set the default lakehouse name.
        environment_name (str, optional): An optional parameter to set the environment name.
        replacements (dict, optional): A dictionary where each key-value pair represents a string to find and a string to replace it with in the code cells of the notebook.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.

    Returns:
        ID of the created/replaced Notebook
    """
    # Resolve the workspace ID
    workspace_id = fabric.resolve_workspace_id(workspace)

    # Extract the notebook name from the path (excluding the .ipynb extension)
    notebook_name = os.path.splitext(os.path.basename(notebook_path))[0]

    # Open the notebook file and load its JSON content
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook_json = json.load(f)

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
        default_lakehouse_id = get_lakehouse_id(default_lakehouse_name, workspace_id)
        new_lakehouse_data = {
            "lakehouse": {
                "default_lakehouse": default_lakehouse_id,
                "default_lakehouse_name": default_lakehouse_name,
                "default_lakehouse_workspace_id": workspace_id
            }
        }
        notebook_json['metadata']['dependencies'] = new_lakehouse_data

    # Append environment details if environment_name exists
    if environment_name:
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        new_environment_data = {
            "environment": {
                "environmentId": environment_id,
                "workspaceId": workspace_id
            }
        }
        notebook_json['metadata']['dependencies'].update(new_environment_data)

    # Construct the request body with the notebook details.
    notebook_definition = {
            "format": "ipynb",
            "parts": [
                {
                    "path": "artifact.content.ipynb",
                    "payload": encode_to_base64(notebook_json),
                    "payloadType": "InlineBase64"
                }
            ]
        }

    # Call the function to create or replace the notebook item in the workspace.
    get_create_or_update_fabric_item(item_name=notebook_name, item_type='Notebook', item_definition=notebook_definition, workspace=workspace_id)