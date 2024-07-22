import json
import os
import time
import yaml

from sempy import fabric

from fabric_utils import create_or_replace_fabric_item, get_item_id, get_lakehouse_id
from file_operations import encode_to_base64


def update_sparkcompute(environment_name, file_path, workspace_id=fabric.get_workspace_id()):
    """
    Updates the SparkCompute configuration for the specified environment.

    Args:
        environment_name (str): Name of the target environment.
        file_path (str): Path to the YAML configuration file.
        workspace_id (str, optional): ID of the workspace. Defaults to fabric.get_workspace_id().

    Returns:
        str: Success message or error details.
    """
    try:
        client = fabric.FabricRestClient()
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        if environment_id:
            with open(file_path, 'r') as yaml_file:
                yaml_content = yaml.safe_load(yaml_file)
            json_content = json.dumps(yaml_content, indent=2)
            request_body = json.loads(json_content)
            endpoint = f"v1/workspaces/{workspace_id}/environments/{environment_id}/staging/sparkcompute"
            response = client.patch(endpoint, json=request_body)
            if response.status_code == 200:
                return "SparkCompute updated successfully."
            else:
                return f"Error updating SparkCompute: {response.text}"
        else:
            return "Environment not found."
    except Exception as e:
        return f"Error: {str(e)}"


def publish_staging_environment(environment_name, workspace_id=fabric.get_workspace_id()):
    """
    Initiates the publishing process for the specified environment.

    Args:
        environment_name (str): Name of the target environment.
        workspace_id (str, optional): ID of the workspace. Defaults to fabric.get_workspace_id().

    Returns:
        str: Publish status message.
    """
    try:
        client = fabric.FabricRestClient()
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        if environment_id:
            endpoint = f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/publish"
            response = client.post(endpoint)
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


def get_publish_state(environment_name, workspace_id=fabric.get_workspace_id()):
    """
    Retrieves the current publish state of the specified environment.

    Args:
        environment_name (str): Name of the target environment.
        workspace_id (str, optional): ID of the workspace. Defaults to fabric.get_workspace_id().

    Returns:
        str: Current publish state (e.g., "running", "success", "failed", etc).
    """
    try:
        client = fabric.FabricRestClient()
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        if environment_id:
            endpoint = f"/v1/workspaces/{workspace_id}/environments/{environment_id}"
            response = client.get(endpoint).json()
            publish_details = response.get('properties', {}).get('publishDetails', {})
            return publish_details.get('state', "Unknown")
        else:
            return "Environment not found."
    except Exception as e:
        return f"Error: {str(e)}"


def upload_file_to_environment(environment_name, file_path, workspace_id=fabric.get_workspace_id()):
    """
    Uploads a library file to the specified environment.

    Args:
        environment_name (str): Name of the target environment.
        file_path (str): Path to the library file on the local system.
        workspace_id (str, optional): ID of the workspace. Defaults to fabric.get_workspace_id().

    Returns:
        str: Success message or error details.
    """
    try:
        client = fabric.FabricRestClient()
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        if environment_id:
            url = f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/libraries"
            with open(file_path, 'rb') as file:
                files = {'file': file}
                response = client.post(url, files=files)
                if response.status_code == 200:
                    return "Library uploaded successfully."
                else:
                    return f"Error uploading library: {response.text}"
        else:
            return "Environment not found."
    except Exception as e:
        return f"Error: {str(e)}"
    

def upload_files_to_environment(environment_name, file_paths, workspace_id=fabric.get_workspace_id()):
    """
    Uploads one or more library files to the specified environment.

    Args:
        environment_name (str): Name of the target environment.
        file_paths (str or list of str): Path(s) to the library file(s) on the local system.
        workspace_id (str, optional): ID of the workspace. Defaults to fabric.get_workspace_id().

    Returns:
        dict: Dictionary with file paths as keys and success messages or error details as values.
    """
    # Ensure file_paths is a list
    if isinstance(file_paths, str):
        file_paths = [file_paths]

    results = {}
    try:
        client = fabric.FabricRestClient()
        environment_id = get_item_id(environment_name, "Environment", workspace_id)
        if environment_id:
            url = f"/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/libraries"
            for file_path in file_paths:
                try:
                    with open(file_path, 'rb') as file:
                        files = {'file': file}
                        response = client.post(url, files=files)
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



def create_and_publish_spark_environment(environment_name, yml_path, py_path, workspace_id=fabric.get_workspace_id()):
    """
    Creates or replaces a Spark environment using the specified YAML and Python files.
    
    Args:
        yml_path (str): Path to the Spark YAML configuration file.
        py_path (str): Path to the fabric_utils.py file.
        environment_name (str): Name of the Spark environment.
        workspace_id (str, optional): ID of the workspace. Defaults to fabric.get_workspace_id().
    """
    item_type = "Environment"
    request_body = {
        'displayName': environment_name,
        'type': item_type
    }
    
    create_or_replace_fabric_item(request_body, workspace_id)
    print(upload_files_to_environment(environment_name, py_path, workspace_id))
    print(update_sparkcompute(environment_name, yml_path, workspace_id))
    print(publish_staging_environment(environment_name, workspace_id))


def create_or_replace_notebook_from_ipynb(notebook_path, default_lakehouse_name=None, environment_name=None, replacements=None, workspace_id=fabric.get_workspace_id()):
    """
    Create or replace a notebook in the Fabric workspace.

    This function takes the path to the notebook file, an optional workspace ID, and a dictionary of replacements. It reads the notebook file, encodes the notebook JSON content to Base64, and sends a request to create or replace the notebook item in the specified Fabric workspace.

    Parameters:
    - notebook_path (str): The path to the notebook .ipynb file.
    - default_lakehouse_name (str): An optional parameter to set the default lakehouse name.
    - environment_name (str): An optional parameter to set the environment name.
    - replacements (dict): An optional dictionary where each key-value pair represents a string to find and a string to replace it with in the code cells of the notebook.
    - workspace_id (str): An optional parameter to set the workspace in which the lakehouse resides. This defaults to the workspace in which the notebook resides.

    Returns:
    None
    """
    # Define the object type for the notebook.
    object_type = 'Notebook'

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
    request_body = {
        "displayName": notebook_name,
        "type": object_type,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "artifact.content.ipynb",
                    "payload": encode_to_base64(notebook_json),
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }

    # Call the function to create or replace the notebook item in the Power BI workspace.
    create_or_replace_fabric_item(request_body, workspace_id)