import base64
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlretrieve
from zipfile import ZipFile

from sempy import fabric

from fabric_utils import get_lakehouse_path


def unzip_files(zip_full_path: str, filenames: list[str] = None, extract_full_path: str = None) -> None:
    """Unzip a batch of files from a zip file to a given path or the entire zip file if no filenames are provided.

    Args:
        zip_full_path (str): The full path of the zip file.
        filenames (list[str], optional): The list of filenames to unzip. Defaults to None.
        extract_full_path (str, optional): The destination path for the unzipped files. Defaults to None.
    """
    # If no extract_full_path is provided, extract to the same directory as the zip file
    if extract_full_path is None:
        extract_full_path = os.path.dirname(zip_full_path)
    
    # Ensure the destination directory exists
    os.makedirs(extract_full_path, exist_ok=True)
        
    # Open the zip file
    with ZipFile(zip_full_path, 'r') as handle:
        # If no filenames are provided, extract the entire zip file
        if filenames is None:
            handle.extractall(path=extract_full_path)
        else:
            handle.extractall(path=extract_full_path, members=filenames)


def unzip_parallel(lakehouse: str, zip_relative_path: str, extract_relative_path: str = None, file_type: str = None) -> None:
    """Unzip all files from a zip file to a given path in parallel.

    Args:
        lakehouse (str): Name of the Lakehouse
        zip_relative_path (str): The relative path of the zip file.
        extract_relative_path (str, optional): The relative path for the destination of the unzipped files. Defaults to None.
        file_type (str, optional): The file type to extract. Defaults to None.
    """
    # Base path using get_lakehouse_path
    base_path = get_lakehouse_path(lakehouse, "local", "Files")
    
    # Full path to the zip file
    zip_full_path = os.path.join(base_path, zip_relative_path)
    
    # Check if the zip file exists
    if not os.path.exists(zip_full_path):
        print(f"The zip file {zip_full_path} does not exist.")
        return

    # Determine the extraction path
    if extract_relative_path is None:
        extract_full_path = os.path.dirname(zip_full_path)
    else:
        extract_full_path = os.path.join(base_path, extract_relative_path)
    
    try:
        # Open the zip file
        with ZipFile(zip_full_path, 'r') as handle:
            # List of all files to unzip
            files = handle.namelist()
        
        # Filter the files by file type if not None
        if file_type is not None:
            files = [f for f in files if f.endswith(file_type)]
        
        n_workers = min(os.cpu_count() * 4, len(files))  # Determine the number of workers based on CPU count and number of files
        chunksize = max(1, len(files) // n_workers) # Determine chunksize

        # Use ThreadPoolExecutor to unzip files in parallel
        with ThreadPoolExecutor(n_workers) as executor:
            futures = []
            for i in range(0, len(files), chunksize):
                filenames = files[i:i + chunksize]
                futures.append(executor.submit(unzip_files, zip_full_path, filenames, extract_full_path))
            
            # Wait for all futures to complete            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error extracting files: {e}")
        
        print(f"Successfully extracted files from {zip_full_path} to {extract_full_path}")

    except Exception as e:
        print(f"An error occurred: {e}")


def download_data(url, lakehouse, path, workspace_id: str = fabric.get_workspace_id()):
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
    str
        The file path of the downloaded file.
    """
    # Create a lake path
    lake_path = os.path.join(get_lakehouse_path(lakehouse, "local", "Files", workspace_id), path)

    # Create a file name from the base URL
    file_path = os.path.join(lake_path, os.path.basename(url))

    # Create a directory for the lake path if it does not exist
    os.makedirs(lake_path, exist_ok=True)

    # Download the data from the base URL and save the file in the path
    urlretrieve(url, file_path)

    return file_path


def encode_to_base64(file):
    """
    Encodes a Python object to a Base64 string.

    This function serializes a Python object to a JSON-formatted string and then encodes
    that string into Base64.

    Parameters:
    - file (any): The Python object to encode. It will be converted to a JSON string before encoding.

    Returns:
    - str: The Base64-encoded string representation of the JSON-encoded Python object.
    """
    return base64.b64encode(json.dumps(file).encode('utf-8')).decode('utf-8')


def get_file_content_as_base64(file_path):
    """
    Reads the content of a file and returns it encoded in Base64.

    This function opens a file in binary mode, reads its content, and encodes it into
    a Base64 string.

    Parameters:
    - file_path (str): The path to the file to be read.

    Returns:
    - str: The Base64-encoded content of the file.
    """
    with open(file_path, 'rb') as file:
        return base64.b64encode(file.read()).decode('utf-8')


def decode_from_base64(encoded_data):
    """
    Decodes a Base64 string back to its original Python object.

    This function decodes a Base64-encoded string to bytes, then converts the bytes to
    a JSON string, and finally deserializes the JSON string back into a Python object.

    Parameters:
    - encoded_data (str): The Base64-encoded string to decode.

    Returns:
    - dict: The decoded Python object.
    """
    # Decode the Base64 data
    decoded_bytes = base64.b64decode(encoded_data)
    # Convert bytes to string
    decoded_str = decoded_bytes.decode('utf-8')
    # Convert string to JSON
    decoded_json = json.loads(decoded_str)
    return decoded_json


def delete_folder_from_lakehouse(lakehouse, path, workspace_id = fabric.get_workspace_id()):
    """
    Deletes a folder from the specified lakehouse.

    Parameters:
    - lakehouse (str): The name of the lakehouse.
    - path (str): The folder path to be deleted within the lakehouse.
    - workspace_id (str): The workspace ID.

    Returns:
    None
    """
    # Construct the lake path
    lake_path = os.path.join(get_lakehouse_path(lakehouse, "local", "Files", workspace_id), path)
    
    # Delete the folder
    shutil.rmtree(lake_path, ignore_errors=True)
