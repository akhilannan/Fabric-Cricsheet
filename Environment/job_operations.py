import datetime
import time
import uuid

from pyspark.sql import functions as F

from notebookutils import mssparkutils
from sempy import fabric
from sempy.fabric.exceptions import FabricHTTPException

from delta_table_operations import (read_delta_table, create_or_replace_delta_table,
                                     upsert_delta_table, create_spark_dataframe)
from fabric_utils import get_item_id, get_lakehouse_id


def get_job_id(lakehouse = None, table = None, job_name = None) -> str:
    """Returns a job id based on the type and the delta table.

    Args:
        lakehouse (str, optional): The lakehouse of the delta table. Defaults to None.
        table (str, optional): The name of the delta table. Defaults to None.
        job_name (str, optional): The name of the job. Defaults to None.
        type (str, optional): The type of the job id. Can be "Random" or "Latest" or "Latest Parent". Defaults to "Random".

    Returns:
        str: The job id as a string.
    """
    job_id = str(uuid.uuid4()) # default to random job id
    if lakehouse and table and job_name:
        try:
            job_id = read_delta_table(lakehouse, table).filter(F.col("job_name") == job_name).orderBy(F.desc("start_time")).first()["job_id"]
        except Exception as e:
            print(f"Returning a random id because of this error: {e}")
            pass # ignore any errors
    return job_id


def insert_or_update_job_table(
    lakehouse_name: str,
    job_name: str,
    job_category: str = "Adhoc",
    status: str = "InProgress",
    parent_job_name: str = None,
    request_id: str = None,
    message: str = None,
    duration: int = None,
) -> None:
    """
    Inserts or updates a row in the job table with the given parameters.

    Args:
        lakehouse_name: The name of Lakehouse where job table has to be stored.
        job_name: The name of the job.
        job_category: Category of a job. Defaults to Adhoc
        parent_job_name: Name of the parent job. Defaults to None.
        request_id: The request ID for the job. If None, a random job ID will be generated. Defaults to None.
        status: The status of the job. If "InProgress", the job will be inserted as a new row in the table, otherwise updated. Defaults to "InProgress".
        message: Captures error message or other messaages if any
    """
    # Check if lakehouse_name is None and return None if it is
    if lakehouse_name is None:
        return None

    table_name = "t_job"
    # Get the job ID and the parent job ID
    job_id = request_id or (
        get_job_id()
        if status == "InProgress"
        else get_job_id(lakehouse_name, table_name, job_name)
    )
    parent_job_id = (
        get_job_id(lakehouse_name, table_name, parent_job_name)
        if parent_job_name
        else None
    )

    # Create a dataframe with the job details
    job_df = create_spark_dataframe(
        data=
        [
            (
                job_id,
                parent_job_id,
                job_name,
                job_category,
                datetime.datetime.today().replace(microsecond=0),
                None,
                status,
                message,
            )
        ],
        schema="job_id string, parent_job_id string, job_name string, job_category string, start_time timestamp, end_time timestamp, status string, message string",
    )

    # Insert or update the job table based on the status column
    if status == "InProgress":
        create_or_replace_delta_table(job_df, lakehouse_name, table_name, "append")
    else:
        job_df = job_df.withColumn("end_time", F.lit(datetime.datetime.today().replace(microsecond=0)))
        end_time = f"t.start_time + INTERVAL {duration} SECONDS" if duration else "s.end_time"
        merge_condition = "s.job_id = t.job_id"
        update_condition = {
            "end_time": end_time,
            "status": "s.status",
            "message": "s.message",
        }
        upsert_delta_table(lakehouse_name, table_name, job_df, merge_condition, update_condition)


def execute_dag(dag):
    """Run multiple notebooks in parallel and return the results or raise an exception."""
    results = mssparkutils.notebook.runMultiple(dag)
    errors = [] # A list to store the errors
    for job, data in results.items(): # Loop through the dictionary
        if data["exception"]: # Check if there is an exception for the job
            errors.append(f"{job}: {data['exception']}") # Add the error message to the list
    if errors: # If the list is not empty
        raise Exception("\n".join(errors)) # Raise an exception with the comma-separated errors
    else:
        return results


def execute_and_log(function: callable, log_lakehouse: str = None, job_name: str = None, job_category: str = None, parent_job_name: str = None, request_id: str = None, **kwargs) -> any:
    """Executes a given function and logs the result to a lakehouse table.

    Args:
        function: The function to execute.
        log_lakehouse: The name of the lakehouse to log to.
        job_name: The name of the job to log.
        job_category: Category of a job
        parent_job_name: The name of the parent job, if any.
        request_id: The request ID for the job, if any.
        **kwargs: The keyword arguments to pass to the function.

    Returns:
        The result of the function execution.

    Raises:
        Exception: If the function execution fails.
    """
    result = None
    # check if log_lakehouse is None
    if log_lakehouse is not None:
        # call the insert_or_update_job_table function
        insert_or_update_job_table(lakehouse_name=log_lakehouse, job_name=job_name, job_category = job_category, parent_job_name=parent_job_name, request_id = request_id)
    try:
        result = function(**kwargs) # assign the result of the function call to a variable
        # check if log_lakehouse is None
        if log_lakehouse is not None:
            # call the insert_or_update_job_table function
            insert_or_update_job_table(lakehouse_name=log_lakehouse, job_name=job_name, status="Completed")
    except Exception as e:
        msg = str(e)
        status = 'Completed' if msg == "No new data" else "Failed"
        # check if log_lakehouse is None
        if log_lakehouse is not None:
            # call the insert_or_update_job_table function
            insert_or_update_job_table(lakehouse_name=log_lakehouse, job_name=job_name, status=status, message=msg)
        raise e
    return result


def get_lakehouse_job_status(operation_id: str, lakehouse_name: str, workspace_id: str=fabric.get_workspace_id()) -> dict:
    """Returns the status of a lakehouse job given its operation ID and lakehouse name.

    Args:
        operation_id (str): The ID of the lakehouse job operation.
        lakehouse_name (str): The name of the lakehouse.
        workspace_id (str, optional): The ID of the workspace. Defaults to None.

    Returns:
        dict: A dictionary containing the operation status.

    Raises:
        FabricHTTPException: If the response status code is not 200.
    """
    client = fabric.FabricRestClient()
    lakehouse_id = get_lakehouse_id(lakehouse_name, workspace_id)
    try:
        response = client.get(f'/v1/workspaces/{workspace_id}/items/{lakehouse_id}/jobs/instances/{operation_id}')
        if response.status_code in (200, 403):
            return response.json()
        else:
            raise FabricHTTPException(response)
    except FabricHTTPException as e:
        print(e)


def execute_with_retries(func: callable, *args: any, max_retries: int=5, delay: int=10, **kwargs: any) -> None:
    """
    Executes a function with a specified number of retries and delay between attempts.

    Parameters:
    func (Callable): The function to be executed.
    *args (Any): Positional arguments to pass to the function.
    max_retries (int): Maximum number of retries. Default is 5.
    delay (int): Delay in seconds between retries. Default is 10.
    **kwargs (Any): Keyword arguments to pass to the function.

    Returns:
    None
    """
    for attempt in range(max_retries):
        try:
            func(*args, **kwargs)
            print('Function succeeded')
            return
        except Exception as e:
            print(f'An error occurred: {e}')
            if attempt < max_retries - 1:
                print(f'Retrying in {delay} seconds...')
                time.sleep(delay)
            else:
                print('Maximum retries reached. Function failed.')
                return


def check_operation_status(operation_id, client=fabric.FabricRestClient()):
    """
    Polls the status of an operation until it is completed.

    This function repeatedly checks the status of an operation using its ID by making
    HTTP GET requests to the Fabric API. The status is checked every 3 seconds until
    the operation is either completed successfully or fails. If the operation fails,
    an exception is raised with details of the error.

    Parameters:
    - operation_id (str): The unique identifier of the operation to check.
    - client (fabric.FabricRestClient, optional): An instance of the FabricRestClient used
      to make the HTTP requests. Defaults to a new instance of FabricRestClient.

    Raises:
    - Exception: If the operation status is 'Failed', an exception is raised with the
      error code and message from the response.

    Prints:
    - A message 'Operation succeeded' if the operation completes successfully.
    """
    operation_response = client.get(f'/v1/operations/{operation_id}').json()
    while operation_response['status'] != 'Succeeded':
        if operation_response['status'] == 'Failed':
            error = operation_response['error']
            error_code = error['errorCode']
            error_message = error['message']
            raise Exception(f'Operation failed with error code {error_code}: {error_message}')
        time.sleep(3)
        operation_response = client.get(f'/v1/operations/{operation_id}').json()
    print('Operation succeeded')


def run_notebook_job(notebook_name, workspace_id):
    """
    Runs a notebook job using the specified notebook name and workspace ID.

    Parameters:
    - notebook_name (str): The name of the notebook to be run.
    - workspace_id (str): The workspace ID where the notebook is located.

    Returns:
    None
    """
    try:
        # Get the notebook ID
        notebook_id = get_item_id(notebook_name, 'Notebook', workspace_id)
        
        # Run the notebook job
        fabric.run_notebook_job(notebook_id=notebook_id, workspace=workspace_id)
        
    except Exception as e:
        print(f"{e}. Check the run details from Monitoring Hub")