import datetime
import time
import uuid

try:
    from pyspark.sql import functions as F
except ImportError:
    F = None

from api_client import FabricPowerBIClient as FPC
from delta_table_operations import (
    read_delta_table,
    create_or_replace_delta_table,
    upsert_delta_table,
    create_spark_dataframe,
)
from fabric_utils import get_item_id, get_lakehouse_id, resolve_workspace_id


def get_job_id(lakehouse: str = None, table: str = None, job_name: str = None) -> str:
    """Returns a job id based on the delta table.

    Args:
        lakehouse (str, optional): The lakehouse of the delta table. Defaults to None.
        table (str, optional): The name of the delta table. Defaults to None.
        job_name (str, optional): The name of the job. Defaults to None

    Returns:
        str: The job id as a string.
    """
    job_id = str(uuid.uuid4())  # default to random job id
    if lakehouse and table and job_name:
        try:
            job_id = (
                read_delta_table(lakehouse, table)
                .filter(F.col("job_name") == job_name)
                .orderBy(F.desc("start_time"))
                .first()["job_id"]
            )
        except Exception as e:
            print(f"Returning a random id because of this error: {e}")
            pass  # ignore any errors
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
        data=[
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
        job_df = job_df.withColumn(
            "end_time", F.lit(datetime.datetime.today().replace(microsecond=0))
        )
        end_time = (
            f"t.start_time + INTERVAL {duration} SECONDS" if duration else "s.end_time"
        )
        merge_condition = "s.job_id = t.job_id"
        update_condition = {
            "end_time": end_time,
            "status": "s.status",
            "message": "s.message",
        }
        upsert_delta_table(
            lakehouse_name, table_name, job_df, merge_condition, update_condition
        )


def execute_dag(dag):
    """Run multiple notebooks in parallel and return the results or raise an exception."""
    import notebookutils

    results = notebookutils.notebook.runMultiple(dag)
    errors = []  # A list to store the errors
    for job, data in results.items():  # Loop through the dictionary
        if data["exception"]:  # Check if there is an exception for the job
            errors.append(
                f"{job}: {data['exception']}"
            )  # Add the error message to the list
    if errors:  # If the list is not empty
        raise Exception(
            "\n".join(errors)
        )  # Raise an exception with the comma-separated errors
    else:
        return results


def execute_and_log(
    function: callable,
    log_lakehouse: str = None,
    job_name: str = None,
    job_category: str = None,
    parent_job_name: str = None,
    request_id: str = None,
    **kwargs,
) -> any:
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
        insert_or_update_job_table(
            lakehouse_name=log_lakehouse,
            job_name=job_name,
            job_category=job_category,
            parent_job_name=parent_job_name,
            request_id=request_id,
        )
    try:
        result = function(
            **kwargs
        )  # assign the result of the function call to a variable
        # check if log_lakehouse is None
        if log_lakehouse is not None:
            # call the insert_or_update_job_table function
            insert_or_update_job_table(
                lakehouse_name=log_lakehouse, job_name=job_name, status="Completed"
            )
    except Exception as e:
        msg = str(e)
        status = "Completed" if msg == "No new data" else "Failed"
        # check if log_lakehouse is None
        if log_lakehouse is not None:
            # call the insert_or_update_job_table function
            insert_or_update_job_table(
                lakehouse_name=log_lakehouse,
                job_name=job_name,
                status=status,
                message=msg,
            )
        raise e
    return result


def get_lakehouse_job_status(
    operation_id: str, lakehouse_name: str, workspace: str = None, client=None
) -> dict:
    """Returns the status of a lakehouse job given its operation ID and lakehouse name.

    Args:
        operation_id (str): The ID of the lakehouse job operation.
        lakehouse_name (str): The name of the lakehouse.
        workspace (str, optional): The name or ID of the workspace. Defaults to the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        dict: A dictionary containing the operation status.
    """

    # Resolve the workspace ID
    workspace_id = resolve_workspace_id(workspace, client=client)

    # Get the lakehouse ID
    lakehouse_id = get_lakehouse_id(lakehouse_name, workspace_id, client=client)

    return get_item_job_instance(
        item_id=lakehouse_id,
        job_instance_id=operation_id,
        workspace_id=workspace_id,
        client=client,
    )


def execute_with_retries(
    func: callable, *args: any, max_retries: int = 5, delay: int = 10, **kwargs: any
) -> None:
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
            print("Function succeeded")
            return
        except Exception as e:
            print(f"An error occurred: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Maximum retries reached. Function failed.")
                return


def start_on_demand_job(
    item_id: str,
    job_type: str,
    execution_data: dict = None,
    workspace_id: str = None,
    client: FPC = None,
) -> str:
    """
    Start an on-demand job for an item in a workspace and return the job instance ID.

    Args:
        item_id (str): The ID of the item to run the job on.
        job_type (str): The type of job to run (e.g., TableMaintenance).
        execution_data (dict, optional): The data needed for job execution. Defaults to None.
        workspace_id (str, optional): The ID of the workspace. If not provided, it uses the current workspace ID.
        client (FPC, optional): An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        str: The job instance ID.
    """
    workspace_id = resolve_workspace_id(workspace_id, client=client)
    params = {"jobType": job_type}

    # Prepare payload
    payload = {"executionData": execution_data}

    try:
        # Start the job
        response = FPC.request_with_client(
            "POST",
            f"/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances",
            params=params,
            json=payload,
            client=client,
        )

        # Extract job instance ID from response headers
        job_instance_id = response.headers.get("Location", "").split("/")[-1]

        return job_instance_id

    except Exception as e:
        print(f"Error: {e}")
        return None


def get_item_job_instance(
    item_id: str, job_instance_id: str, workspace_id: str = None, client: FPC = None
) -> dict:
    """
    Get the status of a job instance for an item in a workspace.

    Args:
        item_id (str): The ID of the item.
        job_instance_id (str): The ID of the job instance.
        workspace_id (str, optional): The ID of the workspace. If not provided, it resolves using resolve_workspace_id.
        client (FPC, optional): An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        dict: The response from the job status API call.
    """
    workspace_id = resolve_workspace_id(workspace_id, client=client)

    try:
        # Get the job status
        response = FPC.request_with_client(
            "GET",
            f"/v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{job_instance_id}",
            return_json=True,
            client=client,
        )
        return response
    except Exception as e:
        print(f"Error fetching job status: {e}")
        return None


def run_on_demand_job_and_wait(
    item_id: str,
    job_type: str,
    execution_data=None,
    workspace_id: str = None,
    client=None,
) -> dict:
    """
    Run an on-demand job for an item in a workspace and wait for its completion.

    Args:
        item_id (str): The ID of the item to run the job on.
        job_type (str): The type of job to run (e.g., TableMaintenance).
        execution_data (dict, optional): The data needed for job execution. Defaults to None.
        workspace_id (str, optional): The ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        dict: The response from the job status API call.
    """
    workspace_id = resolve_workspace_id(workspace_id, client=client)

    # Call the function to start the job and get the job instance ID
    job_instance_id = start_on_demand_job(
        item_id, job_type, execution_data, workspace_id, client=client
    )

    if not job_instance_id:
        print("Failed to start the job.")
        return None

    # Polling for job completion
    print(f"{job_type} job triggered. Waiting for completion...")
    while True:
        job_status = get_item_job_instance(
            item_id, job_instance_id, workspace_id, client=client
        )
        if not job_status:
            print("Error fetching job status.")
            return None

        status = job_status.get("status")

        if status == "Failed":
            error_code = job_status.get("failureReason", {}).get("errorCode")
            if error_code == "NotFound":
                time.sleep(30)
                continue

        if status not in ["InProgress", "NotStarted"]:
            break

        time.sleep(30)

    print(f"{job_type} job finished with {status.upper()} status")
    return job_status


def run_notebook_job(
    notebook_name, wait_for_completion=False, workspace: str = None, client=None
):
    """
    Runs a notebook job using the specified notebook name and workspace.

    Parameters:
    - notebook_name (str): The name of the notebook to be run.
    - wait_for_completion (bool, optional): Whether to wait for the job to complete. Defaults to False.
    - workspace (str, optional): The name or ID of the workspace where the notebook is located. Defaults to the current workspace ID.
    - client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
    Job status if waiting for completion, otherwise None.
    """
    job_type = "RunNotebook"
    workspace_id = resolve_workspace_id(workspace, client=client)
    notebook_id = get_item_id(notebook_name, "Notebook", workspace_id, client=client)

    if wait_for_completion:
        # Run the notebook job and wait for completion
        return run_on_demand_job_and_wait(
            item_id=notebook_id,
            job_type=job_type,
            workspace_id=workspace_id,
            client=client,
        )
    else:
        # Start the notebook job without waiting for completion
        job_instance_id = start_on_demand_job(
            item_id=notebook_id,
            job_type=job_type,
            workspace_id=workspace_id,
            client=client,
        )
        if job_instance_id:
            print(
                f"{job_type} job triggered with job instance ID: {job_instance_id}. Check the run details from Monitoring Hub."
            )
        else:
            print("Failed to start the job.")