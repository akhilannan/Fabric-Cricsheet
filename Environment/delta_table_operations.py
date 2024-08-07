import ast
import dateutil
import os

from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from pyspark.sql import DataFrame, SparkSession, functions as F

    spark = SparkSession.builder.getOrCreate()
except ImportError:
    DataFrame = SparkSession = F = spark = None

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None

from fabric_utils import (
    get_lakehouse_id,
    get_lakehouse_path,
    get_delta_tables_in_lakehouse,
    resolve_workspace_id,
)


def delta_table_exists(lakehouse_name: str, tbl: str) -> bool:
    """Check if a delta table exists at the given path.

    Parameters:
    path (str): The directory where the delta table is stored.
    tbl (str): The name of the delta table.

    Returns:
    bool: True if the delta table exists, False otherwise.
    """
    try:
        # Use the DeltaTable class to access the delta table
        path = get_lakehouse_path(lakehouse_name)
        DeltaTable.forPath(spark, os.path.join(path, tbl))
        # If no exception is raised, the delta table exists
        return True
    except Exception as e:
        # If an exception is raised, the delta table does not exist
        return False


def read_delta_table(lakehouse_name: str, table_name: str) -> DataFrame:
    """Reads a delta table from a given path and table name.

    Args:
        lakehouse_name (str): Name of the Lakehouse where the table exists.
        table_name (str): The name of the delta table.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with the delta table data.
    """
    path = get_lakehouse_path(lakehouse_name)
    return spark.read.format("delta").load(f"{path}/{table_name}")


def create_or_replace_delta_table(
    df: DataFrame, lakehouse_name: str, table_name: str, mode_type: str = "overwrite"
) -> None:
    """Create or replace a delta table from a dataframe.

    Args:
        df (pyspark.sql.DataFrame): The dataframe to write to the delta table.
        lakehouse_name (str): Lakehouse where delta table is stored.
        table_name (str): The name of the delta table.
        mode_type (str, optional): The mode for writing the delta table. Defaults to "overwrite".

    Returns:
        None
    """
    path = get_lakehouse_path(lakehouse_name)
    (
        df.write.mode(mode_type)
        .option("mergeSchema", "true")
        .format("delta")
        .save(f"{path}/{table_name}")
    )


def upsert_delta_table(
    lakehouse_name: str,
    table_name: str,
    df: DataFrame,
    merge_condition: str,
    update_condition: dict = None,
) -> None:
    """Updates or inserts rows into a delta table with the given data frame and conditions.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        table_name (str): The name of the delta table.
        df (pyspark.sql.DataFrame): The data frame to merge with the delta table.
        merge_condition (str): The condition to match the rows for merging.
        update_condition (dict, optional): The dictionary of column names and values to update when matched. Defaults to None.

    Returns:
        None
    """
    table_path = get_lakehouse_path(lakehouse_name)
    delta_table = DeltaTable.forPath(spark, os.path.join(table_path, table_name))
    if update_condition is None:
        # If update_condition is None, just insert new rows when not matched
        (
            delta_table.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # Otherwise, update existing rows when matched and insert new rows when not matched
        (
            delta_table.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdate(set=update_condition)
            .execute()
        )


def create_or_insert_table(
    df: DataFrame,
    lakehouse_name: str,
    table_name: str,
    primary_key: str,
    merge_key: str,
) -> None:
    """Create or insert a delta table from a dataframe.

    Args:
        df (DataFrame): The dataframe to be inserted or used to create the table.
        lakehouse_name (str): The name of the lakehouse where the table is located.
        table_name (str): The name of the table to be created or inserted.
        primary_key (str): The name of the column that serves as the primary key of the table.
        merge_key (str): The name of the column that serves as the merge key of the table.
    """
    if delta_table_exists(lakehouse_name, table_name):
        # Get the maximum value of the primary key from the existing table
        max_primary_key = (
            read_delta_table(lakehouse_name, table_name)
            .agg(F.max(primary_key))
            .collect()[0][0]
            + 1
        )
        # Increment the primary key of the dataframe by the maximum value
        df = df.withColumn(primary_key, F.col(primary_key) + F.lit(max_primary_key))
        # Format the merge condition with the merge key
        merge_condition = f"t.{merge_key} = s.{merge_key}"
        # Upsert the dataframe into the existing table
        upsert_delta_table(lakehouse_name, table_name, df, merge_condition)
    else:
        # Create a new table from the dataframe
        create_or_replace_delta_table(df, lakehouse_name, table_name)


def create_spark_dataframe(data: list, schema: str):
    """
    Create a Spark DataFrame with the given data and schema.

    Args:
        data (list): The data to include in the DataFrame.
        schema (str): The schema for the DataFrame.

    Returns:
        DataFrame: A Spark DataFrame containing the data.
    """
    return spark.createDataFrame(data, schema)


def get_row_count(lakehouse_or_link: str, table_name: str = None) -> int:
    """Get the row count from a delta table or a web page.

    Args:
        table_path: The path of the table or web page.
        table_name: The name of the table. Defaults to None.

    Returns:
        The row count as an integer.
    """
    # If the table name is None, assume the table path is a web page
    if table_name is None:
        # Extract the row count from the HTML table
        try:
            import pyspark.pandas as ps

            row_count = int(
                ps.read_html(lakehouse_or_link)[1]
                .All.str.replace(r"[^\d]", "", regex=True)
                .iloc[0]
            )
        except ValueError:
            print("Invalid row count in the web page")
            return
    else:
        # Get the row count from the delta table
        row_count = read_delta_table(lakehouse_or_link, table_name).count()

    return row_count


def compare_row_count(
    table1_lakehouse: str,
    table1_name: str,
    table2_lakehouse: str,
    table2_name: str = None,
) -> None:
    """Compare the row count of two tables and exit or print the difference.

    This function compares the row count of two delta tables or a delta table and a web page.
    It exits the notebook with message "No new data" if the row counts are equal, or prints the difference otherwise.

    Args:
        table1_lakehouse: The lakehouse of the first table.
        table1_name: The name of the first table.
        table2_lakehouse: The lakehouse of the second table or web page.
        table2_name: The name of the second table. Defaults to None.

    Returns:
        None
    """

    # Check if the first table exists as a delta table
    if delta_table_exists(table1_lakehouse, table1_name):
        # Get the row count and the match IDs from the first table
        row_count_1 = get_row_count(table1_lakehouse, table1_name)

        # Get the row count from the second table or web page
        row_count_2 = get_row_count(table2_lakehouse, table2_name)

        # Compare the row counts and exit or print accordingly
        if row_count_1 == row_count_2:
            # If the row counts are equal, exit the notebook with message "No new data"
            import notebookutils

            notebookutils.notebook.exit("No new data")
        else:
            print(f"Cricsheet has {row_count_2 - row_count_1} more matches added")


def optimize_and_vacuum_table_api(
    lakehouse_name: str, table_name: str, workspace=None, client=None
) -> str:
    """
    Optimize and vacuum a table in a lakehouse.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        table_name (str): The name of the table.
        workspace (str, optional): The name or ID of the workspace. If not provided, it uses the current workspace ID.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.

    Returns:
        Optional[str]: The job instance ID of the job, or None if an error occurs.
    """
    # Prepare the execution data payload
    execution_data = {
        "tableName": table_name,
        "optimizeSettings": {"vOrder": True},
        "vacuumSettings": {"retentionPeriod": "7.01:00:00"},
    }

    # Call start_on_demand_job to start the job
    job_instance_id = start_on_demand_job(
        item_id=get_lakehouse_id(
            lakehouse_name,
            resolve_workspace_id(workspace, client=client),
            client=client,
        ),
        job_type="TableMaintenance",
        execution_data=execution_data,
        workspace=resolve_workspace_id(workspace, client=client),
        client=client,
    )

    return job_instance_id


def optimize_and_vacuum_table(
    lakehouse_name: str, table_name: str, retain_hours: int = None
) -> bool:
    """
    Optimizes and vacuums a Delta table in a lakehouse.

    Args:
        lakehouse_name: Name of the lakehouse containing the table.
        table_name: Name of the table to optimize and vacuum.
        retain_hours: Optional number of hours to retain deleted data files.

    Returns:
        True on success, False on failure.
    """

    full_table_name = f"{lakehouse_name}.{table_name}"
    retain_syntax = ""
    if retain_hours is not None:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
        retain_syntax = f" RETAIN {retain_hours} HOURS"

    try:
        spark.sql(f"OPTIMIZE {full_table_name}")
        spark.sql(f"VACUUM {full_table_name}{retain_syntax}")
    except Exception as e:
        raise Exception(f"Error optimizing and vacuuming {full_table_name}: {e}")


def optimize_and_vacuum_items_api(
    items_to_optimize_vacuum: str | dict,
    log_lakehouse: str = None,
    job_category: str = None,
    parent_job_name: str = None,
    parallelism: int = 3,
    client=None,
) -> None:
    """Optimize and vacuum tables in lakehouses.

    Args:
        items_to_optimize_vacuum: A dictionary of lakehouse names and table lists, or a string that can be evaluated to a dictionary, or a single lakehouse name.
        log_lakehouse: The name of the lakehouse where the job details will be logged, if any.
        job_category: The category of the job, if any.
        parent_job_name: The name of the parent job, if any.
        parallelism: The number of tables to optimize and vacuum concurrently, default is 3.
        client: An optional pre-initialized client instance. If provided, it will be used instead of initializing a new one.
    """
    from job_operations import insert_or_update_job_table, get_lakehouse_job_status

    # Create a queue to store the pending tables
    queue = [
        (item[0], item[1])
        for item in prepare_optimization_items(items_to_optimize_vacuum)
    ]

    # Optimize and vacuum each table in each lakehouse and store the operation ids
    job_details = {}
    # Create a list to store the running tables
    running = []
    operation_status_dict = {}
    # Loop until the queue is empty and the running list is empty
    while queue or running:
        # If the running list is not full and the queue is not empty, pop a table from the queue and start the operation
        while len(running) < parallelism and queue:
            lakehouse_name, table_name = queue.pop(0)
            operation_id = optimize_and_vacuum_table_api(
                lakehouse_name, table_name, client=client
            )
            if log_lakehouse:
                insert_or_update_job_table(
                    lakehouse_name=log_lakehouse,
                    job_name=f"{lakehouse_name}.{table_name}",
                    parent_job_name=parent_job_name,
                    request_id=operation_id,
                    job_category=job_category,
                )
            job_details[lakehouse_name, table_name] = operation_id
            running.append((lakehouse_name, table_name, operation_id))
        # Check the status of the running tables and remove the ones that are finished
        for lakehouse_name, table_name, operation_id in running.copy():
            operation_details = get_lakehouse_job_status(
                operation_id, lakehouse_name, client=client
            )
            operation_status = operation_details["status"]
            if operation_status not in ["NotStarted", "InProgress"]:
                running.remove((lakehouse_name, table_name, operation_id))
                operation_status_dict[lakehouse_name, table_name] = operation_status
                if log_lakehouse:
                    start_time = dateutil.parser.isoparse(
                        operation_details["startTimeUtc"]
                    )
                    end_time = dateutil.parser.isoparse(operation_details["endTimeUtc"])
                    duration = (end_time - start_time).total_seconds()
                    msg = operation_details["failureReason"]
                    insert_or_update_job_table(
                        lakehouse_name=log_lakehouse,
                        job_name=f"{lakehouse_name}.{table_name}",
                        parent_job_name=parent_job_name,
                        request_id=operation_id,
                        status=operation_status,
                        duration=duration,
                        job_category=job_category,
                        message=msg,
                    )

    # Raise an exception if any table failed to optimize
    failed_tables = [
        f"{lakehouse_name}.{table_name}"
        for (lakehouse_name, table_name), status in operation_status_dict.items()
        if status == "Failed"
    ]
    if failed_tables:
        raise Exception(
            f"The following tables failed to optimize: {', '.join(failed_tables)}"
        )


def optimize_and_vacuum_items(
    items_to_optimize_vacuum: str | dict, retain_hours: int = None, parallelism: int = 4
) -> None:
    """
    Optimizes and vacuums Delta tables in parallel across multiple lakehouses.

    Args:
        items_to_optimize_vacuum: A string representing a single item (lakehouse.table)
                                    or a dictionary where keys are lakehouse names
                                    and values are lists of tables (str) or None
                                    to get all tables from that lakehouse.
        retain_hours: Optional number of hours to retain deleted data files.
        parallelism: Number of parallel threads to use for optimization/vacuum tasks.
    """

    # Prepare tasks
    tasks = prepare_optimization_items(items_to_optimize_vacuum, retain_hours)

    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = []
        for task in tasks:
            futures.append(executor.submit(optimize_and_vacuum_table, *task))
        for future in as_completed(futures):
            # No explicit return value handling, potential errors handled in worker function
            pass

    print("Optimization and vacuum completed.")


def prepare_optimization_items(
    items_to_optimize_vacuum: dict, retain_hours: int = None
) -> list:
    """
    Prepares a list of items for optimizing and vacuuming tables in a lakehouse.

    Parameters:
    items_to_optimize_vacuum (dict or str): A dictionary with lakehouse names as keys and lists of table names as values. If a string is provided, it's evaluated to a dictionary or converted into one with a None value.
    retain_hours (int, optional): The number of hours to retain the data during the vacuum process. Defaults to None.

    Returns:
    list: A list of tuples, each containing the lakehouse name, table name, and retain_hours for the optimization task.
    """
    # Evaluate string input to a dictionary or encapsulate it in a dictionary
    if isinstance(items_to_optimize_vacuum, str):
        try:
            items_to_optimize_vacuum = ast.literal_eval(items_to_optimize_vacuum)
        except ValueError:
            items_to_optimize_vacuum = {items_to_optimize_vacuum: None}

    items = []
    # Iterate over the lakehouses and their respective tables
    for lakehouse_name, table_list in items_to_optimize_vacuum.items():
        # If no table list is provided, retrieve all tables in the lakehouse
        table_list = table_list or get_delta_tables_in_lakehouse(lakehouse_name)
        # Ensure the table_list is a list even if a single table name is provided
        table_list = [table_list] if isinstance(table_list, str) else table_list
        # Create a task for each table
        for table_name in table_list:
            items.append((lakehouse_name, table_name, retain_hours))

    return items