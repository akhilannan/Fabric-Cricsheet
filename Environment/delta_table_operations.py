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

from fabric_utils import get_lakehouse_table_path, convert_to_json


def delta_table_exists(
    lakehouse_name: str, table_name: str, schema_name: str = None
) -> bool:
    """Check if a delta table exists at the given path.

    Parameters:
    path (str): The directory where the delta table is stored.
    table_name (str): The name of the delta table.
    schema_name (str): The schema name (optional).

    Returns:
    bool: True if the delta table exists, False otherwise.
    """
    try:
        # Use the DeltaTable class to access the delta table
        table_path = get_lakehouse_table_path(lakehouse_name, table_name, schema_name)
        DeltaTable.forPath(spark, table_path)
        # If no exception is raised, the delta table exists
        return True
    except Exception as e:
        # If an exception is raised, the delta table does not exist
        return False


def read_delta_table(
    lakehouse_name: str, table_name: str, schema_name: str = None
) -> DataFrame:
    """Reads a delta table from a given path, schema name (optional), and table name.

    Args:
        lakehouse_name (str): Name of the Lakehouse where the table exists.
        table_name (str): The name of the delta table.
        schema_name (str, optional): The name of the schema. Defaults to None.

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with the delta table data.
    """
    table_path = get_lakehouse_table_path(lakehouse_name, table_name, schema_name)
    return spark.read.format("delta").load(table_path)


def create_or_replace_delta_table(
    df: DataFrame,
    lakehouse_name: str,
    table_name: str,
    schema_name: str = None,
    mode_type: str = "overwrite",
) -> None:
    """Create or replace a delta table from a dataframe.

    Args:
        df (pyspark.sql.DataFrame): The dataframe to write to the delta table.
        lakehouse_name (str): Lakehouse where delta table is stored.
        table_name (str): The name of the delta table.
        schema_name (str, optional): The name of the schema. Defaults to None.
        mode_type (str, optional): The mode for writing the delta table. Defaults to "overwrite".

    Returns:
        None
    """
    table_path = get_lakehouse_table_path(lakehouse_name, table_name, schema_name)
    (
        df.write.mode(mode_type)
        .option("mergeSchema", "true")
        .format("delta")
        .save(table_path)
    )


def upsert_delta_table(
    lakehouse_name: str,
    table_name: str,
    df: DataFrame,
    merge_condition: str,
    schema_name: str = None,
    update_condition: dict = None,
) -> None:
    """Updates or inserts rows into a delta table with the given data frame and conditions.

    Args:
        lakehouse_name (str): The name of the lakehouse.
        table_name (str): The name of the delta table.
        df (pyspark.sql.DataFrame): The data frame to merge with the delta table.
        merge_condition (str): The condition to match the rows for merging.
        schema_name (str, optional): The name of the schema. Defaults to None.
        update_condition (dict, optional): The dictionary of column names and values to update when matched. Defaults to None.

    Returns:
        None
    """
    table_path = get_lakehouse_table_path(lakehouse_name, table_name, schema_name)
    delta_table = DeltaTable.forPath(spark, table_path)
    merge_builder = delta_table.alias("t").merge(df.alias("s"), merge_condition)

    if update_condition:
        # If there's an update condition, update matched rows and execute immediately
        merge_builder.whenMatchedUpdate(set=update_condition).execute()
    else:
        # If no update condition, insert new rows when not matched
        merge_builder.whenNotMatchedInsertAll().execute()


def create_or_insert_table(
    df: DataFrame,
    lakehouse_name: str,
    table_name: str,
    primary_key: str,
    merge_key: str,
    schema_name: str = None,
) -> None:
    """Create or insert a delta table from a dataframe.

    Args:
        df (DataFrame): The dataframe to be inserted or used to create the table.
        lakehouse_name (str): The name of the lakehouse where the table is located.
        table_name (str): The name of the table to be created or inserted.
        primary_key (str): The name of the column that serves as the primary key of the table.
        merge_key (str): The name of the column that serves as the merge key of the table.
        schema_name (str, optional): The name of the schema. Defaults to None.
    """
    if delta_table_exists(lakehouse_name, table_name, schema_name):
        # Get the maximum value of the primary key from the existing table
        max_primary_key = (
            read_delta_table(lakehouse_name, table_name, schema_name)
            .agg(F.max(primary_key))
            .collect()[0][0]
            + 1
        )
        # Increment the primary key of the dataframe by the maximum value
        df = df.withColumn(primary_key, F.col(primary_key) + F.lit(max_primary_key))
        # Format the merge condition with the merge key
        merge_condition = f"t.{merge_key} = s.{merge_key}"
        # Upsert the dataframe into the existing table
        upsert_delta_table(lakehouse_name, table_name, df, merge_condition, schema_name)
    else:
        # Create a new table from the dataframe
        create_or_replace_delta_table(df, lakehouse_name, table_name, schema_name)


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


def get_row_count(
    lakehouse_or_link: str, table_name: str = None, schema_name: str = None
) -> int:
    """Gets the row count from a delta table or a web page.

    Args:
        lakehouse_or_link: The lakehouse name (if table) or web link.
        table_name: The table name (required if lakehouse).
        schema_name: The schema name (optional, for lakehouse tables).

    Returns:
        The row count, or None if there's an error.  Prints an error message if
        the web page row count is invalid.
    """
    if table_name:  # Check if it's a table
        return read_delta_table(lakehouse_or_link, table_name, schema_name).count()
    else:  # It's a web link
        try:
            import pyspark.pandas as ps

            row_count_str = (
                ps.read_html(lakehouse_or_link)[1]
                .All.str.replace(r"[^\d]", "", regex=True)
                .iloc[0]
            )
            return int(row_count_str)  # Convert to int after cleaning

        except (ValueError, IndexError, TypeError) as e:
            print(f"Invalid row count in web page: {e}")
            return None


def compare_row_count(
    table1_lakehouse: str,
    table1_name: str,
    table2_lakehouse: str,
    table2_name: str = None,
    table1_schema: str = None,
    table2_schema: str = None,
) -> None:
    """Compare the row count of two tables and exit or print the difference.

    This function compares the row count of two delta tables or a delta table and a web page.
    It exits the notebook with message "No new data" if the row counts are equal, or prints the difference otherwise.

    Args:
        table1_lakehouse: The lakehouse of the first table.
        table1_name: The name of the first table.
        table2_lakehouse: The lakehouse of the second table or web page.
        table2_name: The name of the second table. Defaults to None.
        table1_schema: Schema of table 1. Defaults to None.
        table2_schema: Schema of table 2. Defaults to None.

    Returns:
        None
    """

    # Check if the first table exists as a delta table
    if delta_table_exists(table1_lakehouse, table1_name, table1_schema):
        # Get the row count and the match IDs from the first table
        row_count_1 = get_row_count(table1_lakehouse, table1_name, table1_schema)

        # Get the row count from the second table or web page
        row_count_2 = get_row_count(table2_lakehouse, table2_name, table2_schema)

        # Compare the row counts and exit or print accordingly
        if row_count_1 == row_count_2:
            # If the row counts are equal, exit the notebook with message "No new data"
            import notebookutils

            notebookutils.notebook.exit("No new data")
        else:
            print(f"Cricsheet has {row_count_2 - row_count_1} more matches added")


def optimize_and_vacuum_table_schema(
    schema_name: str, table_name: str, retain_hours: int = None
) -> bool:
    """
    Optimizes and vacuums a Delta table in the current lakehouse.

    Args:
        schema_name: Name of the schema containing the table.
        table_name: Name of the table to optimize and vacuum.
        retain_hours: Optional number of hours to retain deleted data files.

    Returns:
        True on success, False on failure.
    """
    full_table_name = f"{schema_name}.{table_name}"
    retain_syntax = ""
    if retain_hours is not None:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
        retain_syntax = f" RETAIN {retain_hours} HOURS"

    try:
        spark.sql(f"OPTIMIZE {full_table_name}")
        spark.sql(f"VACUUM {full_table_name}{retain_syntax}")
        return True
    except Exception as e:
        raise Exception(f"Error optimizing and vacuuming {full_table_name}: {e}")


def process_delta_tables_maintenance(
    schema_table_map: str = None, retain_hours: int = None, parallelism: int = 4
) -> None:
    """
    Optimizes and vacuums Delta tables in parallel in the current lakehouse.

    Args:
        schema_table_map: String in format "Schema1:Table1,Table2|Schema2"
                         If None or empty string, includes all tables from all schemas.
        retain_hours: Optional number of hours to retain deleted data files.
        parallelism: Number of parallel threads to use for optimization/vacuum tasks.
    """
    tasks = prepare_maintenance_tasks(schema_table_map, retain_hours)

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = []
        for task in tasks:
            futures.append(executor.submit(optimize_and_vacuum_table_schema, *task))
        for future in as_completed(futures):
            pass

    print("Optimization and vacuum completed.")


def prepare_maintenance_tasks(
    schema_table_map: str = None, retain_hours: int = None
) -> list:
    """
    Prepares a list of tasks for optimizing and vacuuming tables.

    Args:
        schema_table_map: String in format "Schema1:Table1,Table2|Schema2"
                         If None or empty string, includes all tables from all schemas.
        retain_hours: The number of hours to retain the data during the vacuum process.

    Returns:
        list: A list of tuples (schema_name, table_name, retain_hours)
    """
    tasks = []
    schema_table_list = convert_to_json(schema_table_map, target="lakehouse")

    if not schema_table_list:
        schema_table_list = [{"schema": schema} for schema in get_all_schemas()]

    for item in schema_table_list:
        schema = item["schema"]
        tables = (
            [item["table"]] if "table" in item else get_delta_tables_in_schema(schema)
        )
        tasks.extend((schema, table, retain_hours) for table in tables)

    return tasks


def get_delta_tables_in_schema(schema_name: str) -> list:
    """
    Get all Delta tables in a specific schema in the current lakehouse.

    Args:
        schema_name: Name of the schema to query.

    Returns:
        List[str]: List of table names in the schema.
    """
    try:
        tables_df = (
            spark.sql(f"SHOW TABLES IN {schema_name}")
            .filter("isTemporary = false")
            .select("tableName")
        )
        return [row.tableName for row in tables_df.collect()]
    except Exception as e:
        print(f"Error getting tables for schema {schema_name}: {e}")
        return []


def get_all_schemas() -> list:
    """
    Get all schemas in the current lakehouse.

    Returns:
        List[str]: List of schema names.
    """
    try:
        schemas_df = spark.sql("SHOW SCHEMAS")
        return [row.namespace.replace("`", "") for row in schemas_df.collect()]
    except Exception as e:
        print(f"Error getting schemas: {e}")
        return []
