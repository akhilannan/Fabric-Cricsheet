#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Ingest Data
# 
# New notebook

# # Initialize Common Functions and Libraries

# In[ ]:


get_ipython().run_line_magic('run', '"/Common Functions"')


# # Set Lakehouse name Parameter

# In[ ]:


raw_lakehouse = "lh_bronze"
master_job_name = None


# # Initialize Variables

# In[ ]:


# Define the URL of the dataset source
dataset_url = "https://cricsheet.org/downloads/"
# Define the name of the zip file containing the JSON files
zip_file_name = "all_json.zip"
# Define the name of the folder where the raw data will be stored
raw_folder = "cricketRaw"
#Define raw file format
file_format = "json"
# Define the name of the delta table where the raw data will be loaded
table_name = "t_cricsheet"
# Concatenate the base URL and the file name to get the full URL of the zip file
base_url = dataset_url + zip_file_name
# Define the path of the folder in the lakehouse where the raw files will be saved
spark_raw_path = get_lakehouse_path(raw_lakehouse, "spark", "Files") + "/" + raw_folder
# Set job category
job_category = "Ingest to Raw"


# # Check if Cricsheet has new matches added, else Quit

# In[ ]:


execute_and_log(
    function=compare_row_count,
    table1_lakehouse=raw_lakehouse,
    table1_name=table_name,
    table2_lakehouse=dataset_url,
    log_lakehouse=raw_lakehouse,
    job_name='Compare Row Count Bronze',
    job_category = job_category,
    parent_job_name=master_job_name
    )


# # Download zip file from Cricsheet

# In[ ]:


execute_and_log(
    function=download_data,
    url=base_url,
    lakehouse = raw_lakehouse,
    path=raw_folder,
    log_lakehouse=raw_lakehouse,
    job_name='Download Cricsheet Data',
    job_category = job_category,
    parent_job_name=master_job_name
    )


# # Unzip Files in parallel

# In[ ]:


execute_and_log(
    function=unzip_parallel,
    lakehouse = raw_lakehouse,
    path=raw_folder,
    filename=zip_file_name,
    file_type = file_format,
    log_lakehouse=raw_lakehouse,
    job_name='Unzip Files',
    job_category = job_category,
    parent_job_name=master_job_name
    )


# # Create t_cricsheet table

# In[ ]:


# Get the full file name from the input
full_file_name = F.input_file_name()

# Split the file name by "/" and get the last element which will be the file name
file_name_array = F.split(full_file_name, "/")
file_name = F.element_at(file_name_array, -1)

# Remove the ".json" extension from the file name
match_file_name = F.regexp_replace(file_name, ".json", "")

# Extract the match id from the file name as an integer
file_match_id = F.regexp_extract(match_file_name, "\d+", 0).cast("int")

# Read the json file into a spark data frame with the specified schema
cricket_df = (
  spark
  .read
  .format(file_format)
  .option("multiline", "true")
  .schema("info string, innings string")
  .load(spark_raw_path)
)

cricket_df = (
    cricket_df
    # Select the following columns from the data frame
    .select(
        # If the match file name starts with "wi_", multiply the match id by -1. This is to distinguish between Women and Mens matches
        F.when(match_file_name.like("wi_%"),file_match_id * -1).otherwise(file_match_id).alias("match_id"),
        F.col("info").alias("match_info"), 
        F.col("innings").alias("match_innings"),
        # Add a new column with the file name as a literal value
        F.lit(file_name).alias("file_name"),
        # Add a new column with the current timestamp as the last update time
        F.current_timestamp().alias("last_update_ts"))
    )
        
# Create or replace a delta table with the data frame
execute_and_log(
    function=create_or_replace_delta_table,
    df=cricket_df,
    lakehouse_name=raw_lakehouse,
    table_name=table_name,
    log_lakehouse=raw_lakehouse,
    job_name=table_name,
    job_category = job_category,
    parent_job_name=master_job_name
    )

