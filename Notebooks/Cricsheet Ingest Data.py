#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Ingest Data
# 
# New notebook

# # Initialize Common Functions and Libraries

# In[10]:


get_ipython().run_line_magic('run', '"/Common Functions"')


# # Set Lakehouse name Parameter

# In[2]:


raw_lakehouse = "lh_raw"


# # Initialize Variables

# In[ ]:


# Define the URL of the dataset source
dataset_url = "https://cricsheet.org/downloads/"
# Define the name of the zip file containing the JSON files
file_name = "all_json.zip"
# Define the name of the folder where the raw data will be stored
raw_folder = "cricketRaw"
# Define the name of the delta table where the raw data will be loaded
table_name = "t_cricsheet"
# Concatenate the base URL and the file name to get the full URL of the zip file
base_url = dataset_url + file_name
# Define the path of the table in the lakehouse
raw_table_path = get_lakehouse_path(raw_lakehouse, "spark", "Tables")
# Define the path of the folder in the lakehouse where the raw files will be saved
spark_raw_path = get_lakehouse_path(raw_lakehouse, "spark", "Files") + "/" + raw_folder
# Define the path of the folder in the local file system where the raw files will be extracted
lake_path = get_lakehouse_path(raw_lakehouse, "local", "Files") + "/" + raw_folder + "/"
# Define the path of the zip file in the local file system
lake_zip_file = lake_path + file_name


# # Check if Cricsheet has new matches added, else Quit

# In[11]:


# Check if the table exists and get the row count
# If the table does not exist, set min/max match_id to 0 and write mode to Overwrite
# If the table exists and the row count does not match the dataset, get the min/max match_id from the table and set write mode to Append
# If the table exists and the row count matches the dataset, exit the Notebook execution
max_match_id, min_match_id, write_mode = compare_row_count(raw_table_path, table_name, dataset_url)


# # Download zip file from Cricsheet

# In[ ]:


# Create a directory for the lake path if it does not exist
pathlib.Path(lake_path).mkdir(parents=True, exist_ok=True) 

# Download the data from the base URL and save it as a zip file in the lake path
urlretrieve(base_url, lake_zip_file)


# # Unzip and remove zip + readme files

# In[ ]:


# Check if the lake zip file exists in the current directory
if os.path.exists(lake_zip_file):
    # Unzip the lake zip file to the specified lake path using a parallel process
    unzip_parallel(lake_path, lake_zip_file)
    # Delete the lake zip file after unzipping
    os.remove(lake_zip_file)
    # Delete the README.txt file from the lake path
    os.remove(lake_path + "README.txt")


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
  .format("json")
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
        F.current_timestamp().alias("last_update_ts")
    )
    # Filter the data frame by the match id range
    .filter((F.col("match_id") > max_match_id) | (F.col("match_id") <  min_match_id))
    )
        
# Create or replace a delta table with the data frame
create_or_replace_delta_table(cricket_df, raw_table_path, table_name, write_mode)

