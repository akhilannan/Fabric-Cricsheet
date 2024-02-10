#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Ingest Data
# 
# New notebook

# # Initialize Common Functions and Libraries

# In[ ]:


get_ipython().run_line_magic('run', '"/Common Functions"')


# # Initialize Variables

# In[ ]:


dataset_url = "https://cricsheet.org/downloads/"
file_name = "all_json.zip"
raw_folder = "cricketRaw"
table_name = "t_cricsheet"
base_url = dataset_url + file_name
local_path = create_mount_point(raw_lakehouse_id)
spark_raw_path = raw_abfss_path +"/Files/" + raw_folder
lake_path = local_path + "/Files/" + raw_folder + "/"
lake_zip_file = lake_path + file_name


# # Check if Cricsheet has new matches added, else Quit

# In[ ]:


if delta_table_exists(raw_table_path, table_name):
    match_count_web = int(
        pd
        .read_html(dataset_url)[1]
        .All
        .str
        .extract('([\\d,]+)')
        .iloc[0,0]
        .replace(',', '')
        )

    match_count_tbl = read_delta_table(raw_table_path, table_name).count()

    if(match_count_web == match_count_tbl):
        mssparkutils.notebook.exit(1)
    else:
        print("Cricsheet has " + str(match_count_web - match_count_tbl) + " more matches added" )


# # Download zip file from Cricsheet

# In[ ]:


pathlib.Path(lake_path).mkdir(parents=True, exist_ok=True) 
urlretrieve(base_url, lake_zip_file)


# # Unzip and remove zip + readme files

# In[ ]:


if os.path.exists(lake_zip_file):
    unzip_parallel(lake_path, lake_zip_file)
    os.remove(lake_zip_file)
    os.remove(lake_path + "README.txt")


# # Create t_cricsheet table

# In[ ]:


full_file_name = F.input_file_name()
file_name_array = F.split(full_file_name, "/")
match_file_name_with_extn = F.element_at(file_name_array, -1)
match_file_name = F.regexp_replace(match_file_name_with_extn, ".json", "")
file_match_id = F.regexp_extract(match_file_name, "\d+", 0).cast("int")

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
    .select(F.when(match_file_name.like("wi_%"),file_match_id * -1).otherwise(file_match_id).alias("match_id"),
            F.col("info").alias("match_info"), 
            F.col("innings").alias("match_innings"),
            F.lit(match_file_name_with_extn).alias("file_name"),
            F.current_timestamp().alias("last_update_ts"))
    )
        
create_or_replace_delta_table(cricket_df, raw_table_path, table_name)

