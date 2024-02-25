#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Optimize and Vacuum
# 
# New notebook

# # Initialize Parameters

# In[ ]:


raw_lakehouse = "lh_bronze"
items_to_optimize_vacuum_str = "{'lh_bronze': None, 'lh_gold': None}" # None means all tables in the lakehouse
optimize_and_vacuum = True
retention_hours = 0 # None means default 7 days
master_job_name = None


# # Check if optimize_and_vacuum flag is set, if not Exit notebook

# In[ ]:


if not optimize_and_vacuum:
    mssparkutils.notebook.exit("optimize_and_vacuum flag not set")


# # Initialize Common Functions and Libraries

# In[ ]:


get_ipython().run_line_magic('run', '"/Common Functions"')


# # Execute Optimize Vacuum

# In[ ]:


for lakehouse, tables in ast.literal_eval(items_to_optimize_vacuum_str).items():
    # Ensure tables is a list
    tables = tables if isinstance(tables, list) else [tables]

    # Optimize and log each table
    for table in tables:
        job_name =  f"{lakehouse}.{table}" if table else f"All tables in {lakehouse}"
        execute_and_log(
            function=optimize_and_vacuum_table,
            lakehouse_name=lakehouse,
            table_name=table,
            retention_hours = retention_hours,
            log_lakehouse = raw_lakehouse,
            job_name=job_name,
            job_category="Optimize and Vacuum",
            parent_job_name=master_job_name
        )

