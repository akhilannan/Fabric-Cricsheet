#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Optimize and Vacuum
# 
# New notebook

# # Initialize Parameters

# In[ ]:


raw_lakehouse = "lh_bronze"
items_to_optimize_vacuum_str = "{'lh_gold': None, 'lh_bronze': None}" # None means all tables in the lakehouse
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


optimize_and_vacuum_items(
    items_to_optimize_vacuum=items_to_optimize_vacuum_str,
    retention_hours=retention_hours,
    log_lakehouse=raw_lakehouse,
    job_category='Optimize and Vacuum',
    parent_job_name=master_job_name,
    )

