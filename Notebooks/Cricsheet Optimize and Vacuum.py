#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Optimize and Vacuum
# 
# New notebook

# # Initialize Parameters

# In[ ]:


raw_lakehouse = "lh_bronze"
items_to_optimize_vacuum = {'lh_gold': None} # None means all tables in the lakehouse
optimize_and_vacuum = True
master_job_name = None
optimize_parallelism = 3


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
    items_to_optimize_vacuum=items_to_optimize_vacuum,
    parallelism = optimize_parallelism,
    log_lakehouse=raw_lakehouse,
    job_category='Optimize and Vacuum',
    parent_job_name=master_job_name,
    )

