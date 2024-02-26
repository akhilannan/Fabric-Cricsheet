#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Orchestrator
# 
# New notebook

# # Initialize Common Functions and Libraries

# In[ ]:


get_ipython().run_line_magic('run', '"/Common Functions"')


# # Initialize Parameters

# In[ ]:


raw_lakehouse = "lh_bronze"
clean_lakehouse = "lh_gold"
semantic_model_list = "Cricsheet Model" # Can pass multiple coma seperated "model1,model2"
master_job_name = "Cricsheet Master"
optimize_and_vacuum = False # Change to True to enable optimize and vaccum
items_to_optimize_vacuum = {clean_lakehouse: None, raw_lakehouse: None} # None corresponds to all tables in Lakehouse
optimize_parallelism = 3


# # Create DAG

# In[ ]:


DAG = {
    "activities": [
        {
            "name": "Cricsheet Ingest Data", # activity name, must be unique
            "path": "Cricsheet Ingest Data", # notebook path
            "timeoutPerCellInSeconds": 1800, # max timeout for each cell, default to 90 seconds
            "args": {"raw_lakehouse": raw_lakehouse, "master_job_name": master_job_name} # notebook parameters
        },
        {
            "name": "Cricsheet Build Facts and Dimensions",
            "path": "Cricsheet Build Facts and Dimensions",
            "timeoutPerCellInSeconds": 1800,
            "dependencies": ["Cricsheet Ingest Data"],
            "args": {"raw_lakehouse": raw_lakehouse, "clean_lakehouse": clean_lakehouse, "master_job_name": master_job_name}
        },
        {
            "name": "Cricsheet Optimize and Vacuum",
            "path": "Cricsheet Optimize and Vacuum",
            "timeoutPerCellInSeconds": 1800,
            "dependencies": ["Cricsheet Build Facts and Dimensions"],
            "args": {"optimize_and_vacuum": optimize_and_vacuum, "master_job_name": master_job_name, "items_to_optimize_vacuum": str(items_to_optimize_vacuum), "optimize_parallelism": optimize_parallelism}
        },
        {
            "name": "Cricsheet Model Refresh",
            "path": "Cricsheet Model Refresh",
            "timeoutPerCellInSeconds": 180,
            "dependencies": ["Cricsheet Optimize and Vacuum"],
            "args": {"semantic_model_list": semantic_model_list, "master_job_name": master_job_name, "raw_lakehouse": raw_lakehouse}
        }
    ]
}


# # Execute DAG

# In[ ]:


execute_and_log(function=execute_dag,
                log_lakehouse=raw_lakehouse, 
                job_name=master_job_name,
                job_category= "Orchestrator",
                dag=DAG)

