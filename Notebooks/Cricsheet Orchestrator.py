#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Orchestrator
# 
# New notebook

# In[ ]:


raw_lakehouse = "lh_bronze"
clean_lakehouse = "lh_gold"
semantic_model_list = "Cricsheet Model" # Pass multiple coma seperated "model1,model2"


# # Create DAG

# In[ ]:


DAG = {
    "activities": [
        {
            "name": "Cricsheet Ingest Data", # activity name, must be unique
            "path": "Cricsheet Ingest Data", # notebook path
            "timeoutPerCellInSeconds": 1800, # max timeout for each cell, default to 90 seconds
            "args": {"raw_lakehouse": raw_lakehouse} # notebook parameters
        },
        {
            "name": "Cricsheet Build Facts and Dimensions",
            "path": "Cricsheet Build Facts and Dimensions",
            "timeoutPerCellInSeconds": 1800,
            "dependencies": ["Cricsheet Ingest Data"],
            "args": {"raw_lakehouse": raw_lakehouse, "clean_lakehouse": clean_lakehouse}
        },
        {
            "name": "Cricsheet Model Refresh",
            "path": "Cricsheet Model Refresh",
            "timeoutPerCellInSeconds": 180,
            "dependencies": ["Cricsheet Build Facts and Dimensions"],
            "args": {"semantic_model_list": semantic_model_list}
        }
    ]
}


# # Execute DAG

# In[ ]:


mssparkutils.notebook.runMultiple(DAG)

