#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Orchestrator
# 
# New notebook

# # Create DAG

# In[ ]:


DAG = {
    "activities": [
        {
            "name": "Cricsheet Ingest Data", # activity name, must be unique
            "path": "Cricsheet Ingest Data", # notebook path
            "timeoutPerCellInSeconds": 1800, # max timeout for each cell, default to 90 seconds
        },
        {
            "name": "Cricsheet Build Facts and Dimensions",
            "path": "Cricsheet Build Facts and Dimensions",
            "timeoutPerCellInSeconds": 1800,
            "dependencies": ["Cricsheet Ingest Data"]
        },
        {
            "name": "Cricsheet Model Refresh",
            "path": "Cricsheet Model Refresh",
            "timeoutPerCellInSeconds": 1800,
            "dependencies": ["Cricsheet Build Facts and Dimensions"]
        }
    ]
}


# # Execute DAG

# In[ ]:


mssparkutils.notebook.runMultiple(DAG)

