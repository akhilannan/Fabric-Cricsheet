#!/usr/bin/env python
# coding: utf-8

# ## Cricsheet Model Refresh
# 
# New notebook

# # Initialize Common Functions and Libraries

# In[ ]:


get_ipython().run_line_magic('run', '"/Common Functions"')


# # Set Semantic Model Lists

# In[ ]:


semantic_model_list_str = "Cricsheet Model" # Pass multiple coma seperated "model1,model2"
raw_lakehouse = "lh_bronze"
master_job_name = None


# # Refresh and Wait for Dataset completion

# In[ ]:


job_category = "Semantic Model Refresh"

# Split the dataset_list string by commas and store the result as a list
semantic_model_list = semantic_model_list_str.split(",")

# Start the dataset refresh and wait for completion
refresh_and_wait(dataset_list = semantic_model_list, logging_lakehouse = raw_lakehouse, parent_job_name = master_job_name, job_category = job_category)

