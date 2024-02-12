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


semantic_model_list = "Cricsheet Model" # Pass multiple coma seperated "model1,model2"


# # Refresh and Wait for Dataset completion

# In[ ]:


# Split the dataset_list string by commas and store the result as a list
semantic_model_list = semantic_model_list.split(",")

# Start the dataset refresh and wait for completion
refresh_and_wait(semantic_model_list)

