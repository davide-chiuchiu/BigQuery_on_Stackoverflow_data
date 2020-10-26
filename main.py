#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 18:40:52 2020

@author: dabol99

This script contains is a minimum working example to use python to interface to
a big database with bigquery, perform some simple postprocessing of the 
database with SQL, and then make some exploratory analysis
"""

# import packages
import os
import google.auth
from google.cloud import bigquery

# set current work directory to the one with this script.
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# import functions from auxiliary files
from errors_aux import custom_error
from import_sql_query_files import import_sql_query

# set environment variable to point to the google credentials. Raise error
# without the .json credential file
google_credentials_path = 'bigquery-stackoverflow-DC-fdb49371cf87.json'
if os.path.exists(google_credentials_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials_path
else:
    error_no_credentials = "File bigquery-stackoverflow-DC-fdb49371cf87.json " + \
                            "with google credentials not in working directory. "+ \
                            "Move it here if you have it, or ask Davide for " + \
                            "one if you don't."
    raise custom_error(error_no_credentials)


# read query to perform from the file 
query_file_path = "stack_overflow_query.sql"
stackoverflow_query = import_sql_query(query_file_path)

# build authentication object 
# guidelines at https://cloud.google.com/bigquery/docs/bigquery-storage-python-pandas
credentials, project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

# initialize bigquery client with project id and credentials
client = bigquery.Client(credentials = credentials, project = project_id)

# point to stackoverflow dataset as default dataset in the job_query_configurations 
stackoverflow_dataset_id = 'bigquery-public-data.stackoverflow'
job_query_config = bigquery.job.QueryJobConfig(default_dataset = stackoverflow_dataset_id)

# perform query with job_query_config as configurations. This allows to 
# reference only tables within the bigquery-public -data.stackoverflow dataset
query_job = client.query(stackoverflow_query, job_config = job_query_config)

# # # For examples on more sophisticated queries follows the examples in
# # # https://console.cloud.google.com/marketplace/product/stack-exchange/stack-overflow?filter=solution-type:dataset&q=public%20data&id=46a148ff-896d-444c-b08d-360169911f59
# # also the code on
# # https://cloud.google.com/bigquery/docs/samples


# store results in dataframe
dataframe_query = query_job.result().to_dataframe()

# close client
client.close()
