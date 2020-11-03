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
from google.cloud import bigquery

# set current work directory to the one with this script.
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# import functions from auxiliary files
from bigquery_aux import point_google_authentication_as_global_variable
from bigquery_aux import initialize_bigquery_client
from bigquery_aux import send_query_to_database


'''
Import global variables to point to the google cloud authentication credentials,
initialize bigquery client and create dataset reference as default option for queries
'''
# set environment variable to point to the google credentials. Raise error
# without the .json credential file
google_credentials_path = os.path.join(os.getcwd(), 'Google_credentials', 'bigquery-stackoverflow-DC-fdb49371cf87.json')
point_google_authentication_as_global_variable(google_credentials_path)

# initialize bigquery client 
bigquery_client = initialize_bigquery_client()

# point to stackoverflow dataset as default dataset in the job_query_configurations 
stackoverflow_dataset_id = 'bigquery-public-data.stackoverflow'
job_query_config = bigquery.QueryJobConfig(default_dataset = stackoverflow_dataset_id)


'''
Import info about the dataset structure and save it as .csv 
'''
database_structure = send_query_to_database(bigquery_client, job_query_config, "schema_stack_overflow_query.sql", "sql_queries")
database_structure = database_structure.pivot_table(index = 'column_name', columns = 'table_name', aggfunc='size')
database_structure.to_csv('database_entity_relation_diagram.csv')


'''
Perform query to get [fill in info]
'''
dataframe_query = send_query_to_database(bigquery_client, job_query_config, "stack_overflow_query.sql", "sql_queries")



# close client
bigquery_client.close()
