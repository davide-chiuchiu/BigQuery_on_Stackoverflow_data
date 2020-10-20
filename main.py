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
from errors_aux import custom_error


# initialize client object using the bigquery key I generated from Google clouds
google_credentials_path = 'bigquery-stackoverflow-DC-fdb49371cf87.json'
if os.path.exists(google_credentials_path):
    client = bigquery.Client.from_service_account_json(google_credentials_path)
else:
    error_no_credentials = "File bigquery-stackoverflow-DC-fdb49371cf87.json " + \
                            "with google credentials not in working directory. "+ \
                            "Move it here if you have it, or ask Davide for " + \
                            "one if you don't."
    raise custom_error(error_no_credentials)


# create simple query
query_job = client.query(
    """
    SELECT
      CONCAT(
        'https://stackoverflow.com/questions/',
        CAST(id as STRING)) as url,
      view_count
    FROM `bigquery-public-data.stackoverflow.posts_questions`
    WHERE tags like '%google-bigquery%'
    ORDER BY view_count DESC
    LIMIT 10"""
)

# store results in dataframe
dataframe_query = query_job.result().to_dataframe()


