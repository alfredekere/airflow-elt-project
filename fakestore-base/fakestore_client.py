# build a script that reads data from 4 endpoints of our API
import json
import os
import requests
import pandas as pd
from time import sleep
from datetime import datetime, timedelta
from pprint import pprint
from hashlib import md5
from pathlib import Path



base_url = "https://fakestoreapi.com/"
endpoints = ["products", "carts", "users"]

def _transform_col_names(column_names:list)->list:
    # iterate through col names and split at "." occurences, get the first elemnt of the result
    # return a list containing the new column names
    transformed_column_names = []
    for col in column_names:
        if "." in col:
            new_column_name:str = col.split(".")[-1]
            transformed_column_names.append(new_column_name.lower())
        else:
            transformed_column_names.append(col.lower())
            
    return transformed_column_names


def _extract_data(endpoint:str)-> list:
    """ used for extracting Data from the API.

    `args:`
        None
    `returns:`
        list: a list of dictionaries containing product details.
    """
    # implement exception handling
    try:
        print(f"{endpoint} Extraction in progress....")
        raw_data = requests.get(base_url + endpoint).json()
        
    except Exception as e:
        print(f"ERROR: Occured during {endpoint} extraction-> ",e)
        raise e
    print(f"{endpoint} Extraction Succesful. -> Got `{len(raw_data)}` {endpoint}")
    return raw_data


def _load_api_raw_data(raw_data:list, file_id:str)->str:
    """function to load raw data to a file store.

    parameters
    ------------
    raw_data: list
        a list of dictionaries containing product details.
    file_id: str
        the endpoint name.
    
    returns
    ------------
    str
        the file path.
    
    """
    #  write the data to a file
    base_dir = os.getcwd()+"/data"
    file_path = f"{base_dir}/{file_id}.json"
    # #  check if a data directrory exists and create if needed
    # if not os.path.exists(f"{base_dir}/data"):
    #     os.mkdir(f"{base_dir}/data")
    try:

        with open(f"{file_path}", "w") as f:
            # prepare data output with metadata
            data_output = {
                f"{file_id}_count": len(raw_data),
                "data":raw_data}
            
            json.dump(data_output, f,indent=4)
        print(f"Raw data saved to data/{file_id}.json")

    except Exception as e:
        print(f"ERROR: Occured during {file_id} data saving-> ",e)
        raise e
    return file_path


def extract_and_load_to_file_storage(endpoint:str)->str:
    """
    Function extracts data from the API and loads it to a file store.

    parameters
    ------------
    endpoint: str
        the endpoint to extract data from.
    
    returns
    ------------
    str
        the endpoint name.
    """

    # extract data from API
    # call the extract fxn locally
    raw_data = _extract_data(endpoint)
    
    # call the load fxn loally
    file_path_loaded = _load_api_raw_data(raw_data=raw_data,file_id=endpoint)
    return file_path_loaded


def transform_api_data(
        file_path:str)-> pd.DataFrame:
    """
    Function to take in json file path and transform to a dataframe.

    parameters
    ------------
    file_path: str
        path to the json file.
    
    returns
    ------------
    pd.DataFrame
        a dataframe containing the transformed data.
    
    """
    
    with open(file_path, "r") as f:
        raw_data = json.load(f)
    # flatten the json data
    if "carts" in file_path:
        transformed_data = pd.json_normalize(raw_data["data"],"products",meta=["id","userId","date"])
        transformed_data = transformed_data.reindex(columns=["id","date","userId","productId","quantity"])

    else:

        transformed_data = pd.json_normalize(raw_data["data"])
    # transform column names
    transformed_data.columns = _transform_col_names(transformed_data.columns.to_list())
    # add timestamp column to dataframe
    transformed_data["updated_at"] = datetime.strftime(datetime.now()+ timedelta(hours=1),"%Y-%m-%d %H:%M:%S")
    # dropped unwanted columns

    if "users" in file_path:
        transformed_data["password"] = (transformed_data["password"]).apply(
                                            lambda val: md5(val.encode()).hexdigest())

    transformed_data_clean = transformed_data.drop(columns=["__v"],errors="ignore")
    transformed_data_clean.to_csv(file_path.replace("json","csv"),index=False)

    return transformed_data


# build a script that loads the tabular data into postgres db.
# refer back to week 6 assessment

# def load_to_db(
#         df:pd.DataFrame,
#         table_name:str):
#     pass


# if __name__ == "__main__":
#     # call the functions
#     # to sort -> ?sort=desc&limit=5

#     for table in endpoints:
#         print(f"==> Extracting {table}")
#         file_location = extract_and_load_to_file_storage(table)
#         sleep(1)
#         print(f"==> Transforming {table}")
#         transform_api_data(file_location)
#         print(f"Transformation of {table} done...")


#  build a transformer that takes our json extracts and makes it ready for tabular storage







# Go to Airflow for Orchestration


# AWS
# Web Scraping
# Capstone Project
# How to set up Apache Airflow for development
