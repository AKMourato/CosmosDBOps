"""
Utilities for database operations.
"""

import os
import math
import datetime
import json
from typing import Union, List
import pprint
import pymongo
from bson.json_util import loads


def calculate_bmi(
    weight: Union[int, float], height: Union[int, float]
) -> Union[float, None]:
    """
    Function to calculate the Body Mass Index (BMI) based on weight in kg and height in meters.
    """

    if weight is None or height is None:
        return None
    bmi = round(weight / math.pow(height / 100, 2), 2)
    if math.isnan(bmi):
        return None
    return bmi


def calculate_mosteller_bsa(
    weight: Union[int, float], height: Union[int, float]
) -> Union[float, None]:
    """
    Function to calculate the Mosteller Body Surface Area (BSA)[m2]
    based on weight in kg and height in centimeters.
    """

    if weight is None or height is None:
        return None
    bsa = round(math.sqrt(weight * height / 3600), 2)
    if math.isnan(bsa):
        return None
    return bsa


def get_current_datetime() -> datetime.datetime:
    """
    Function to get the current UTC datetime.
    """
    return datetime.datetime.utcnow()


def _add_date_time(doc: dict) -> None:
    """
    Function to add the current UTC datetime to a document.
    """
    if "datetime_creation" not in doc.keys():
        doc["datetime_creation"] = get_current_datetime()


def _add_documents_to_collection(filename: str, collection: str) -> None:
    """
    Function to insert json files into a MongoDB collection.
    """

    with open(filename, encoding="utf-8") as file:
        file_data = json.load(file)

    file_data = loads(json.dumps(file_data))
    if isinstance(file_data, list):
        for document in file_data:
            _add_date_time(document)
        collection.insert_many(file_data)
    else:
        _add_date_time(file_data)
        collection.insert_one(file_data)


def add_test_data_to_db(database: pymongo.database.Database, collections: list) -> None:
    """
    Function to add the test json files to a MongoDB database.
    """

    root_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", ".."))
    test_data_dir = os.path.join(root_dir, "tests", "data", "initial_inmemory_data")

    for collection in collections:
        try:
            json_file = os.path.join(test_data_dir, "test_" + collection + ".json")
            _add_documents_to_collection(json_file, database[collection])
        except FileNotFoundError:
            pass


def json_printer(obj: Union[dict, list]):
    """
    Function to print jsons nicely.
    """
    to_print = pprint.PrettyPrinter(indent=1, width=40, compact=True)
    to_print.pprint(obj)


def fcsv2list(path_to_fcsv: str) -> List[float]:
    """
    Function to get coordinates from fcsv file
    into a list.
    """
    landmarks = []
    with open(path_to_fcsv, "r", encoding="utf-8") as f:
        for x in f:
            if x[0].isdigit():
                x_splitted = x.split(",")
                landmarks.append([float(i) for i in x_splitted[1:4]])
    return landmarks
