import requests
import json
from slugify import slugify
from copy import deepcopy
import logging


logger = logging.getLogger("waitress")
logger.setLevel(logging.DEBUG)


with open("staticfiles/mongo_mappings.json") as f:
    mongo_map = json.load(f)


def get_mongo(obj_list):
    """
    decorate the object from the initial ILDB harvest and make_canonical process with Mongo data.

    If there is an iaid use that for the iaid

    :param obj_list:
    :return:
    """
    ids = [{"id": obj["id"], "level": obj["level"]} for obj in obj_list]
    # Run a request to the Mongo service (kentigern) to get the Mongo data for that list of ids.
    mongo_data = requests.post(
        url="https://alpha.nationalarchives.gov.uk/kentigern/gather", json=ids
    )
    if mongo_data.status_code == requests.codes.ok:
        mongo = mongo_data.json()
    else:
        mongo = None
    # if we have data, filter it to just things that have data and which match an id in the list from ILDB
    if mongo:
        mongo_filtered = [
            list(filter(lambda mongo_o: mongo_o["id"] == o["id"], mongo))[0]
            for o in obj_list
        ]
    else:
        mongo_filtered = None
    # Map the mongo data to have the right field names rather than the abbreviated/cryptic form
    # used in Mongo
    if mongo_filtered:
        mongo_ = map_mongo(mong_data=mongo_filtered)
    else:
        mongo_ = None
    # Create a copy of the list (do we need to do this?) and decorate with reformatted mongo data.
    if mongo_:
        new_obj_list = deepcopy(obj_list)
        for obj in new_obj_list:
            obj["mongo"] = mongo_.get(obj["id"])
            if obj["mongo"]:
                obj["iaid"] = obj["mongo"]["iaid"]
        return new_obj_list
    else:
        return obj_list


def mongo_recurse(mongo_dict, mappings):
    new_dict = {}
    if isinstance(mongo_dict, dict):
        for k, v in mongo_dict.items():
            if mappings:
                if mappings.get(k):
                    try:
                        new_key = slugify(mappings[k]["label"]).replace("-", "_")
                    except TypeError:
                        new_key = slugify(mappings[k]).replace("-", "_")
                else:
                    new_key = slugify(k).replace("-", "_")
            else:
                logging.error("This is a no mappings error when trying to identify the mapped key"
                              " for a dict key/value pairß.")
                new_key = slugify(k).replace("-", "_")
                logging.error(f"Existing key: {k}")
                logging.error(f"Key: {new_key}")
                logging.error(f"Value list: {v}")
                logging.error(f"Mongo dict: {mongo_dict}")
            if isinstance(v, dict):
                new_dict[new_key] = mongo_recurse(v, mappings[k]["nested"])
            elif isinstance(v, list):
                try:
                    # hack catch for when the val is just a list of values
                    if all([isinstance(val, (str, int)) for val in v]):
                        new_dict[new_key] = v
                    else:
                        new_dict[new_key] = [
                            mongo_recurse(x, mappings[k]["nested"]) for x in v
                        ]
                except TypeError:
                    logging.error("This is a type error when parsing a list of values.")
                    logging.error(f"Existing key: {k}")
                    logging.error(f"Key: {new_key}")
                    logging.error(f"Value list: {v}")
                    logging.error(f"Mappings: {mappings[k]}")
                    logging.error(f"Mongo dict: {mongo_dict}")
            else:
                new_dict[new_key] = v
    elif isinstance(mongo_dict, list):
        return [mongo_recurse(x, mappings=mappings) for x in mongo_dict]
    else:
        return mongo_dict
    return new_dict


def map_mongo_test():
    with open("staticfiles/mongo_mappings.json") as f:
        mongo_map = json.load(f)
    mong = requests.get("https://alpha.nationalarchives.gov.uk/kentigern/test")
    if mong.status_code == requests.codes.ok:
        mong_data = json.loads(mong.content)
    else:
        mong_data = None
    if mong_data:
        return {
            x["id"]: mongo_recurse(x["iadata"], mappings=mongo_map) for x in mong_data
        }
    else:
        return []


def map_mongo(mong_data=None):
    if mong_data:
        return {
            x["id"]: mongo_recurse(x["iadata"], mappings=mongo_map) for x in mong_data
        }
    else:
        print("No data received from Mongo")
        return []
