import requests
import json
from slugify import slugify
from copy import deepcopy
import logging
from nlp import flatten_to_string, string_to_entities
from iteration_utilities import grouper
from es_docs_mongo import make_canonical


logger = logging.getLogger("waitress")
logger.setLevel(logging.DEBUG)


with open("staticfiles/mongo_mappings.json") as f:
    mongo_map = json.load(f)


def get_mongo(obj_list, spacy_nlp=None):
    """
    decorate the object from the initial ILDB harvest and make_canonical process with Mongo data.

    If there is an iaid use that for the iaid

    :param obj_list:
    :param spacy_nlp: optional
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
    # if we have data, filter it to just things that have data and
    # which match an id in the list from ILDB
    if mongo:
        mongo_filtered = [
            list(filter(lambda mongo_o: mongo_o["id"] == o["id"], mongo))[0] for o in obj_list
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
            if spacy_nlp:
                e = string_to_entities(input_string=flatten_to_string(obj), nlp=spacy_nlp)
                if e:
                    obj.update(e)
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
                logging.error(
                    "This is a no mappings error when trying to identify the mapped key"
                    " for a dict key/value pairß."
                )
                try:
                    logging.error("I am trying to get the key from master map")
                    new_key = slugify(mongo_map[k]["label"]).replace("-", "_")
                except:
                    logging.error("I FAILED to get key from master map")
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
                        new_dict[new_key] = [mongo_recurse(x, mappings[k]["nested"]) for x in v]
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
        return {x["id"]: mongo_recurse(x["iadata"], mappings=mongo_map) for x in mong_data}
    else:
        return []


def map_mongo(mong_data=None):
    if mong_data:
        return {x["id"]: mongo_recurse(x["iadata"], mappings=mongo_map) for x in mong_data}
    else:
        print("No data received from Mongo")
        return []


def reverse_mong(letter_code, division, series, piece, level="Item", max_id_range=260000):
    """
    Generator that yields ids in 200 id chunks suitable for passing to Kentigern

    :param letter_code
    :param division
    :param series
    :param piece
    :param level
    :param max_id_range:
    :return:
    """
    base_id = f"{letter_code}:~{division}:{series}:{piece}"
    items = [
        {
            "id": base_id + ":" + str(i),
            "level": level,
            "letter_code": letter_code,
            "class_no": series,
            "series": str(series),
            "division_no": str(division),
            "item_ref": str(i),
            "catalogue_ref": f"{letter_code} {series}/{piece}/{i}",
            "piece_ref": str(piece),

        }
        for i in range(1, max_id_range)
    ]
    for group in grouper(items, 200, fillvalue=None):
        yield [g for g in group if g is not None]


def iterate_reverse_mong(rev, nlp_proc=None):
    """
    Iterate a list of ids that have been provided by the reverse_mong function (that just generates some IDs)
    fetching the records from mongo via Kentigern and decorating with NLP.

    :param rev:
    :param nlp_proc: optional spacy NLP model
    :return:
    """
    for item_list in rev:
        mongos = [m for m in get_mongo(obj_list=item_list, spacy_nlp=nlp_proc) if m.get("mongo")]
        if mongos:
            yield mongos
        else:
            break


if __name__ == "__main__":
    import spacy

    nlp = spacy.load("en_core_web_sm")
    count = 0
    for x in iterate_reverse_mong(reverse_mong(
        letter_code="WO",
        division=16,
        series=372,
        piece=1,
        level="Item"
    ), nlp_proc=None):
        print(x[0])
        print(make_canonical(x[0]))
