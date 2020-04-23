import json
from collections import defaultdict
import requests


test = {
    "catalogue_ref": "AB 7",
    "class_no": 7,
    "division_no": 2,
    "first_date": "1944-01-01",
    "first_date_obj": {"century": 19, "day": 1, "month": 1, "year": 1944},
    "id": "AB:~2:7",
    "last_date": "1990-12-31",
    "last_date_obj": {"century": 19, "day": 31, "month": 12, "year": 1990},
    "letter_code": "AB",
    "level": "Series",
    "matches": ["AB/~2/7", "AB:7", "AB 7", "AB/7", "AB:~2:7"],
    "path": {
        "Department": "AB",
        "Division": 2,
        "Item": None,
        "Piece": None,
        "Series": "7",
        "Subseries": None,
        "Subsubseries": None,
    },
    "series": "7",
    "subclass_no": None,
    "title": "United Kingdom Atomic Energy Authority and predecessors: Northern Groups: Reports and Memoranda",
}


def load_guide_data():
    """
    Loads guide data from JSON file and returns corresponding dict

    :return: guides
    :rtype: dict
    """
    with open("staticfiles/flattened_guides.json", "r") as guides:
        guides = json.load(guides)
    with open("staticfiles/researchguide_map.json", "r") as f:
        integer_map = json.load(f)
    return guides, integer_map


def flatten_guides(g, g_dict=None):
    """"
    Recursively flattens the guides into a single key lookup

    :param g: the initial input
    :param g_dict: the parsed version
    """
    if not g_dict:
        g_dict = defaultdict(list)
    for k, v in g.items():
        if v.get("guides"):
            for i, t in v["guides"].items():
                g_dict[k].append({"id": i, "title": t})
        if v.get("records"):
            g_dict = flatten_guides(v["records"], g_dict)
    return g_dict


def create_integer_map():
    """
    Make a version of the guides with a simple integer key for use as a map

    :return:
    """
    with open("staticfiles/flattened_guides.json", "r") as f:
        guides = json.load(f)
    guide_list = []
    for _, x in guides.items():
        for g in x:
            guide_list.append(g)

    # all_ = [json.loads(a) for a in list(set([json.dumps(x, sort_keys=True) for x in guide_list]))]
    all_ = [dict(s) for s in set(frozenset(d.items()) for d in guide_list)]
    for i, a in enumerate(all_):
        a["key"] = i + 1
    z = range(1, len(all_) + 1)
    all_guides = dict(zip(z, [{"id": item["id"], "title": item["title"]} for item in all_]))
    with open(f"staticfiles/researchguide_map.json", "w") as rf:
        json.dump(all_guides, rf, indent=2, sort_keys=True)
    return all_guides


def identify_guides(cat_ref, path, flattened_guides, integer_map):
    """
    Identify the relevant guides for an archival object

    :param cat_ref: catalogue reference
    :param path: path lookup from the dict
    :param flattened_guides: dict with the flattened guides
    :param integer_map: integer map for the guides
    :return: dict with the guides for that object, and lettercode and series
    """

    research_guides = dict(
        Object=flattened_guides.get(cat_ref), Department=flattened_guides.get(path["Department"])
    )
    if path.get("Series"):
        s = " ".join([path["Department"], path["Series"]])
        research_guides["Series"] = flattened_guides.get(s)
    all_guides = []
    for _, d in research_guides.items():
        if d:
            all_guides.extend(d)
    all_ = [json.loads(a) for a in list(set([json.dumps(x, sort_keys=True) for x in all_guides]))]
    research_guides["All"] = all_
    new_guides = {k: [] for k, v in research_guides.items()}
    for k, v in research_guides.items():
        if v:
            for x in v:
                doc = [km for km, vm in integer_map.items() if vm["id"] == x["id"]][0]
                new_guides[k].append(int(doc))
    for k, v in new_guides.items():
        if type(v) == list and len(v) == 0:
            new_guides[k] = None
        new_guides[k] = sorted(v)
    return new_guides


def get_guidefile():
    """
    Load the data, flatten it, return it
    :return:
    """
    guides, mapped = load_guide_data()
    return guides, mapped


def invert_guides():
    g = requests.get("https://alpha.nationalarchives.gov.uk/staticdata/flattened_guides.json")
    if g.status_code == requests.codes.ok:
        guides = g.json()
    else:
        guides = None
    m = requests.get("https://alpha.nationalarchives.gov.uk/staticdata/researchguide_map.json")
    if m.status_code == requests.codes.ok:
        integer_map = m.json()
    else:
        integer_map = None
    if not guides and not integer_map:
        guides, integer_map = load_guide_data()
    # print(json.dumps(integer_map, indent=2, sort_keys=True))
    inverted = {v["id"]: {"key": int(k), "title": v["title"]} for k, v in integer_map.items()}
    with open("staticfiles/decorated_guides.json", "r") as rf:
        decorated_guides = json.load(rf)
    for k, v in inverted.items():
        for guide in decorated_guides:
            if guide["id"] == k:
                v.update(guide)
    with open("staticfiles/inverse_researchguide_map.json", "w") as f:
        json.dump(inverted, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    # t = identify_guides(test["catalogue_ref"], test["path"], *load_guide_data())
    invert_guides()
