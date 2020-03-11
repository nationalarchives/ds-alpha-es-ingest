"""
Take a structured record and just return a blob of text for entity extraction
"""
import json


def flatten(d, sep="_"):
    import collections

    obj = collections.OrderedDict()

    def recurse(t, parent_key=""):

        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    recurse(d)

    return obj


sample = {
    "letter_code": "FO",
    "division_no": 1,
    "class_no": 371,
    "subclass_no": None,
    "class_hdr_no": 492,
    "subheader_no": None,
    "piece_ref": "85613",
    "first_date": "1950-01-01",
    "last_date": "1950-12-31",
    "title": "German Iron and Steel industry: minutes of meetings of the Combined Steel Group; production and allocation. Code CE file 34 (papers 1 to 413)",
    "series": "371",
    "path": {
        "Department": "FO",
        "Division": 1,
        "Series": "371",
        "Subseries": 492,
        "Subsubseries": None,
        "Piece": "85613",
        "Item": None,
    },
    "level": "Piece",
    "catalogue_ref": "FO 371/85613",
    "id": "FO:~1:371:~492:85613",
    "matches": [
        "FO:~1:371:~492:85613",
        "FO/371/85613",
        "FO/~1/371/~492/85613",
        "FO:371:85613",
        "FO 371/85613",
    ],
    "first_date_obj": {"year": 1950, "month": 1, "day": 1, "century": 19},
    "last_date_obj": {"year": 1950, "month": 12, "day": 31, "century": 19},
    "eras": ["postwar"],
    "research_guides": {
        "Object": [],
        "Department": [6, 23, 29, 35, 42, 71, 93, 102, 107, 112, 118, 151, 158, 267, 270],
        "Series": [23, 33, 35, 78, 104, 112, 120, 165, 182, 183, 226, 253],
        "All": [
            6,
            23,
            29,
            33,
            35,
            42,
            71,
            78,
            93,
            102,
            104,
            107,
            112,
            118,
            120,
            151,
            158,
            165,
            182,
            183,
            226,
            253,
            267,
            270,
        ],
    },
    "mongo": {
        "browse_parent_iaid": "C7685",
        "covering_from_date": 19500101,
        "covering_to_date": 19501231,
        "catalogue_id": 3594141,
        "closure": {"closure_code": "30", "closure_status": "O", "closure_type": "N"},
        "covering_dates": "1950",
        "former_reference_dept": "File 34 (papers 1 to 413)",
        "held_by": [
            {
                "archon_no": "66",
                "archon_iaid": "A13530124",
                "corporate_body_name": "The National Archives, Kew",
            }
        ],
        "iaid": "C2843092",
        "legal_status": "Public Record(s)",
        "catalogue_level_id": 6,
        "parent_iaid": "C58286",
        "pref": "85613",
        "reference": "FO 371/85613",
        "scope_and_content": {
            "description": "<scopecontent><p>German Iron and Steel industry: minutes of meetings of the Combined Steel Group; production and allocation. Code CE file 34 (papers 1 to 413)</p></scopecontent>"
        },
        "sort_key": "06 0 785443000 0#85443000",
    },
}


def prune(input_dict):
    flattened = flatten(input_dict)
    keys = ["first_date", "last_date", ]


print(json.dumps(flatten(d=sample), indent=2))

