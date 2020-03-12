import spacy
from geotext import GeoText
import dateparser
from flashtext import KeywordProcessor
from collections import defaultdict
import datetime
from operator import itemgetter
from personalnames import names
from bs4 import BeautifulSoup
from dictor import dictor


def is_int(val):
    try:
        num = int(val)
    except ValueError:
        return False
    return True


def flatten_to_string(input_obj):
    """
    Flatten a document post Mongo enrichment to produce a nice simple string that can be used
    to extract entities.

    :param input_obj:
    :return:
    """
    source_keys = [
        "mongo.covering_dates",
        "mongo.creators.corporate_body_name",
        "mongo.note",
        "mono.physical_description_form",
        "mongo.scope_and_content.description",
        "mongo.related_material.description",
        "mongo.separated_material.description",
        "mongo.title",
        "title",
        "description",
        "mongo.former_reference_dept",
    ]
    flat = " ".join([x for x in [dictor(input_obj, k) for k in source_keys] if x])
    return flat


def entity_list_to_dict(entity_list):
    """
    Convert a list of entities into something that is a lookup by entity type
    :param entity_list:
    :return:
    """
    lookup = defaultdict(list)
    for entity in entity_list:
        lookup[entity["label"]].append(entity)
    return lookup


def string_to_entities(
    input_string: str, nlp, ent_types=("DATE", "GPE", "ORG", "FAC", "LOC", "PERSON")
):
    """

    :param input_string:
    :param nlp: spacy model
    :param ent_types: filter to just these entity types
    :return:
    """
    soup = BeautifulSoup(input_string, features="html.parser")
    text = soup.get_text()
    if text and nlp:
        doc = nlp(text)
        places = GeoText(text)
        geo_ents = []
        date_ents = []
        name_ents = []
        ents = []
        for c in places.cities:
            geo_ents.append({"text": c, "label": "GPE"})
        for c in places.country_mentions:
            geo_ents.append({"text": c, "label": "GPE"})
            ents.append({"text": c, "label": "GPE"})
        # Use flash text to get the bounds for the non-Spacy entities
        # Can also be used later to decorate with, e.g. the orgnames from Mongo
        for ent in doc.ents:
            if ent.label_ in ent_types:
                if ent.label_ == "DATE":
                    # Little bit of a hack to handle date ranges
                    if "-" in ent.text:  # We something that looks like a date range
                        split_ents = [x.strip() for x in ent.text.split("-")]
                    else:  # Or we don't
                        split_ents = [ent.text]
                    for entity in split_ents:
                        if is_int(
                            entity
                        ):  # Handle cases where this is a year only, to avoid insertion of today
                            d = dateparser.parse(entity).year
                        else:
                            d = dateparser.parse(entity)
                        if d:
                            end_year = dateparser.parse(
                                entity,
                                settings={
                                    "RELATIVE_BASE": datetime.datetime(2020, 12, 31)
                                },
                            )
                            start_year = dateparser.parse(
                                entity,
                                settings={
                                    "RELATIVE_BASE": datetime.datetime(2020, 1, 1)
                                },
                            )
                            date_ents.append(
                                {
                                    "text": entity,
                                    "date": f"{d}",
                                    "label": "DATE",
                                    "year_start": start_year,
                                    "year_end": end_year,
                                }
                            )
                        else:  # Date parser couldn't identify the date, but we know it is one.
                            date_ents.append({"text": entity, "label": "DATE"})
                elif ent.label_ == "PERSON":
                    variants = names.name_initials(
                        name=ent.text,
                        name_formats=["firstnamelastname", "lastnamefirstname"],
                    )
                    name_ents.append(
                        {
                            "text": ent.text,
                            "label": ent.label_,
                            "variants": sorted(variants),
                        }
                    )
                else:  # Just iterate the entities
                    matches = [e["text"] for e in ents + date_ents + name_ents]
                    if ent.text not in matches:
                        ents.append({"text": ent.text, "label": ent.label_})
        master_list = ents + name_ents + date_ents
        return {
            "entity_list": master_list,
            "entities_by_type": entity_list_to_dict(master_list),
        }
    return


doc = {
    "letter_code": "FO",
    "division_no": 1,
    "class_no": 371,
    "subclass_no": None,
    "class_hdr_no": 492,
    "subheader_no": None,
    "piece_ref": "85611",
    "first_date": "1950-01-01",
    "last_date": "1950-12-31",
    "title": "Fusion Agreement: US-USA-French trizonal arrangements: drain on Germany's sterling; trizone drawing rights; OEEC Joint Trade and Intra-European Payments Committee. Code CE file 24 (papers 739 - 3444)",
    "series": "371",
    "path": {
        "Department": "FO",
        "Division": 1,
        "Series": "371",
        "Subseries": 492,
        "Subsubseries": None,
        "Piece": "85611",
        "Item": None,
    },
    "level": "Piece",
    "catalogue_ref": "FO 371/85611",
    "id": "FO:~1:371:~492:85611",
    "matches": [
        "FO 371/85611",
        "FO:~1:371:~492:85611",
        "FO/371/85611",
        "FO/~1/371/~492/85611",
        "FO:371:85611",
    ],
    "first_date_obj": {"year": 1950, "month": 1, "day": 1, "century": 19},
    "last_date_obj": {"year": 1950, "month": 12, "day": 31, "century": 19},
    "eras": ["postwar"],
    "research_guides": {
        "Object": [],
        "Department": [
            6,
            23,
            29,
            35,
            42,
            71,
            93,
            102,
            107,
            112,
            118,
            151,
            158,
            267,
            270,
        ],
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
        "catalogue_id": 3594139,
        "closure": {"closure_code": "30", "closure_status": "O", "closure_type": "N"},
        "covering_dates": "1950",
        "former_reference_dept": "File 24 (papers 739-3444)",
        "held_by": [
            {
                "archon_no": "66",
                "archon_iaid": "A13530124",
                "corporate_body_name": "The National Archives, Kew",
            }
        ],
        "iaid": "C2843090",
        "legal_status": "Public Record(s)",
        "catalogue_level_id": 6,
        "parent_iaid": "C58286",
        "pref": "85611",
        "reference": "FO 371/85611",
        "scope_and_content": {
            "description": "<scopecontent><p>Fusion Agreement: US-USA-French trizonal arrangements: drain on Germany's sterling; trizone drawing rights; OEEC Joint Trade and Intra-European Payments Committee. Code CE file 24 (papers 739 - 3444)</p></scopecontent>"
        },
        "sort_key": "06 0 785441000 0#85441000",
    },
    "iaid": "C2843090",
}


print(
    string_to_entities(
        input_string=flatten_to_string(input_obj=doc), nlp=spacy.load("en_core_web_sm")
    )
)
