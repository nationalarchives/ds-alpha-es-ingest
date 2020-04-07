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
        "mongo.administrative_background",
        "mongo.arrangement",
        "mongo.custodial_history"
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
    input_string: str, nlp, ent_types=("DATE", "GPE", "ORG", "FAC", "LOC", "PERSON"),
        medal_card=False
):
    """

    :param input_string:
    :param nlp: spacy model
    :param ent_types: filter to just these entity types
    :param medal_card: if True, ignore persons.
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
                        try:
                            if is_int(
                                entity
                            ):  # Handle cases where this is a year only, to avoid insertion of today
                                d = dateparser.parse(entity).year
                            else:
                                if str(entity[0]) == "-":
                                    entity = str(entity[1:])
                                d = dateparser.parse(entity)
                        except ValueError or IndexError:
                            d = None
                        if d:
                            try:
                                end_year = dateparser.parse(
                                    entity,
                                    settings={
                                        "RELATIVE_BASE": datetime.datetime(2020, 12, 31)
                                    },
                                )
                            except ValueError:
                                end_year = None
                            try:
                                start_year = dateparser.parse(
                                    entity,
                                    settings={
                                        "RELATIVE_BASE": datetime.datetime(2020, 1, 1)
                                    },
                                )
                            except ValueError:
                                start_year = None
                            if start_year and end_year:
                                date_ents.append(
                                    {
                                        "text": entity,
                                        "date": f"{d}",
                                        "label": "DATE",
                                        "year_start": start_year,
                                        "year_end": end_year,
                                    }
                                )
                            else:
                                date_ents.append({"text": entity, "label": "DATE"})
                        else:  # Date parser couldn't identify the date, but we know it is one.
                            date_ents.append({"text": entity, "label": "DATE"})
                elif ent.label_ == "PERSON":
                    try:
                        variants = names.name_initials(
                            name=ent.text,
                            name_formats=["firstnamelastname", "lastnamefirstname"],
                        )
                        sorted_v = sorted(variants)
                    except IndexError or KeyError or ValueError:
                        sorted_v = None
                    name_ents.append(
                        {
                            "text": ent.text,
                            "label": ent.label_,
                            "variants": sorted_v,
                        }
                    )
                else:  # Just iterate the entities
                    matches = [e["text"] for e in ents + date_ents + name_ents]
                    if ent.text not in matches:
                        ents.append({"text": ent.text, "label": ent.label_})
        if medal_card:
            master_list = date_ents
        else:
            master_list = ents + name_ents + date_ents
        return {
            "entity_list": master_list,
            "entities_by_type": entity_list_to_dict(master_list),
        }
    return

