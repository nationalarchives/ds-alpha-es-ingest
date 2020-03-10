import spacy
from geotext import GeoText
import dateparser
from flashtext import KeywordProcessor
from collections import defaultdict
import datetime
from operator import itemgetter
from personalnames import names
from bs4 import BeautifulSoup


def is_int(val):
    try:
        num = int(val)
    except ValueError:
        return False
    return True


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
        keyword_processor = KeywordProcessor()
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
                    print("Got a date")
                    # Little bit of a hack to handle date ranges
                    # Could probably add something in here to add the month and the day for the lower as
                    # January 1st, and for the higher as December 31st. To Do.
                    if "-" in ent.text:  # We something that looks like a date range
                        split_ents = [
                            x.strip()
                            for x in ent.text.split("-")
                        ]
                    else:  # Or we don't
                        print("Not split")
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
                                entity, settings={"RELATIVE_BASE": datetime.datetime(2020, 12, 31)}
                            )
                            start_year = dateparser.parse(
                                entity, settings={"RELATIVE_BASE": datetime.datetime(2020, 1, 1)}
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
                            date_ents.append(
                                {
                                    "text": entity,
                                    "label": "DATE",
                                }
                            )
                elif ent.label_ == "PERSON":
                    variants = names.name_initials(
                                name=ent.text,
                                name_formats=["firstnamelastname", "lastnamefirstname"],
                            )
                    name_ents.append(
                        {
                            "text": ent.text,
                            "label": ent.label_,
                            "variants": sorted(variants)
                        }
                    )
                else:  # Just iterate the entities
                    matches = [e["text"] for e in ents + date_ents + name_ents]
                    if ent.text not in matches:
                        ents.append(
                            {
                                "text": ent.text,
                                "label": ent.label_,
                            }
                        )
        master_list = ents + name_ents + date_ents
        return {
            "entity_list": master_list,
            "entities_by_type": entity_list_to_dict(master_list),
        }
    return


foo = "<scopecontent><p>Fusion Agreement: US-USA-French trizonal arrangements: drain on Germany's sterling; trizone drawing rights; OEEC Joint Trade and Intra-European Payments Committee. Code CE file 24 (papers 739 - 3444)</p></scopecontent>"

print(string_to_entities(input_string=foo, nlp=spacy.load("en_core_web_sm")
))
