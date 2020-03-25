from es_docs import make_canonical


def convert_mongo(mongo_):
    """
    Convert a record that has come back from Kentigern with potential NLP data added into the right document
    format for ingest into Elastic.

    Primarily, convert the mongo fields to replicate the ILDB generated fields that are missing.

    1) matches
    2) also_matches
    3) eras
    4) first and last date and their related objects
    5) all of the ILDB related fields: class_hdr_no, class_no, division_no, letter_code, piece_ref, item_ref,
        subclass_no, subheader_no
    6) subjects
    7) research guides
    8) path


    :param mongo_:
    :return:
    """
    return make_canonical(mongo_)
