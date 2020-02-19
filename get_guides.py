from get_fragments import *
import json

"""
This module gets the guides related to a given reference or reference fragment 
"""


def load_guide_data():
    """
    Loads guide data from JSON file and returns corresponding dict

    :return: guides
    :rtype: dict
    """
    with open('app/data/references_in_guides_backlinked_deduped.min.json') as guides:
        guides = json.load(guides)

    return guides


def get_guides_for_lettercode(lettercode):
    """
    Gets guides for a given letter code. If none, returns empty string

    :param lettercode: a lettercode
    :type lettercode: str
    :return: guides if found, '' otherwise
    :rtype: dict or str
    """

    guides = load_guide_data()

    if lettercode:
        if lettercode in guides:
            if 'guides' in guides[lettercode]:
                return guides[lettercode]['guides']
    return ''


def get_guides_for_series(lettercode, series):
    """
    Gets guides for a given series

    :param lettercode: a lettercode
    :param series: a series
    :return: guides related to a given series, otherwise ''
    :rtype: dict or str
    """

    guides = load_guide_data()

    if lettercode in guides:
        if series in guides[lettercode]['records']:
            if 'guides' in guides[lettercode]['records'][series]:
                return guides[lettercode]['records'][series]['guides']
    return ''


def get_guides_for_reference(lettercode, series, reference):
    """
    Gets guides for a given reference
    :param lettercode: a lettercode
    :param series: a series
    :param reference: a reference
    :return: guides related to a given reference, otherwise ''
    :rtype: dict or str
    """
    guides = load_guide_data()

    if lettercode in guides:
        if series in guides[lettercode]['records']:
            if 'records' in guides[lettercode]['records'][series]:
                if reference in guides[lettercode]['records'][series]['records']:
                    if 'guides' in guides[lettercode]['records'][series]['records'][reference]:
                        return guides[lettercode]['records'][series]['records'][reference]['guides']

    return ''


def get_guides(ref):
    """
    Gets guides for the lettercode, series and reference represented in the given argument
    :param reference: a document reference
    :return: a dict of results
    :rtype: dict
    """
    fragments = get_fragments(ref)

    letter_code = fragments['letter_code']
    series = fragments['series']
    reference = fragments['reference']

    results = {
        reference: get_guides_for_reference(letter_code, series, reference),
        series: get_guides_for_series(letter_code, series),
        letter_code: get_guides_for_lettercode(letter_code)
    }

    return results
