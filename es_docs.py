from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from elasticsearch.exceptions import NotFoundError
import pyodbc
import json
from copy import deepcopy
import requests
from ildb_queries import (
    piece_query,
    series_query,
    subseries_query,
    subsubseries_query,
    lettercodes_query,
    item_query,
    division_query,
)
from date_handling import gen_date, identify_eras, parse_eras
from highlight_data import get_highlights
from guides import load_guide_data, identify_guides
from settings import (
    ildb_host,
    ildb_password,
    ildb_port,
    ildb_user,
    es_resolver_index,
    es_port,
    es_host,
)
import logging
import certifi
from collections import OrderedDict
from typing import Dict, Optional, Union, List, Tuple
import time
import requests
import gzip


es_logger = logging.getLogger("")
es_logger.setLevel(logging.DEBUG)

# Create globals
# Default to getting these from the staticdata service, but if not, make/load them locally.

eras = parse_eras()

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


with gzip.open("taxonomy_datafiles/taxonomy_ab.json.gz", "rb") as f:
    taxonomy_data = json.loads(f.read())


def construct_cat_ref(path: Optional[Dict] = None) -> Optional[str]:
    """
    Construct a catalogue reference from the path component in the Elasticsearch document.

     "path": {
        "Department": "CP",
        "Division": 1,
        "Item": null,
        "Piece": "25/155/2HENVIIIEASTER",
        "Series": "25/2",
        "Subseries": 18,
        "Subsubseries": null
    },

    cat_ref: CP 25/2/25/155/2HENVIIIEASTER


    :param path:
    :return:
    """
    if path:
        # Get the parts of the path that are visible in the catalogue reference
        path_components_ = [path.get("Series"), path.get("Piece"), path.get("Item")]
        path_components = [x for x in path_components_ if x]
        # Join these together in the appropriate way, DEPT A/B/C/D/E etc.
        cat_ref = " ".join([path.get("Department"), "/".join(path_components)]).strip()
        return cat_ref
    else:
        return


def make_frags(
    row: Dict, levels_to_process: Tuple[str] = ("Series", "Piece", "Item")
) -> Optional[List[str]]:
    """
    Generate the fragmentary paths that can be created by URL hacking from an object identifier path

    :param row: dict from the make_canonical function
    :param levels_to_process: tuple of object levels to parse
    :return:
    """
    level_lookup = {
        "Country": -1,
        "Archon": 0,
        "Department": 1,
        "Division": 2,
        "Series": 3,
        "Subseries": 4,
        "Subsubseries": 5,
        "Piece": 6,
        "Item": 7,
    }
    lvl = row.get("level")
    if lvl in levels_to_process:  # Levels in the archive that may have slashes in their identifier
        if row["path"].get(lvl):
            path_item = row["path"][lvl]
            if "/" in path_item:
                # Split the path on the slash character into a list
                # i.e. A/B/C/D/E
                # becomes ["A", "B", "C", "D", "E"]
                slashed = path_item.split("/")
                keys = []
                # iterate the list of split path componenents
                # i.e. for A/B/C/D/E
                # Generate (on each successive iteration in the loop)
                #   A/B
                #   A/B/C
                #   A/B/C/D
                #   A/B/C/D/E
                for x in range(1, len(slashed) + 1):
                    # Construct the partial path
                    frag = "/".join(slashed[0:x])
                    # Create a copy of the "path" dict, and then replace the Series, Piece, or Item
                    # with the fragmentary form.
                    temp_frag_path = deepcopy(row["path"])
                    temp_frag_path[lvl] = frag
                    # Get the keys that match this new "path" and add to the list
                    _, k = generate_keys(path=temp_frag_path, level=level_lookup[lvl])
                    keys += k
                return keys  # Return the list of keys that this object might match via URL hacking
    return


def generate_keys(
    path: Dict, level: int, separators: Tuple[str] = ("/", ":"), id_separator: str = ":"
) -> Tuple[str, List[str]]:
    """

    :param path: path dictionary to process
    :param level: level in the archival hierarchy
    :param separators: tuple of separators to use to join the keys
    :param id_separator: separator to use to generate the canonical id for the document
    :return:
    """
    # Lookup to identify whether a level has a tilde prefix
    lookup = {
        "Country": {"level": -1, "prefix": ""},
        "Archon": {"level": 0, "prefix": ""},
        "Department": {"level": 1, "prefix": ""},
        "Division": {"level": 2, "prefix": "~"},
        "Series": {"level": 3, "prefix": ""},
        "Subseries": {"level": 4, "prefix": "~"},
        "Subsubseries": {"level": 5, "prefix": "~"},
        "Piece": {"level": 6, "prefix": ""},
        "Item": {"level": 7, "prefix": ""},
    }
    all_parts = {}
    visible_parts = {}
    for k, v in path.items():  # Iterate the path dict
        # Check the level and prefix for a given key in that dict
        lookup_value = lookup.get(k)
        if lookup_value:
            if v is not None:  # If the level in the path is not Null
                level = lookup_value.get("level")
                prefix = lookup_value.get("prefix")
                l_str = "".join([prefix, str(v)])  # Construct the string, e.g. ~1 for a Divsion
                all_parts[level] = l_str  # Add the string to the complete list of parts
                if (
                    prefix != "~"
                ):  # Add just those levels that are part of the cat ref to the visible list of parts
                    visible_parts[level] = l_str
    document_id = id_separator.join([str(v) for _, v in sorted(all_parts.items())])
    keys = []
    if level in [1, 3, 6, 7]:
        # Is this a visible level in the hierarchy? If so, it has a catalogue reference, so this should be in the keys
        for s in separators:  # Usually slash and colon, but we could add others if required
            keys.append(construct_cat_ref(path=path))
            keys += [
                s.join([str(v) for _, v in sorted(visible_parts.items())]),
                s.join([str(v) for _, v in sorted(all_parts.items())]),
            ]
    else:
        # This is a Divsion, Subseries, or subsubseries, so it has no catalogue reference, so just add the whole
        # path, and not the cat ref (which will generate many false matches otherwise).
        for s in separators:
            keys += [s.join([str(v) for _, v in sorted(all_parts.items())])]
    return document_id, list(set(keys))  # list of keys that this object should match


def make_canonical(row_dict: Dict) -> Dict:
    """
    Generate a simple representation of an object
    for later reuse.

    :param row_dict:
    :return:
    """
    # 1. Generate the human readable series name
    # Handle cases where there may be a series with a slash, e.g. CP 25/2
    if row_dict.get("subclass_no") and row_dict.get("class_no"):
        series = "/".join([str(row_dict.get("class_no")), str(row_dict.get("subclass_no"))])
    else:
        if row_dict.get("class_no"):
            series = str(row_dict.get("class_no"))
        else:
            series = None
    row_dict["series"] = series
    # 2. Generate a dictionary of levels using user friendly names for the levels
    # This is ordered, so the lowest level is returned last.
    row_dict["path"] = OrderedDict(
        [
            ("Department", row_dict.get("letter_code")),
            ("Division", row_dict.get("division_no")),
            ("Series", series),
            ("Subseries", row_dict.get("class_hdr_no")),
            ("Subsubseries", row_dict.get("subheader_no")),
            ("Piece", row_dict.get("piece_ref")),
            ("Item", row_dict.get("item_ref")),
        ]
    )
    # 3. Work out what level THIS object is at in the hierarchy.
    row_dict["level"] = [k for k, v in row_dict["path"].items() if v][-1]
    # 4. Generate the catalogue reference
    # Check for cases in which the level is not one that has a catalogue reference
    if row_dict["level"] in ("Division", "Subseries", "Subsubseries"):
        row_dict["catalogue_ref"] = None
    else:  # otherise, populate the catalogue reference field
        row_dict["catalogue_ref"] = construct_cat_ref(path=row_dict["path"])
    # 5. Generate all of the possible TRUE identifiers that THIS document will match
    # Add the various paths as a list of keys in the document object
    row_dict["id"], row_dict["matches"] = generate_keys(
        path=row_dict["path"], level=row_dict["level"]
    )
    # 6. Construct the list of keys tht MIGHT match or partially match this object, when generated via URL hacking, e.g.
    # when a piece_ref with a slash, or an item_ref with a slash, is chopped up.
    also_matches = make_frags(row=row_dict)
    if also_matches:
        # Add the unique list of matches.
        # Don't add the canonical identifiers to this list.
        row_dict["also_matches"] = list(
            set([x for x in also_matches if x not in row_dict["matches"]])
        )
    # 7. Create an Elasticsearch readable data, and an object with year, month, day, etc for the start date for this
    if row_dict.get("first_date"):
        row_dict["first_date"], row_dict["first_date_obj"] = gen_date(
            str(row_dict["first_date"]), row_dict["id"], row_dict["catalogue_ref"]
        )
    # 8. Create an Elasticsearch readable data, and an object with year, month, day, etc for the end date for this
    if row_dict.get("last_date"):
        row_dict["last_date"], row_dict["last_date_obj"] = gen_date(
            str(row_dict["last_date"]), row_dict["id"], row_dict["catalogue_ref"]
        )
    # 9. Identify which era(s) this particular object falls within
    row_dict["eras"] = identify_eras(
        era_dict=eras,
        item_start=row_dict.get("first_date_obj"),
        item_end=row_dict.get("last_date_obj"),
    )
    # 10. Identify which research guides are associated with this object
    row_dict["research_guides"] = identify_guides(
        row_dict["catalogue_ref"], row_dict["path"], guides, integer_map
    )
    # 11. Identify which taxonomy terms are associated with this object
    # if row_dict.get("catalogue_ref"):
    #     t = taxonomy_data.get(row_dict["catalogue_ref"])
    #     if t:
    #         row_dict["iaid"] = t.get("iaid")
    #         row_dict["subjects"] = t.get("taxonomy_ids")
    return row_dict


def ingest_list(item_list: List, index: str = "test-index") -> Dict:
    """
    Generator to yield ES compatible dicts that can be used by the ES bulk APIs.

    ? What should be used as the document ID? In this case, I'm just B64 encoding the last identifier in the matches
    Which will generally be the Foo:~1:bar form. That's just a place holder. We should probably use whatever
    the most commonly used identifier in our API calls is going to be.


    :param item_list: input list to parse
    :param index: ES index to use
    :return: dict
    """
    for doc in item_list:
        yield {
            "_op_type": "update",
            "_index": index,
            # "_type": "resolver",
            "_id": doc["id"],
            "doc": doc,
            "doc_as_upsert": True,
            "retry_on_conflict": 5,
        }


def p_bulk(es_, index_: str, iterator, chunk: int = 200, verbose: bool = True):
    """
    Simple wrapper around the ES parallel bulk API

    The chunk size should be kept fairly small otherwise the amount of data being sent over the
    HTTP(S) transport is likely to cause ES to throw errors.

    :param es_: ES connection
    :param index_: ES index to use
    :param iterator: Iterator which should yield docs
    :param chunk: chunk size to use
    :param verbose: boolean, if True, print every update status not just failures.
    :return:
    """
    for success, info in parallel_bulk(
        client=es_,
        actions=iterator,
        raise_on_error=True,
        raise_on_exception=True,
        index=index_,
        chunk_size=chunk,
        request_timeout=1000,
    ):
        if not success:
            es_logger.error(f"Doc failed: {info}")
        else:
            if verbose:
                es_logger.debug(f"Doc OK: {info}")
    return


def get_path(es_, es_index, path, d_type="resolver"):
    """
    Quick test to check what gets returned from a Query
    :param es_:
    :param es_index:
    :param path:
    :param d_type:
    :return:
    """
    try:
        return es_.get(index=es_index, id=path, doc_type=d_type, request_timeout=30)
    except NotFoundError:
        return


def get_matches(es_, es_index, path):
    """
    Quick test to check what gets returned from a Query


    :param es_:
    :param es_index:
    :param path:
    :return:
    """
    es_logger.debug(f"I am querying ES for {path}")
    q = {
        "query": {"bool": {"must": [{"term": {"matches.keyword": path}}]}},
        "_source": {
            "exclude": ["matches", "also_matches"]
        },  # We may actually want to return the keys.
    }
    q2 = {
        "query": {"bool": {"must": [{"term": {"also_matches.keyword": path}}]}},
        "sort": [{"id.keyword": "asc"}],
        "_source": {
            "exclude": ["matches", "also_matches"]
        },  # We may actually want to return the keys.
    }
    es_logger.debug(f"Passing query JSON: {q} and {q2}")
    result = OrderedDict()
    try:
        canonical_search = es_.search(index=es_index, body=q, request_timeout=30)
        also_search = es_.search(index=es_index, body=q2, request_timeout=30)
        if canonical_search["hits"].get("hits"):
            result["canonical"] = [x["_source"] for x in canonical_search["hits"]["hits"]]
        if also_search["hits"].get("hits"):
            result["also_matches"] = [x["_source"] for x in also_search["hits"]["hits"]]
        return result
    except NotFoundError:
        return


def cursor_get(database_connection, query_string, chunk_size=1000):
    """
    Iterate through a DB connection cursor adding to a list.

    This is where the documents that are ingested into Elasticsearch are generated.

    :param database_connection:
    :param query_string:
    :param chunk_size: how big should the cursor into MS SQL be?
    :return:
    """
    if database_connection and query_string:
        crsr = database_connection.cursor()
        crsr.execute(query_string)
        while True:
            row = crsr.fetchmany(chunk_size)  # The data from ILDB
            columns = [column[0] for column in crsr.description]  # The column names from ILDB
            rows = [
                make_canonical(dict(zip(columns, r))) for r in row
            ]  # Make a dict and then parse the dict for reuse
            if not row:
                break
            yield rows
        crsr.close()
    return True


def es_iterator(elastic, elastic_index, level, cursor_output, verbosity, ingest):
    """
    Iterate the list of parsed (make_canonical({})) cursor output from ILDB and use
    Elastic search's parallel bulk ingest to push into ES

    :param elastic:
    :param elastic_index:
    :param level: level being indexed (for logging)
    :param cursor_output:
    :param verbosity:
    :param ingest:
    :return:
    """
    es_logger.info(f"Bulk ingesting the canonical identifiers, level {level}")
    if ingest:
        for c in cursor_output:
            p_bulk(
                es_=elastic,
                index_=elastic_index,
                iterator=ingest_list(item_list=c, index=elastic_index),
                verbose=verbosity,
            )


def process_data(
    elastic,
    elastic_index="test-index",
    start=None,
    end=None,
    database_connection=False,
    lettercode=None,
    verbosity=None,
    ingest=False,
):
    """
    Wrapper function to process departments into ES.

    :param elastic: ES connection
    :param elastic_index: Index to use
    :param start: Integer. Start point in list
    :param end: Integer. End point in the list
    :param database_connection: connection to ILDB
    :param verbosity: pass to the bulk func
    :param lettercode: single lettercode to pass in, to just ingest this lettercode
    :param ingest: boolean, if True, push into ES
    :return:
    """
    es_index_settings = {
        "settings": {"index": {"refresh_interval": "-1"}},
        "mappings": {
            "_source": {"enabled": "true"},
            "properties": {
                "also_matches": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "highlight": {"type": "boolean"},
                "catalogue_ref": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "title": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "first_date": {"type": "date"},
                "last_date": {"type": "date"},
                "class_hdr_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
                "class_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
                "division_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
                "id": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                        "tree": {"type": "text", "analyzer": "custom_path_tree"},
                        "tree_reversed": {"type": "text", "analyzer": "custom_path_tree_reversed"},
                    },
                },
                "item_ref": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "letter_code": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "level": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "matches": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "eras": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "first_date_obj": {
                    "properties": {
                        "century": {"type": "short"},
                        "day": {"type": "short"},
                        "month": {"type": "short"},
                        "year": {"type": "short"},
                    }
                },
                "last_date_obj": {
                    "properties": {
                        "century": {"type": "short"},
                        "day": {"type": "short"},
                        "month": {"type": "short"},
                        "year": {"type": "short"},
                    }
                },
                "research_guides": {
                    "properties": {
                        "All": {"type": "short"},
                        "Department": {"type": "short"},
                        "Object": {"type": "short"},
                        "Series": {"type": "short"},
                    }
                },
                "path": {
                    "properties": {
                        "Department": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                                "raw": {"type": "keyword", "index": "true"},
                            },
                        },
                        "Division": {
                            "type": "long",
                            "fields": {"raw": {"type": "keyword", "index": "true"}},
                        },
                        "Item": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                                "raw": {"type": "keyword", "index": "true"},
                            },
                        },
                        "Piece": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                                "raw": {"type": "keyword", "index": "true"},
                            },
                        },
                        "Series": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                                "raw": {"type": "keyword", "index": "true"},
                            },
                        },
                        "Subseries": {
                            "type": "long",
                            "fields": {"raw": {"type": "keyword", "index": "true"}},
                        },
                        "Subsubseries": {
                            "type": "long",
                            "fields": {"raw": {"type": "keyword", "index": "true"}},
                        },
                    }
                },
                "piece_ref": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "series": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "subclass_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
                "subheader_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
            },
        },
    }
    es_create_settings = {
        "settings": {
            "index": {"refresh_interval": "-1"},
            "analysis": {
                "analyzer": {
                    "custom_path_tree": {"tokenizer": "custom_hierarchy"},
                    "custom_path_tree_reversed": {"tokenizer": "custom_hierarchy_reversed"},
                },
                "tokenizer": {
                    "custom_hierarchy": {"type": "path_hierarchy", "delimiter": ":"},
                    "custom_hierarchy_reversed": {
                        "type": "path_hierarchy",
                        "delimiter": ":",
                        "reverse": "true",
                    },
                },
            },
        },
        "mappings": {
            "_source": {"enabled": "true"},
            "properties": {
                "also_matches": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "highlight": {"type": "boolean"},
                "catalogue_ref": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "title": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "eras": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "first_date": {"type": "date"},
                "last_date": {"type": "date"},
                "class_hdr_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
                "class_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
                "division_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
                "id": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                        "tree": {"type": "text", "analyzer": "custom_path_tree"},
                        "tree_reversed": {"type": "text", "analyzer": "custom_path_tree_reversed"},
                    },
                },
                "item_ref": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "letter_code": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "level": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "matches": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "first_date_obj": {
                    "properties": {
                        "century": {"type": "short"},
                        "day": {"type": "short"},
                        "month": {"type": "short"},
                        "year": {"type": "short"},
                    }
                },
                "last_date_obj": {
                    "properties": {
                        "century": {"type": "short"},
                        "day": {"type": "short"},
                        "month": {"type": "short"},
                        "year": {"type": "short"},
                    }
                },
                "research_guides": {
                    "properties": {
                        "All": {"type": "short"},
                        "Department": {"type": "short"},
                        "Object": {"type": "short"},
                        "Series": {"type": "short"},
                    }
                },
                "path": {
                    "properties": {
                        "Department": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                                "raw": {"type": "keyword", "index": "true"},
                            },
                        },
                        "Division": {
                            "type": "long",
                            "fields": {"raw": {"type": "keyword", "index": "true"}},
                        },
                        "Item": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                                "raw": {"type": "keyword", "index": "true"},
                            },
                        },
                        "Piece": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                                "raw": {"type": "keyword", "index": "true"},
                            },
                        },
                        "Series": {
                            "type": "text",
                            "fields": {
                                "keyword": {"type": "keyword", "ignore_above": 256},
                                "raw": {"type": "keyword", "index": "true"},
                            },
                        },
                        "Subseries": {
                            "type": "long",
                            "fields": {"raw": {"type": "keyword", "index": "true"}},
                        },
                        "Subsubseries": {
                            "type": "long",
                            "fields": {"raw": {"type": "keyword", "index": "true"}},
                        },
                    }
                },
                "piece_ref": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "series": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256},
                        "raw": {"type": "keyword", "index": "true"},
                    },
                },
                "subclass_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
                "subheader_no": {
                    "type": "long",
                    "fields": {"raw": {"type": "keyword", "index": "true"}},
                },
            },
        },
    }

    es_index_done_settings = {"settings": {"index": {"refresh_interval": "10s"}}}
    # 1. Get Elasticsearch ready to go
    if ingest:
        if not elastic.ping:  # Can't connect to Elasticsearch
            return False
        # If the index isn't here, make it
        if not elastic.indices.exists(index=elastic_index):
            elastic.indices.create(index=elastic_index, body=es_create_settings)
        else:
            # Get the index ready for a bulk ingest, by turning off indexing
            elastic.indices.put_settings(index=elastic_index, body=es_index_settings)
    # 2. Check I can actually connect to Elasticsearch
    if not elastic.ping:  # Can't connect to Elasticsearch
        return False
    # 3. Set up the database connection cursor
    crsr = database_connection.cursor()
    # 4. Get the lettercodes to iterate over
    crsr.execute(lettercodes_query())
    lettercodes_ = []
    lettercodes_tuples = []
    while True:
        row = crsr.fetchmany(100)
        if not row:
            break
        for r in row:
            lettercodes_.append(r[0])
            lettercodes_tuples.append(r)
    crsr.close()
    lettercodes_tuples = sorted(lettercodes_tuples, key=lambda x: x[0])
    lettercodes_list = [lett[0] for lett in lettercodes_tuples]
    # 5. Identify WHICH lettercodes are to be ingested
    if lettercode:  # If a lettercode is passed in as a parameter, just ingest this.
        working_lettercodes = [lett for lett in lettercodes_tuples if lett[0] == lettercode]
    else:
        if start and end:  # If a range, e.g. "AB" to "C" is passed in, ingest this range of lettercodes
            working_lettercodes = lettercodes_tuples[lettercodes_list.index(start): lettercodes_list.index(end) + 1]
        else:  # Just ingest everything
            working_lettercodes = lettercodes_tuples
    print(f"Working lettercodes: {working_lettercodes}")
    # 6. Iterate the set of lettercodes to be ingested
    for lettercode, lettercode_title in working_lettercodes:
        #  7. Load the sharded taxonomy file and load into a global variable for reuse
        global taxonomy_data
        shard = "".join([x for x in lettercode[0:2] if x.isalpha()]).lower()
        with gzip.open(f"taxonomy_datafiles/taxonomy_{shard}.json.gz", "rb") as taxonomy_file:
            taxonomy_data = json.loads(taxonomy_file.read())
        # 8. Dogfood the IDResolver stats api to identify how big this lettercode is
        stats_request = requests.get(
            f"https://alpha.nationalarchives.gov.uk/idresolver/stats/" f"{lettercode}"
        )
        # 9. Set how long to wait between iterations based on the size of the lettercode, 1 sec = 100,000 items
        if stats_request.status_code == requests.codes.ok:
            stats = stats_request.json()
            total = sum([int(v) for k, v in stats.items()])
            sleep_time = int(total / 100000)
            print(f"Sleep: {sleep_time}")
        else:
            sleep_time = 15
        es_logger.info(f"Running ILDB queries for: {lettercode}")
        # 10. Begin iterating each level in the hierarchy
        pieces_canonical = cursor_get(
            database_connection=database_connection, query_string=piece_query(lettercode=lettercode)
        )
        es_iterator(
            elastic=elastic,
            elastic_index=elastic_index,
            level=6,
            cursor_output=pieces_canonical,
            verbosity=verbosity,
            ingest=ingest,
        )
        if ingest:
            elastic.indices.put_settings(index=elastic_index, body=es_index_done_settings)
            time.sleep(sleep_time)
            elastic.indices.put_settings(index=elastic_index, body=es_index_settings)
        division_canonical = cursor_get(
            database_connection=database_connection,
            query_string=division_query(lettercode=lettercode),
        )
        es_iterator(
            elastic=elastic,
            elastic_index=elastic_index,
            level=2,
            cursor_output=division_canonical,
            verbosity=verbosity,
            ingest=ingest,
        )
        if ingest:
            elastic.indices.put_settings(index=elastic_index, body=es_index_done_settings)
            elastic.indices.put_settings(index=elastic_index, body=es_index_settings)
        subseries_canonical = cursor_get(
            database_connection=database_connection,
            query_string=subseries_query(lettercode=lettercode),
        )
        es_iterator(
            elastic=elastic,
            elastic_index=elastic_index,
            level=4,
            cursor_output=subseries_canonical,
            verbosity=verbosity,
            ingest=ingest,
        )
        if ingest:
            elastic.indices.put_settings(index=elastic_index, body=es_index_done_settings)
            elastic.indices.put_settings(index=elastic_index, body=es_index_settings)
        subsubseries_canonical = cursor_get(
            database_connection=database_connection,
            query_string=subsubseries_query(lettercode=lettercode),
        )
        es_iterator(
            elastic=elastic,
            elastic_index=elastic_index,
            level=5,
            cursor_output=subsubseries_canonical,
            verbosity=verbosity,
            ingest=ingest,
        )
        if ingest:
            elastic.indices.put_settings(index=elastic_index, body=es_index_done_settings)
            elastic.indices.put_settings(index=elastic_index, body=es_index_settings)
        items_canonical = cursor_get(
            database_connection=database_connection, query_string=item_query(lettercode=lettercode)
        )
        es_iterator(
            elastic=elastic,
            elastic_index=elastic_index,
            level=7,
            cursor_output=items_canonical,
            verbosity=verbosity,
            ingest=ingest,
        )
        if ingest:
            elastic.indices.put_settings(index=elastic_index, body=es_index_done_settings)
            time.sleep(sleep_time)
            elastic.indices.put_settings(index=elastic_index, body=es_index_settings)
        series_canonical = cursor_get(
            database_connection=database_connection,
            query_string=series_query(lettercode=lettercode),
        )
        es_iterator(
            elastic=elastic,
            elastic_index=elastic_index,
            level=3,
            cursor_output=series_canonical,
            verbosity=verbosity,
            ingest=ingest,
        )
        if ingest:
            elastic.indices.put_settings(index=elastic_index, body=es_index_done_settings)
            time.sleep(sleep_time)
            elastic.indices.put_settings(index=elastic_index, body=es_index_settings)
        lettercodes_canonical = [
            [make_canonical({"letter_code": lettercode, "title": lettercode_title})]
        ]
        es_iterator(
            elastic=elastic,
            elastic_index=elastic_index,
            level=1,
            cursor_output=lettercodes_canonical,
            verbosity=verbosity,
            ingest=ingest,
        )
        es_logger.info("Done with indexing.")
        if ingest:
            elastic.indices.put_settings(index=elastic_index, body=es_index_done_settings)
    if ingest:
        elastic.indices.put_settings(index=elastic_index, body=es_index_done_settings)
    return True


if __name__ == "__main__":
    es_logger = logging.getLogger("")
    es_logger.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # add formatter to ch
    ch.setFormatter(formatter)
    es_logger.addHandler(ch)
    # Connect to ILDB
    ildb_connection = pyodbc.connect(
        server=ildb_host,
        database="ILDB",
        user=ildb_user,
        tds_version="7.4",
        password=ildb_password,
        port=ildb_port,
        driver="FreeTDS",
    )
    # Connect to ES
    es = Elasticsearch(
        hosts=[
            {
                "host": es_host,
                "use_ssl": True,
                "verify_certs": True,
                "port": es_port,
                "ca_certs": certifi.where(),
            }
        ]
    )
    print(f"{es}")
    print("Processing data")
    process_data(
        elastic=es,
        elastic_index=es_resolver_index,
        lettercode=None,
        start=None,
        end=None,
        database_connection=ildb_connection,
        verbosity=True,
        ingest=False,
    )
    # print(json.dumps(get_matches(es_=es, es_index="new-resolver", path="AVIA 14/2"), indent=2))
