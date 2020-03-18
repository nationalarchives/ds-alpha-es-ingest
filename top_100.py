import csv
from elasticsearch import Elasticsearch
import certifi
from elasticsearch.exceptions import NotFoundError
from collections import OrderedDict
from typing import List, Dict
from elasticsearch.helpers import parallel_bulk
import requests


def get_matches(es_, es_index, path):
    """
    Quick test to check what gets returned from a Query


    :param es_:
    :param es_index:
    :param path:
    :return:
    """
    q = {
        "track_total_hits": True,
        "query": {"bool": {"must": [{"term": {"matches.keyword": path}}]}},

    }
    q2 = {
        "track_total_hits": True,
        "query": {"bool": {"must": [{"term": {"also_matches.keyword": path}}]}},
        "sort": [{"id.keyword": "asc"}],
    }
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


def get_highlights(catalogue_reference):
    """
    return the highlight data from the CSV
    :param catalogue_reference: catalogue reference to query for.
    :return:
    """
    if catalogue_reference:
        pass
    return


def identify_tops():
    input_data = None
    r = requests.get("https://alpha.nationalarchives.gov.uk/staticdata/top100.json")
    if r.status_code == requests.codes.ok:
        input_data = r.json()
    if input_data:
        for k, v in input_data.items():
            key = v["cat_ref"]
            simplified_item = dict(description=v["description"],
                                   document_format=v.get("document_format"))
            yield key, simplified_item


def ingest_list(item_list: List, index: str = "test-index") -> Dict:
    """
    Generator to yield ES compatible dicts that can be used by the ES bulk APIs.

    ? What should be used as the document ID? In this case, I'm just B64 encoding the last \
    identifier in the matches Which will generally be the Foo:~1:bar form. That's just a place \
    holder. We should probably use whatever the most commonly used identifier in our API calls \
    is going to be.

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


def fetch_es_record(key, item, es_, index="path-resolver-taxonomy"):
    """

    :param key:
    :param item:
    :param es_:
    :return:
    """
    resolver_dict = get_matches(es_=es_, es_index=index, path=key)
    try:
        c = resolver_dict["canonical"][0]
        doc = c
        doc["top_items"] = [item]
        doc["top_item"] = True
        ident_ = doc["id"]
        es_doc = {
            "_op_type": "update",
            "_index": index,
            "_id": ident_,
            "doc": doc,
            "doc_as_upsert": True,
            "retry_on_conflict": 5,
        }
        return es_doc
    except KeyError:
        print(f"Key Error: {key}")
        pass


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
            print(f"Doc failed: {info}")
        else:
            if verbose:
                print(f"Doc OK: {info}")
    return


if __name__ == "__main__":
    es = Elasticsearch(
        hosts=[
            {
                "host": "vpc-dev-elasticsearch-6njgchnnn3kml3qbyhrp52g37m.eu-west-2.es.amazonaws.com",
                "use_ssl": True,
                "verify_certs": True,
                "port": 9201,
                "ca_certs": certifi.where(),
            }
        ]
    )
    import json
    #
    highlights = [fetch_es_record(*i, es) for i in identify_tops()]
    chunked_highlights = [n for n in highlights if n]
    print(len(chunked_highlights))
    # print(json.dumps(chunked_highlights, indent=2))
    # p_bulk(es_=es, iterator=chunked_highlights, index_="path-resolver-taxonomy", verbose=False)


