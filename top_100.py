import csv
from elasticsearch import Elasticsearch
import certifi
from elasticsearch.exceptions import NotFoundError
from collections import OrderedDict
from typing import List, Dict
from elasticsearch.helpers import parallel_bulk
import requests
import urllib.request


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
            # print(f"Key: {key}")
            simplified_item = dict(
                description=v["description"], document_format=v.get("document_format")
            )
            if key:
                imagelibrequest = requests.get(
                    f"https://alpha.nationalarchives.gov.uk/image-library/catref/{key}"
                )
                if imagelibrequest.status_code == requests.codes.ok:
                    simplified_item["image_library"] = imagelibrequest.json()
                replicarequest = requests.get(
                    f"https://alpha.nationalarchives.gov.uk/replica/catref/{key}"
                )
                if replicarequest.status_code == requests.codes.ok:
                    # print(f"Got replica")
                    simplified_item["replica"] = replicarequest.json()
                    image_files = simplified_item["replica"].get("Files")
                    if image_files:
                        image_file = image_files[0].get("Name")
                        if image_file:
                            simplified_item["iiif_component"] = image_file
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


def fetch_es_record(key, item, es_, index="path-resolver-mongo", image_path_base=None):
    """

    :param key:
    :param item:
    :param es_:
    :param index
    :param image_path_base: the location of the ds-alpha-analytics-service repo
    :return:
    """
    resolver_dict = get_matches(es_=es_, es_index=index, path=key)
    try:
        c = resolver_dict["canonical"][0]
        doc = c
        if doc.get("iaid"):
            if item.get("iiif_component"):
                doc[
                    "iiif_full"
                ] = f"https://ctest-discovery.nationalarchives.gov.uk/image/{doc['iaid']}/" f"{item['iiif_component'].replace('66/','')}/full/full/0/default.jpg"
                doc[
                    "iiif_medium"
                ] = f"https://ctest-discovery.nationalarchives.gov.uk/image/{doc['iaid']}/" f"{item['iiif_component'].replace('66/','')}/full/256,/0/default.jpg"
                doc[
                    "iiif_thumb"
                ] = f"https://ctest-discovery.nationalarchives.gov.uk/image/{doc['iaid']}/" f"{item['iiif_component'].replace('66/','')}/full/155,/0/default.jpg"
        doc["top_items"] = [item]
        doc["medium_thumbs"] = []
        doc["small_thumbs"] = []
        if item:
            if item.get("image_library"):
                if item["image_library"].get("records"):
                    for record, record_item in item["image_library"]["records"].items():
                        for image in record_item["images"]:
                            doc["medium_thumbs"].append(
                                f"https://nationalarchives.github.io/"
                                f"ds-alpha-analytics-service/"
                                f"medium/{image['ImageURL'].split('?id=')[1]}.jpg"
                            )
                            doc["small_thumbs"].append(
                                f"https://nationalarchives.github.io/"
                                f"ds-alpha-analytics-service/"
                                f"thumbs/{image['ImageURL'].split('?id=')[1]}.jpg"
                            )
                else:
                    for image in item["image_library"]["images"]:
                        doc["medium_thumbs"].append(
                            f"https://nationalarchives.github.io/"
                            f"ds-alpha-analytics-service/"
                            f"medium/{image['ImageURL'].split('?id=')[1]}.jpg"
                        )
                        doc["small_thumbs"].append(
                            f"https://nationalarchives.github.io/"
                            f"ds-alpha-analytics-service/"
                            f"thumbs/{image['ImageURL'].split('?id=')[1]}.jpg"
                        )
        if doc["medium_thumbs"]:
            verified_thumbs = []
            for m in doc["medium_thumbs"]:
                i_req = requests.get(m)
                if i_req.status_code == requests.codes.ok:
                    verified_thumbs.append(m)
            doc["medium_thumbs"] = verified_thumbs
        if doc["small_thumbs"]:
            verified_thumbs = []
            for m in doc["small_thumbs"]:
                i_req = requests.get(m)
                if i_req.status_code == requests.codes.ok:
                    verified_thumbs.append(m)
            doc["small_thumbs"] = verified_thumbs
        if doc.get("iiif_thumb") and not doc["small_thumbs"]:
            print(f"Doc has IIIF but no static image in analytics")
            print(f"{doc['iiif_thumb']}")
            image_id = doc["iiif_thumb"].split("/")[-5]
            if image_path_base:
                # s_image_path = (
                #     f"/Users/matt.mcgrattan/Documents/Github/ds-alpha-analytics-service/"
                #     + f"docs/thumbs/{image_id}"
                # )
                s_image_path = "".join([image_path_base,
                                        f"docs/thumbs/{image_id}"])
                # m_image_path = (
                #     f"/Users/matt.mcgrattan/Documents/Github/ds-alpha-analytics-service/"
                #     + f"docs/medium/{image_id}"
                # )
                m_image_path = "".join([image_path_base,
                                        f"docs/medium/{image_id}"])
                urllib.request.urlretrieve(doc["iiif_thumb"], s_image_path)
                urllib.request.urlretrieve(doc["iiif_medium"], m_image_path)
                doc["small_thumbs"].append(
                    f"https://nationalarchives.github.io/"
                    f"ds-alpha-analytics-service/"
                    f"thumbs/{image_id}"
                )
                doc["medium_thumbs"].append(
                    f"https://nationalarchives.github.io/"
                    f"ds-alpha-analytics-service/"
                    f"medium/{image_id}"
                )
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
    """
    This code is very much hacky one-time code.
    
    It will run against the top100 list (json stored in the staticdata service), and attempt to
    update those records in Elasticsearch.
    
    It also verifies thumbnails, and will fetch the images that don't exist and put them in a repo for
    upload later.
    
    N.B. this should only be done once.
    """
    import json

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
    # Get the elasticrecords for the items identified as top100 items via the staticdata
    # Enrich in the process
    highlights = [fetch_es_record(*i, es, image_path_base=None) for i in identify_tops()]
    # filter out the empty records
    chunked_highlights = [n for n in highlights if n]
    # create a pruned version which was used to dump to file in diagnosing the original ingest
    pruned_highlights = [
        {"mongo": x["doc"]["mongo"], "top_items": x["doc"]["top_items"]} for x in chunked_highlights
    ]
    with open("staticfiles/top100_chunked.json", "w") as f:
        json.dump(chunked_highlights, f, indent=2)
    # If this is uncommented, it will push the enriched data back into Elastic
    print("Putting the stuff into Elastic")
    p_bulk(es_=es, iterator=chunked_highlights, index_="path-resolver-mongo", verbose=False)
