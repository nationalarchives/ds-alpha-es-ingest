import glob
import json
import requests


def parse_taxonomy_files(source_dir):
    """

    :param source_dir: Directory where the JSON files are stored
    :return:
    """
    r = requests.get("https://alpha.nationalarchives.gov.uk/staticdata/taxonomy_keys.json")
    if r.status_code == requests.codes.ok:
        taxonomy_lookup = r.json()
    else:
        return
    master = {}
    files = glob.glob(f"{source_dir}/*")
    for i, file in enumerate(files):
        print(f"Working on {i} of {len(files)}")
        with open(file, "r") as f:
            j = json.load(f)
            try:
                for x in j["hits"]["hits"]:
                    cat_ref = x["_source"].get("CATALOGUE_REFERENCE")
                    if cat_ref:
                        shard = "".join([x for x in cat_ref[0:2] if x.isalpha()]).lower()
                        d = {
                            "iaid": x["_id"],
                            "taxonomy_ids": [
                                taxonomy_lookup.get(y) for y in x["_source"]["TAXONOMY_ID"]
                            ],
                        }
                        if not master.get(shard):
                            master[shard] = {}
                        master[shard][cat_ref] = d
            except KeyError:
                print(f"File with error: {file}")
    for k, v in master.items():
        with open(f"taxonomy_{k}.json".lower(), "w") as wf:
            json.dump(v, wf, indent=2)


def get_taxonomy(catalogue_reference):
    """
    return the taxonomy data and the information asset ID for a given catalogue reference
    :param catalogue_reference:
    :return:
    """
    if catalogue_reference:
        pass
    return None, None


def make_taxonomy_lookup():
    taxo = {}
    r = requests.get("https://alpha.nationalarchives.gov.uk/staticdata/taxonomy.json")
    if r.status_code == requests.codes.ok:
        j = r.json()
    for list_item in j:
        taxo[list_item["code"]] = list_item
    with open("taxonomy_keys.json", "w") as wf:
        json.dump(taxo, wf, indent=2)


if __name__ == "__main__":
    parse_taxonomy_files(source_dir="/home/ec2-user/Python-3.8.1/ds-alpha-code/ildbweb/Graph/tmp/TAXONOMY/")
    # make_taxonomy_lookup()
