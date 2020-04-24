# README

## Introduction

This repository holds the code that is used to ingest data from:

1) ILDB
2) Mongo
3) Various static data exports:
    * stored locally as part of the repo
    * provided via network access via the /staticdata/ service on Alpha.
    * images stored in https://github.com/nationalarchives/ds-alpha-analytics-service and made available via Github pages
    
And push this data to Elasticsearch for reuse by the prototypes on Alpha.

This code is _not_ production ready, and was never intended to be so. Instead, the code has accrued throughout the lifetime of the \
project. So:

* data generation is inefficient, for example:
    * sometimes the same data is looped over multiple times while it is being enriched
    * sometimes data generated at an earlier part of the process is not reused but is generated again in a slightly different form
* there are a lot of potential performance and efficiency gains that could be produced through refactoring
* code is synchronous, rather than asynchronous, so processes that involve network overheads or processing lags are handled less efficiently than if the service spawned a load of tasks as futures and gathered the results when needed
* some of the static data is sharded into quite large files, which makes it hard to run the process on a host with low RAM or CPU
* some validation of data happens, largely to ensure that processes do not break entirely during ingest, but:
    * there is no validation against a defined schema or data model
    * there is no attempt to efficiently handle data and only process changes rather than just bulk replace everything
    

With that said, the code does quite a lot with the data sources and enriches it beyond the current data present in Discovery.
There are also some attempts to handle scale:

1) The code processes requests in chunks, and uses lazily evaluated iterators/generators against:
    * IDLB
    * Mongo (via the Kentigern service)
  so the overall size of the dataset doesn't pose any major problems and it can run over several days.
2) The code attempts to parse and normalise dates (see: date_handling.py) which makes timelines, date histograms, and search APIs easier to work with.
3) The code uses Spacy.io to run NLP named entity recognition against the metadata to produce lists of:
    * People
    * Places
    * Organisations
    * Dates
4) The code uses the subject codes from the taxonomy lists (sorted as gzipped compressed files in taxonomy_datafiles)
5) Incorporates special handling for:
    * Chancery records
    * Top 100 records
    * Highlight records (for 2D Nav, from data by Helen and Hari)
    * Records that don't exist in ILDB but do exist in Mongo (WO medal cards, specifically)

There is also a webservice which can be left running, and which can be used to trigger an ingest via a GET request. 

However, we didn't use this web service in any long-running ingests as the size of data (especially the taxonomy static files) makes it prone to falling over.

## Basic structure of the codebase and the logic for standard document handling

The "master" functions are all in __es_docs.py__.

The logic is as follows:

1) Instantiate connections to:
    * ILDB (using pyodbc and FreeTDS)
    * Elasticsearch (using the Python Elasticsearch library)
2) Fetch records from ILDB in chunks of 1000
3) Zip together the field labels with the data to create a Python dictionary for each row in the table (a list of 1000 of these)
4) Convert each of these dictionaries into an enriched format (also a dict) using the _make_canonical_ function. (as a list comprehension on the list of 1000)
    * Generate the correct series label (to deal with series that have subclasses, e.g. CP 25/2)
    * Create a "path" object which contains the correct Department, Division, Series, Subseries, Subsubseries, Piece, and Item for the object
    * Identify which level, e.g. "piece" this object is and store as a simple key for lookup
    * generate a properly formatted catalogue reference (there is a _construct_cat_ref_ function for this)
    * generate a list of all possible _valid_ identifiers this object might match and store as a "matches" key (_generate_keys_ function)
    * generate a list of possible matches or partial matches that might match this object via URL hacking (_make_frags_ function)
    * call the _gen_date_ function (from data_handling.py) to create normalised start and end dates, and date objects with century, year, month, day.
    * identify which _era_ this object falls in (using the eras from the Education website, via the _identify_eras_ function)
    * identify which research guides this document is associated with (_identify_guides_ function from get_guides.py)
    * identify which taxonomy terms are associated with this reference (using the _taxonomy_data_ which is loaded from sharded gzip files)
5) Retrieve the Mongo data for this list of 1000 rows by calling _kentigern_ via an HTTP request. N.B. Kentigern works asynchronously and can handle the request for 1000 simultaneous records quickly.
    * _get_mongo_: Accept a list of objects (produced as a list of objects per row produced using _make_canonical_), request the Mongo data from kentigern using an HTTP POST request.
    * use _map_mongo_ function to replace the abbreviated field names with human readable field names
    * if a spacy_nlp instance is available:
        * flatten the data to a string suitable for named entity extraction
        * run that string through named entity extract _string_to_entities_ (nlp.py)
        * if the record is a Chancery record (lettercode C):
            * extract Short Title, Plaintiff, Defendants from that description with assistance from Spacy NLP
    * return the list of Objects back to _es_docs_ for further processing (step 6 below) 
6) Convert this list of dicts (one per row) into Elasticsearch documents suitable for ingest into the ES index
7) Index these into Elasticsearch using the parallel_bulk API provided by Elasticsearch (this is done in smaller chunks so as not to exceed the transport size allowed by Elastic's HTTP endpoint)

Note that the process is driven from the cursor retrieval of records from ILDB. Only objects that have records in ILDB are processed this way.

The ingest handles, on a reasonably provisioned machine, somewhere in the region of 30-40 records per second. This includes:

* requests to ILDB
* requests to Kentigern
* named entity extraction and data normalisation
* ingest into Elasticsearch via HTTP transport

This is relatively fast considering what is being done, but as per above, this could be considerably improved via production code that:

1) Targetted a specific data model
2) Efficiently parallelised tasks
3) Was written from the ground up for performance and reliability

At 30-40 records per second that will still take 5 days to process the entirety of tNA's holdings. This was only done a few times throughout Alpha,
so the top100 and medal card ingests were done as later processes, rather than doing them "in-line" during processing.

## Running at scale for ingest

* es_docs
* medal cards
* top 100
* highlights





## Local testing

Build the docker image.

```bash
docker build -t ingest .
```

Run the image (e.g.on Mac).

```bash
docker run -p 8000:8000 -e ildb_host=host.docker.internal -e ildb_user=USER -e ildb_password=PASSWORD -e es_host=host.docker.internal -e flask_local=True ingest
```

Make a request:

[http://localhost:8000/ingest?start=A&end=C](http://localhost:8000/ingest?start=A&end=C)

If you want to push data (rather than just testing the basic connections) add the `action=True` parameter, e.g.

[http://localhost:8000/ingest?lettercode=ADM&action=True](http://localhost:8000/ingest?lettercode=ADM&action=True)



'Driver=FreeTDS;Server=host.docker.internal;Database=ILDB;UID=digirati;PWD=jaffaCAKES9;'





