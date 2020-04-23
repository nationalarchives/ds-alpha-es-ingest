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

## Basic structure of the codebase

The "master" functions are all in __es_docs.py__.

The logic is as follows:

1) Instantiate connections to:
    * ILDB (using pyodbc and FreeTDS)
    * Elasticsearch (using the Python Elasticsearch library)
2) Fetch records from ILDB in chunks of 1000
3) Zip together the field labels with the data to create a Python dictionary for each row in the table (a list of 1000 of these)
4) Convert each of these dictionaries into an enriched format (also a dict) using the _make_canonical_ function. (as a list comprehension on the list of 1000)
5) Retrieve the Mongo data for this list of 1000 rows by calling _kentigern_ via an HTTP request. N.B. Kentigern works asynchronously and can handle the request for 1000 simultaneous records quickly.
6) Convert this list of dicts (one per row) into Elasticsearch documents suitable for ingest into the ES index
7) Index these into Elasticsearch using the parallel_bulk API provided by Elasticsearch (this is done in smaller chunks so as not to exceed the transport size allowed by Elastic's HTTP endpoint)



## Running at scale for ingest

* es_docs
* highlights
* reverse_mongo
* top100




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





