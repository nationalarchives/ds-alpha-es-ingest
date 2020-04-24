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

# Ingesting Data

## Records from ILDB

See below for instructions on setting up network connections.

1. Update settings.py if required
    * change the ildb user and ildb password to the appropriate user name and password for the instance of ILDB on the Alpha AWS estate
    * change the es_port to whatever Elasticsearch is available at in your environment (see below)
    * change the es_index to the index in use (currently the "production" index on Alpha is `path-resolver-mongo`)
2. Open es_docs.py
    * set the `start` and `end` in the `process_data` lines at the end of the file to the lettercode you want to start with and the lettercode you want to end with.
    * alternatively, set `lettercode` to a specific lettercode if you just want to ingest one department.
    * if these are left as _None_ the system will start with lettercode A and run to the end (this will take several days)
    * set `ingest` to `True`.
3. Run `python es_docs.py` and the ingest will begin.

In the intial ingest, I tended to run in 40-50 lettercodes at a time, and then check them.

If you leave this at this point the services will mostly work, but:

* there will be no medal card records
* the top 100 will not be updated with images and flagged as top 100
* the highlight items will not be flagged as highlights and updated with images.


## Medal cards

## Top 100

## Highlights




## Network connections and local running/testing

You can set up a Python 3.7 or 3.8 virtual environment, e.g. 

1. Ensure you have Python 3.7+ and pip installed
2. Clone this repository
3. Create a virtual environment with `python3 -m venv venv`
4. From the root directory run `source venv/bin/activate`
5. Install dependencies with `pip install -r requirements.txt`

The code expects to have access to ILDB and Elasticsearch running on the Alpha AWS cluster. If you are running locally,
this can be handled by SSH tunnelling via the Alpha bastion service.


### Elasticsearch

For example, to tunnel Elastic to port `9201` on hte local machine.

```bash
ssh -N -L 9201:vpc-dev-elasticsearch-6njgchnnn3kml3qbyhrp52gm.eu-west-2.es.amazonaws.com:443 ec2-user@ec2-3-10-202-210.eu-west-2.compute.amazonaws.com -i ~/.ssh/alpha-bastion.pem
```

Add `vpc-dev-elasticsearch-6njgchnnn3kml3qbyhrp52gm.eu-west-2.es.amazonaws.com` to `/etc/hosts` to, if you want to
allow certificate verification.

e.g.

```.env
# Host Database
#
# localhost is used to configure the loopback interface
# when the system is booting.  Do not change this entry.
##
127.0.0.1	localhost
127.0.0.1	vpc-dev-elasticsearch-6njgchnnn3kml3qbyhrp52g37m.eu-west-2.es.amazonaws.com
```

### ILDB

```bash
ssh -N -L 1433:10.50.98.102:1433 ec2-user@ec2-3-10-202-210.eu-west-2.compute.amazonaws.com -i ~/.ssh/alpha-bastion.pem
```

Access to ILDB will need FreeTDS.

For example, on Ubuntu, you could install the freeTDS driver:

```
sudo apt-get install freetds-dev freetds-bin unixodbc-dev tdsodbc
```

You will then need to create/edit `/etc/odbcinst.ini`:

```
[FreeTDS]
Description=FreeTDS Driver
Driver=/usr/lib/odbc/libtdsodbc.so
Setup=/usr/lib/odbc/libtdsS.so
```

This will vary depending on OS, for example in Ubuntu 16.04 64 bit, this looks like:

```
[FreeTDS]
Description=FreeTDS Driver
Driver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
Setup=/usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
```

#### OS X

See: [https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Mac-OSX](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Mac-OSX)





