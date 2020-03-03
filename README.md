# README

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





