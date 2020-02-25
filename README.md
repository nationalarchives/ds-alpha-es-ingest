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




