FROM python:3.8-slim

RUN apt-get update && apt-get install -y python3-dev \
python3-numpy \
 python3-pandas \
  uwsgi-plugin-python3 \
  build-essential \
  chrpath \
  libssl-dev \
  libxft-dev \
  libfreetype6 \
  libfreetype6-dev \
  libfontconfig1 \
  libfontconfig1-dev \
  openssl

ENV ildb_user=foo
ENV ildb_password=bar
ENV ildb_host=localhost
ENV ildb_port=1433
ENV es_resolver_index=path-resolver-mongo
ENV es_port=9201
ENV es_host=localhost
ENV use_es=True
ENV flask_local=False
ENV es_update=True

EXPOSE 8000
EXPOSE 1433

WORKDIR /opt/ingest

COPY requirements.txt /opt/ingest

RUN pip install -r requirements.txt

# This ensures directory sructure is preserved but will copy everything
COPY . /opt/ingest/
COPY odbcinst.ini /etc/odbcinst.ini

RUN ls /opt/ingest

RUN printenv

CMD waitress-serve --port=8000 --channel-timeout=6000 app:app
