FROM python:3.8-alpine
#FROM jamiehewland/alpine-pypy:latest


RUN apk add python3-dev build-base linux-headers pcre-dev uwsgi-python unixodbc unixodbc-dev freetds freetds-dev ca-certificates

ENV ildb_user=foo
ENV ildb_password=bar
ENV ildb_host=localhost
ENV ildb_port=1433
ENV es_resolver_index=path-resolver-taxonomy
ENV es_port=9201
ENV es_host=localhost
ENV use_es=True
ENV flask_local=False

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

#CMD gunicorn --log-level debug -k gevent -t 1000 --graceful-timeout 1000 --keep-alive 300 --threads 20 -w 4 -b 0.0.0.0:8000 app:app
CMD waitress-serve --port=8000 --channel-timeout=6000 app:app
#CMD pypy3 app.py