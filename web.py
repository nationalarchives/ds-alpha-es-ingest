from flask import Flask, request, jsonify, abort, render_template
from flask_cors import CORS
from flask_executor import Executor
import pyodbc
import settings
from elasticsearch import Elasticsearch
import certifi


app = Flask(__name__)
CORS(app)
Executor(app)


@app.route("/")
def root_index():
    return "You shouldn't see me in the alpha environment, because I am at the root."


# Housekeeping routes
@app.route("/ping")
def ping():
    return "OK", 200


@app.route("/ingest")
def ingest():
    """
    Trigger an ingest into Elasticsearch
    :return:
    """
    lettercode = request.args.get("lettercode", default=None)
    start = request.args.get("start", default=None)
    end = request.args.get("end", default=None)
    es_index = request.args.get("es_index", default=settings.es_resolver_index)
    es_port = request.args.get("es_port", default=settings.es_port)
    es_host = request.args.get("es_host", default=settings.es_host)
    action = bool(request.args.get("action", default=None))
    settings_dict ={"lettercode": lettercode, "start": start, "end": end, "es_index": es_index, "es_port": es_port,
                    "es_host": es_host,
                    "ingest": action,
                    "settings.ildb": [settings.ildb_host,
                                      settings.ildb_port,
                                      settings.ildb_user,
                                      settings.ildb_password]}
    if settings.ildb_host and settings.ildb_user and settings.ildb_password and settings.ildb_port:
        ildb_connection = pyodbc.connect(
            server=settings.ildb_host,
            database="ILDB",
            user=settings.ildb_user,
            tds_version="7.4",
            password=settings.ildb_password,
            port=settings.ildb_port,
            driver="FreeTDS",
        )
    else:
        ildb_connection = None
    if es_index and es_port and es_host:
        es = Elasticsearch(
            hosts=[
                {
                    "host": es_host,
                    "use_ssl": True,
                    "verify_certs": True,
                    "port": es_port,
                    "ca_certs": certifi.where(),
                }
            ]
        )
    else:
        es = None
    settings_dict["ildb_connection"] = bool(ildb_connection)
    settings_dict["es_connection"] = bool(es)
    return jsonify(settings_dict)


if __name__ == "__main__":
    app.run(debug=True)