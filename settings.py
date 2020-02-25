import os
from distutils.util import strtobool
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


ildb_user = os.environ.get("ildb_user", "foo")
ildb_password = os.environ.get("ildb_password", "bar")
ildb_port = os.environ.get("ildb_port", 1433)
ildb_host = os.environ.get("ildb_host", "localhost")
es_host = os.environ.get("es_host", "vpc-dev-elasticsearch-6njgchnnn3kml3qbyhrp52g37m.eu-west-2.es.amazonaws.com")
es_port = os.environ.get("es_port", 443)
es_resolver_index = os.environ.get("es_resolver_index", "path-resolver-taxonomy")
flask_local = bool(strtobool(str(os.environ.get("flask_local", False))))
