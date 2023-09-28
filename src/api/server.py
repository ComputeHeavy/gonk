# Copyright 2023 - Compute Heavy Industries Incorporated
# This work is released, distributed, and licensed under AGPLv3.

import sys
import uuid
import click
import flask
import base64
import string
import typing
import sqlite3
import hashlib
import pathlib
import secrets
import werkzeug
import traceback 
import jsonschema
import multiprocessing

from gonk.core import validators
from gonk.core import interfaces
from gonk.core import integrity
from gonk.core import events
from gonk.impl import sq3
from gonk.impl import fs

lock = multiprocessing.Lock() # TODO: lock per dataset 

root_directory = pathlib.Path("root")
datasets_directory = root_directory.joinpath("datasets")
database_path = root_directory.joinpath("gonk.db")

class RegexConverter(werkzeug.routing.BaseConverter):
    def __init__(self, url_map, *items):
        super().__init__(url_map)
        self.regex = items[0]

app = flask.Flask(__name__)

app.url_map.converters['re'] = RegexConverter

# from werkzeug.middleware.profiler import ProfilerMiddleware
# app.wsgi_app = ProfilerMiddleware(app.wsgi_app)

@click.group()
def cli():
    pass

def generate_api_key():
    bank = string.ascii_letters + string.digits
    rand = "".join([secrets.choice(bank) for ea in range(32)])
    return f"gk_{rand}"

def show_api_key(username, api_key):
    print("== THIS API KEY WILL ONLY BE SHOWN ONCE ==")
    print(f"USER: {username}")
    print(f"KEY: {api_key}")
    print()

@cli.command("init")
@click.option("--username", required=True, type=str)
@click.pass_context
def init(ctx, username):
    if not root_directory.exists():
        root_directory.mkdir()

    if not datasets_directory.exists():
        datasets_directory.mkdir()

    con = sqlite3.connect(database_path)
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        api_key_hash TEXT NOT NULL
    )""")

    con.commit()
    con.close()

    ctx.forward(user_add)

@cli.group("users")
def users():
    pass

@users.command("add")
@click.argument("username")
def user_add(username):
    if not database_path.exists():
        print("Please initialize the application with the `init` command.")
        exit(1)

    allowed = set(string.ascii_letters + string.digits + '._-')
    if len(set(username).difference(allowed)) > 0:
        print("Invalid username. [A-Za-z0-9._-]")
        exit(1)

    api_key = generate_api_key()

    con = sqlite3.connect(database_path)
    cur = con.cursor()
    cur.execute("""INSERT INTO users 
            (username, api_key_hash) 
            VALUES (?, ?)""",
        (username, hashlib.sha256(api_key.encode()).hexdigest()))

    con.commit()
    con.close()
    
    show_api_key(username, api_key)

@users.command("rekey")
@click.argument("username")
def user_rekey(username):
    if not database_path.exists():
        print("Please initialize the application with the `init` command.")
        exit(1)

    api_key = generate_api_key()

    con = sqlite3.connect(database_path)
    cur = con.cursor()
    cur.execute("""UPDATE users 
            SET api_key_hash = ? 
            WHERE username = ?""",
        (hashlib.sha256(api_key.encode()).hexdigest(), username))
    
    con.commit()
    con.close()

    show_api_key(username, api_key)

@users.command("list")
def user_list():
    if not database_path.exists():
        print("Please initialize the application with the `init` command.")
        exit(1)

    con = sqlite3.connect(database_path)
    cur = con.cursor()
    cur.execute("""SELECT id, username FROM users""")
    users = cur.fetchall()

    con.close()

    for id_, username in users:
        print(f"{id_}\t{username}")

@app.before_request
def before_request():
    flask.g.con = sqlite3.connect(database_path)

@app.teardown_request
def teardown_request(error):
    if hasattr(flask.g, "con"):
        flask.g.con.close()

def authorize(endpoint):
    def authwrap(*args, **kwargs):
        api_key = flask.request.headers.get('x-api-key')
        if not api_key:
            return flask.jsonify({"error": "Missing x-api-key header."}), 400

        api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        cur = flask.g.con.cursor()
        cur.execute("""SELECT id, username 
                FROM users
                WHERE api_key_hash = ?""", 
            (api_key_hash,))

        res = cur.fetchall()
        if len(res) != 1:
            return flask.jsonify({"error": "Invalid API key."}), 401

        user_id, username = res[0]
        flask.g.user_id = user_id
        flask.g.username = username

        return endpoint(*args, **kwargs)
    authwrap.__name__ = endpoint.__name__
    return authwrap

def accept_json(endpoint):
    def jsonwrap(*args, **kwargs):
        content_type = flask.request.headers.get('Content-Type')
        if content_type != "application/json":
            return flask.jsonify({"error": "Endpoint only accepts JSON."}), 400

        return endpoint(*args, **kwargs)
    jsonwrap.__name__ = endpoint.__name__
    return jsonwrap

@app.get("/")
@authorize
def index():
    return flask.jsonify({"message": "hello"})

class Dataset:
    def __init__(self, dataset_directory):
        self.dataset_directory = dataset_directory
        self.record_keeper = fs.RecordKeeper(dataset_directory)
        self.linker = integrity.HashChainLinker(self.record_keeper)
        self.machine = interfaces.Machine()
        self.depot = fs.Depot(dataset_directory)
        self.state = sq3.State(dataset_directory, self.record_keeper)

        self.machine.register(validators.FieldValidator())
        self.machine.register(integrity.HashChainValidator(self.record_keeper))
        self.machine.register(validators.SchemaValidator(self.depot))
        self.machine.register(self.record_keeper)
        self.machine.register(self.state)

@app.post("/datasets")
@accept_json
@authorize
def datasets_create():
    """Creates a dataset."""
    request_data = flask.request.json
    if request_data is None:
        return flask.jsonify({"error": "Request JSON is None."}), 500

    if "name" not in request_data:
        return flask.jsonify({"error": "Missing key 'name'."}), 400

    dataset_name = request_data["name"]

    if len(dataset_name) < 1:
        return flask.jsonify({"error": "Dataset name is empty."}), 400

    allowed = set(string.ascii_letters + string.digits + '-')
    if len(set(dataset_name).difference(allowed)) > 0:
        return flask.jsonify(
            {"error": "Only letters, numbers, and dashes (-) allowed."}), 400

    if dataset_name.startswith("-"):
        return flask.jsonify({"error": "Names may not start with a dash."}), 400

    dataset_directory = datasets_directory.joinpath(dataset_name)

    if dataset_directory.exists():
        return flask.jsonify({"error": "Dataset already exists."}), 400

    dataset_directory.mkdir()

    dataset = Dataset(dataset_directory)
    oae = events.OwnerAddEvent(flask.g.username)
    oae = dataset.linker.link(oae, flask.g.username)
    dataset.machine.process_event(oae)

    return flask.jsonify({
        "dataset": dataset_name,
    })

@app.get("/datasets")
@authorize
def datasets_list():
    return flask.jsonify([d.stem for d in datasets_directory.iterdir()])

@app.errorhandler(Exception)
def exception_handler(error):
    etype, exc, tb = sys.exc_info()
    traceback.print_exception(etype, exc, tb)

    ename = "Exception"
    if etype is not None:
        ename = etype.__name__

    msg = "An incommunicable error occurred."
    if exc is not None:
        if etype == jsonschema.exceptions.ValidationError:
            msg = " ".join(
                str(exc).replace("\n\n", " - ").replace("\n", " ").split())
        elif isinstance(exc, werkzeug.exceptions.HTTPException):
            msg = str(exc)
        elif len(exc.args) > 0 and type(exc.args[0]) == str:
            msg = exc.args[0]

    return flask.jsonify({"error": {ename: msg}}), 500

@app.post("/datasets/<dataset_name>/schemas")
@accept_json
@authorize
def schemas_create(dataset_name):
    request_data = flask.request.json
    if request_data is None:
        return flask.jsonify({"error": "Request JSON is None."}), 500

    if "name" not in request_data:
        return flask.jsonify({"error": "Missing key 'name'."}), 400

    if "schema" not in request_data:
        return flask.jsonify({"error": "Missing key 'schema'."}), 400

    schema_name = request_data["name"]
    schema_buf = base64.b64decode(request_data["schema"])

    if not validators.is_schema(schema_name):
        return flask.jsonify(
            {"error": "Schema names must start with 'schema-'."}), 400
    
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    sce = dataset.linker.link(
        events.ObjectCreateEvent(
            events.Object(
                schema_name, 
                "application/schema+json",
                len(schema_buf), 
                events.HashTypeT.SHA256, 
                hashlib.sha256(schema_buf).hexdigest())), 
        flask.g.username)

    with lock:
        id_ = sce.object.identifier()
        try:
            dataset.depot.reserve(id_, sce.object.size)
            dataset.depot.write(id_, 0, schema_buf)
            dataset.depot.finalize(id_)
            dataset.machine.process_event(sce)
        except Exception as e:
            if dataset.depot.exists(id_):
                dataset.depot.purge(id_)
            raise e

    schema_infos = dataset.state.schemas_all(name=schema_name)

    if len(schema_infos) != 1:
        return flask.jsonify({"error": "Schema not found."}), 404

    schema_info, = schema_infos

    return flask.jsonify(schema_info.serialize())

@app.get("/datasets/<dataset_name>/schemas")
@authorize
def schemas_list(dataset_name):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    schema_infos = [schema_info.serialize() 
        for schema_info in dataset.state.schemas_all()]

    return flask.jsonify(schema_infos)

@app.get("/datasets/<dataset_name>/schemas/<re('schema-.*'):schema_name>")
@authorize
def schemas_info(dataset_name, schema_name):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    schema_infos = dataset.state.schemas_all(name=schema_name)

    if len(schema_infos) != 1:
        return flask.jsonify({"error": "Schema not found."}), 404

    schema_info, = schema_infos

    return flask.jsonify(schema_info.serialize())

@app.get("/datasets/<dataset_name>/schemas/<schema_status>")
@authorize
def schemas_status(dataset_name, schema_status):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    after = None
    if "after" in flask.request.args:
        after = uuid.UUID(flask.request.args["after"])

    if schema_status not in {'accepted', 'pending', 'deprecated', 'rejected'}:
        return flask.jsonify({
        "error": "Invalid status.",
        "valid_statuses": ["accepted", "pending", "deprecated", "rejected"],
    }), 400

    dataset = Dataset(dataset_directory)
    schema_identifiers = [schema.serialize() for schema in 
        dataset.state.schemas_by_status(schema_status, after=after)]

    return flask.jsonify(schema_identifiers)

@app.get("/datasets/<dataset_name>/schemas/<schema_name>/<int:schema_version>")
@authorize
def schemas_get(dataset_name, schema_name, schema_version):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    schema = dataset.state.schema(schema_name, schema_version)

    if schema is None:
        return flask.jsonify({"error": "Schema not found."}), 404

    schema_buf = dataset.depot.read(schema.identifier(), 0, schema.size)

    events_ = [event.serialize() for event in 
        dataset.state.events_by_object(
            events.Identifier(schema.uuid, schema.version))]

    return flask.jsonify({
        "schema": schema.serialize(),
        "bytes": base64.b64encode(schema_buf).decode(),
        "events": events_,
    })

@app.patch("/datasets/<dataset_name>/schemas/<re('schema-.*'):schema_name>")
@accept_json
@authorize
def schemas_update(dataset_name, schema_name):
    request_data = flask.request.json
    if request_data is None:
        return flask.jsonify({"error": "Request JSON is None."}), 500

    if "schema" not in request_data:
        return flask.jsonify({"error": "Missing key 'schema'."}), 400

    schema_buf = base64.b64decode(request_data["schema"])

    if not validators.is_schema(schema_name):
        return flask.jsonify(
            {"error": "Schema names must start with 'schema-'."}), 400
    
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    with lock:
        schema_info = dataset.state.schemas_all(name=schema_name)

        if len(schema_info) != 1:
            return flask.jsonify({"error": "Schema not found."}), 404

        schema_info, = schema_info

        sue = dataset.linker.link(
            events.ObjectUpdateEvent(
                events.Object(
                    schema_info.name, 
                    "application/schema+json",
                    len(schema_buf), 
                    events.HashTypeT.SHA256, 
                    hashlib.sha256(schema_buf).hexdigest(),
                    schema_info.uuid,
                    schema_info.versions)), 
            flask.g.username)

        id_ = sue.object.identifier()
        try:
            dataset.depot.reserve(id_, sue.object.size)
            dataset.depot.write(id_, 0, schema_buf)
            dataset.depot.finalize(id_)
            dataset.machine.process_event(sue)
        except Exception as e:
            if dataset.depot.exists(id_):
                dataset.depot.purge(id_)
            raise e

        schema_info = dataset.state.schemas_all(name=schema_name)

        if len(schema_info) != 1:
            return flask.jsonify({"error": "Schema not found."}), 404

        schema_info, = schema_info

    return flask.jsonify(schema_info.serialize())

@app.delete(
    "/datasets/<dataset_name>/schemas/<schema_name>/<int:schema_version>")
@authorize
def schemas_deprecate(dataset_name, schema_name, schema_version):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    schema = dataset.state.schema(schema_name, schema_version)

    if schema is None:
        return flask.jsonify({"error": "Schema not found."}), 404

    with lock:
        ode = events.ObjectDeleteEvent(schema.identifier())
        ode = dataset.linker.link(ode, flask.g.username)
        dataset.machine.process_event(ode)

    return flask.jsonify(interfaces.NamedIdentifier(
            schema.uuid, schema_version, schema_name).serialize())

@app.get("/datasets/<dataset_name>/owners")
@authorize
def owners_list(dataset_name):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    owners = dataset.state.owners()

    return flask.jsonify(owners)

@app.put("/datasets/<dataset_name>/owners/<user>")
@authorize
def owners_add(dataset_name, user):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    con = sqlite3.connect(database_path)
    cur = con.cursor()
    cur.execute("""SELECT COUNT(*) 
            FROM users WHERE username = ?""",
        (user,))
    count, = cur.fetchone()
    con.close()

    if count != 1:
        return flask.jsonify({"error": "User does not exist."}), 400

    with lock:
        oae = events.OwnerAddEvent(user)
        oae = dataset.linker.link(oae, flask.g.username)
        dataset.machine.process_event(oae)

    return flask.jsonify({
        "user": user,
    })

@app.delete("/datasets/<dataset_name>/owners/<user>")
@authorize
def owners_remove(dataset_name, user):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    with lock:
        oae = events.OwnerRemoveEvent(user)
        oae = dataset.linker.link(oae, flask.g.username)
        dataset.machine.process_event(oae)

    return flask.jsonify({
        "user": user,
    })

@app.post("/datasets/<dataset_name>/objects")
@accept_json
@authorize
def objects_create(dataset_name):
    request_data = flask.request.json
    if request_data is None:
        return flask.jsonify({"error": "Request JSON is None."}), 500

    if "name" not in request_data:
        return flask.jsonify({"error": "Missing key 'name'."}), 400

    if "object" not in request_data:
        return flask.jsonify({"error": "Missing key 'object'."}), 400

    if "mimetype" not in request_data:
        return flask.jsonify({"error": "Missing key 'mimetype'."}), 400

    object_name = request_data["name"]
    object_buf = base64.b64decode(request_data["object"])
    object_mime = request_data["mimetype"]

    if validators.is_schema(object_name):
        return flask.jsonify(
            {"error": "Object names may not start with 'schema-'."}), 400
    
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    oce = dataset.linker.link(
        events.ObjectCreateEvent(
            events.Object(
                object_name, 
                object_mime,
                len(object_buf), 
                events.HashTypeT.SHA256, 
                hashlib.sha256(object_buf).hexdigest())), 
        flask.g.username)

    with lock:
        id_ = oce.object.identifier()
        try:
            dataset.depot.reserve(id_, oce.object.size)
            dataset.depot.write(id_, 0, object_buf)
            dataset.depot.finalize(id_)
            dataset.machine.process_event(oce)
        except Exception as e:
            if dataset.depot.exists(id_):
                dataset.depot.purge(id_)
            raise e

    return flask.jsonify(oce.object.identifier().serialize())

@app.get("/datasets/<dataset_name>/objects")
@authorize
def objects_list(dataset_name):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    after = None
    if "after" in flask.request.args:
        after = uuid.UUID(flask.request.args["after"])

    dataset = Dataset(dataset_directory)
    objects = [object_.serialize() 
        for object_ in dataset.state.objects_all(after=after)]

    return flask.jsonify(objects)

@app.get(
    "/datasets/<dataset_name>/objects"
    "/<re('[0-9A-Fa-f-]{36}'):object_uuid>")
@authorize
def objects_info(dataset_name, object_uuid):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    object_uuid = uuid.UUID(object_uuid)

    dataset = Dataset(dataset_directory)
    objects = [object_.serialize() 
        for object_ in dataset.state.objects_all(uuid_=object_uuid)]

    if len(objects) != 1:
        return flask.jsonify({"error": "Schema not found."}), 404

    object_, = objects

    return flask.jsonify(object_)

@app.patch("/datasets/<dataset_name>/objects/<object_uuid>")
@accept_json
@authorize
def objects_update(dataset_name, object_uuid):
    request_data = flask.request.json
    if request_data is None:
        return flask.jsonify({"error": "Request JSON is None."}), 500

    if "name" not in request_data:
        return flask.jsonify({"error": "Missing key 'name'."}), 400

    if "object" not in request_data:
        return flask.jsonify({"error": "Missing key 'object'."}), 400

    if "mimetype" not in request_data:
        return flask.jsonify({"error": "Missing key 'mimetype'."}), 400

    object_name = request_data["name"]
    object_buf = base64.b64decode(request_data["object"])
    object_mime = request_data["mimetype"]

    if validators.is_schema(object_name):
        return flask.jsonify(
            {"error": "Object names may not start with 'schema-'."}), 400
    
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    with lock:
        object_info = dataset.state.objects_all(uuid_=object_uuid)

        if len(object_info) != 1:
            return flask.jsonify({"error": "Object not found."}), 404

        object_info, = object_info

        oue = dataset.linker.link(
            events.ObjectUpdateEvent(
                events.Object(
                    object_name, 
                    object_mime,
                    len(object_buf), 
                    events.HashTypeT.SHA256, 
                    hashlib.sha256(object_buf).hexdigest(),
                    object_info.uuid,
                    object_info.versions)), 
            flask.g.username)

        id_ = oue.object.identifier()
        try:
            dataset.depot.reserve(id_, oue.object.size)
            dataset.depot.write(id_, 0, object_buf)
            dataset.depot.finalize(id_)
            dataset.machine.process_event(oue)
        except Exception as e:
            if dataset.depot.exists(id_):
                dataset.depot.purge(id_)
            raise e

    return flask.jsonify(oue.object.identifier().serialize())

@app.get("/datasets/<dataset_name>/objects/<object_status>")
@authorize
def objects_status(dataset_name, object_status):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    after = None
    if "after" in flask.request.args:
        after = uuid.UUID(flask.request.args["after"])

    if object_status not in {'accepted', 'pending', 'deleted', 'rejected'}:
        return flask.jsonify({
        "error": "Invalid status.",
        "valid_statuses": ["accepted", "pending", "deleted", "rejected"],
    }), 400

    dataset = Dataset(dataset_directory)
    objects = [object_.serialize() for object_ in 
        dataset.state.objects_by_status(object_status, after=after)]

    return flask.jsonify(objects)

@app.get(
    "/datasets/<dataset_name>/objects"
    "/<re('[0-9A-Fa-f-]{36}'):object_uuid>/<int:object_version>")
@authorize
def objects_get(dataset_name, object_uuid, object_version):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    object_ = dataset.state.object(
        events.Identifier(object_uuid, object_version))

    if object_ is None:
        return flask.jsonify({"error": "Object not found."}), 404

    object_buf = dataset.depot.read(object_.identifier(), 0, object_.size)

    events_ = [event.serialize() for event in 
        dataset.state.events_by_object(
            events.Identifier(object_.uuid, object_.version))]

    annotations = [annotation.serialize() for annotation in 
        dataset.state.annotations_by_object(object_.identifier())]

    return flask.jsonify({
        "object": object_.serialize(),
        "bytes": base64.b64encode(object_buf).decode(),
        "events": events_,
        "annotations": annotations,
    })

@app.delete(
    "/datasets/<dataset_name>/objects/<object_uuid>/<int:object_version>")
@authorize
def objects_delete(dataset_name, object_uuid, object_version):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    object_ = dataset.state.object(
        events.Identifier(object_uuid, object_version))

    if object_ is None:
        return flask.jsonify({"error": "Object not found."}), 404

    if validators.is_schema(object_.name):
        return flask.jsonify(
            {"error": "Schemas should not be deprecated here."}), 400

    with lock:
        ode = events.ObjectDeleteEvent(object_.identifier())
        ode = dataset.linker.link(ode, flask.g.username)
        dataset.machine.process_event(ode)

    return flask.jsonify({
        "uuid": object_uuid,
        "version": object_version,
    })

@app.get("/datasets/<dataset_name>/events")
@authorize
def events_list(dataset_name):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    after = None
    if "after" in flask.request.args:
        after = uuid.UUID(flask.request.args["after"])

    dataset = Dataset(dataset_directory)

    def rk_event_type_serializer(dataset):
        def fn(info):
            event = dataset.record_keeper.read(info.uuid)
            data = event.serialize()
            data["type"] = info.type
            return data
        return fn

    events_ = list(map(rk_event_type_serializer(dataset), 
        dataset.state.events_all(after=after)))

    return flask.jsonify(events_)

@app.put("/datasets/<dataset_name>/events/<event_uuid>/accept")
@authorize
def events_accept(dataset_name, event_uuid):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    with lock:
        rae = events.ReviewAcceptEvent(uuid.UUID(event_uuid))
        rae = dataset.linker.link(rae, flask.g.username)
        dataset.machine.process_event(rae)

    return flask.jsonify({
        "uuid": event_uuid,
    })

@app.put("/datasets/<dataset_name>/events/<event_uuid>/reject")
@authorize
def events_reject(dataset_name, event_uuid):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    with lock:
        rre = events.ReviewRejectEvent(uuid.UUID(event_uuid))
        rre = dataset.linker.link(rre, flask.g.username)
        dataset.machine.process_event(rre)

    return flask.jsonify({
        "uuid": event_uuid,
    })

@app.post("/datasets/<dataset_name>/annotations")
@accept_json
@authorize
def annotations_create(dataset_name):
    request_data = flask.request.json
    if request_data is None:
        return flask.jsonify({"error": "Request JSON is None."}), 500

    if "annotation" not in request_data:
        return flask.jsonify({"error": "Missing key 'annotation'."}), 400

    if "schema" not in request_data:
        return flask.jsonify({"error": "Missing key 'schema_name'."}), 400

    if "name" not in request_data["schema"]:
        return flask.jsonify({"error": "Missing schema key 'name'."}), 400

    if "version" not in request_data["schema"]:
        return flask.jsonify({"error": "Missing schema key 'version'."}), 400

    if "object_identifiers" not in request_data:
        return flask.jsonify(
            {"error": "Missing key 'object_identifiers'."}), 400

    if len(request_data["object_identifiers"]) < 1:
        return flask.jsonify(
            {"error": "Requires at least 1 object identifier."}), 400

    object_identifiers = []
    for obj_id in request_data["object_identifiers"]:
        if "uuid" not in obj_id or "version" not in obj_id:
            return flask.jsonify({"error": 
                "Object identifiers require 'uuid' and 'version'."}), 400
        object_identifiers.append(
            events.Identifier(uuid.UUID(obj_id["uuid"]), obj_id["version"])) 

    schema_name = request_data["schema"]["name"]
    schema_version = request_data["schema"]["version"]

    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    schema = dataset.state.schema(schema_name, schema_version)
    if schema is None:
        return flask.jsonify({"error": "Schema not found."}), 404

    annotation_buf = base64.b64decode(request_data["annotation"])

    ace = dataset.linker.link(
        events.AnnotationCreateEvent(
            object_identifiers,
            events.Annotation(
                schema.identifier(),
                len(annotation_buf), 
                events.HashTypeT.SHA256, 
                hashlib.sha256(annotation_buf).hexdigest())), 
        flask.g.username)

    with lock:
        id_ = ace.annotation.identifier()
        try:
            dataset.depot.reserve(id_, ace.annotation.size)
            dataset.depot.write(id_, 0, annotation_buf)
            dataset.depot.finalize(id_)
            dataset.machine.process_event(ace)
        except Exception as e:
            if dataset.depot.exists(id_):
                dataset.depot.purge(id_)
            raise e

    return flask.jsonify(ace.annotation.identifier().serialize())

@app.get("/datasets/<dataset_name>/annotations")
@authorize
def annotations_list(dataset_name):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    after = None
    if "after" in flask.request.args:
        after = uuid.UUID(flask.request.args["after"])

    dataset = Dataset(dataset_directory)
    annotations = [annotation.serialize() 
        for annotation in dataset.state.annotations_all(after=after)]

    return flask.jsonify(annotations)

@app.get(
    "/datasets/<dataset_name>/annotations"
    "/<re('[0-9A-Fa-f-]{36}'):annotation_uuid>")
@authorize
def annotations_info(dataset_name, annotation_uuid):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    annotation_uuid = uuid.UUID(annotation_uuid)

    dataset = Dataset(dataset_directory)
    annotations = [annotation.serialize() 
        for annotation in dataset.state.annotations_all(uuid_=annotation_uuid)]

    if len(annotations) != 1:
        return flask.jsonify({"error": "Schema not found."}), 404

    annotation, = annotations

    return flask.jsonify(annotation)

@app.patch("/datasets/<dataset_name>/annotations/<annotation_uuid>")
@accept_json
@authorize
def annotations_update(dataset_name, annotation_uuid):
    request_data = flask.request.json
    if request_data is None:
        return flask.jsonify({"error": "Request JSON is None."}), 500

    if "annotation" not in request_data:
        return flask.jsonify({"error": "Missing key 'annotation'."}), 400

    if "schema" not in request_data:
        return flask.jsonify({"error": "Missing key 'schema_name'."}), 400

    if "name" not in request_data["schema"]:
        return flask.jsonify({"error": "Missing schema key 'name'."}), 400

    if "version" not in request_data["schema"]:
        return flask.jsonify({"error": "Missing schema key 'version'."}), 400

    schema_name = request_data["schema"]["name"]
    schema_version = request_data["schema"]["version"]

    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)

    schema = dataset.state.schema(schema_name, schema_version)
    if schema is None:
        return flask.jsonify({"error": "Schema not found."}), 404

    annotation_buf = base64.b64decode(request_data["annotation"])

    with lock:
        annotation_info = dataset.state.annotations_all(uuid_=annotation_uuid)

        if len(annotation_info) != 1:
            return flask.jsonify({"error": "Annotation not found."}), 404

        annotation_info, = annotation_info

        aue = dataset.linker.link(
        events.AnnotationUpdateEvent(
            events.Annotation(
                schema.identifier(),
                len(annotation_buf), 
                events.HashTypeT.SHA256, 
                hashlib.sha256(annotation_buf).hexdigest(),
                annotation_info.uuid,
                annotation_info.versions)), 
        flask.g.username)

        id_ = aue.annotation.identifier()
        try:
            dataset.depot.reserve(id_, aue.annotation.size)
            dataset.depot.write(id_, 0, annotation_buf)
            dataset.depot.finalize(id_)
            dataset.machine.process_event(aue)
        except Exception as e:
            if dataset.depot.exists(id_):
                dataset.depot.purge(id_)
            raise e

    return flask.jsonify(aue.annotation.identifier().serialize())

@app.get("/datasets/<dataset_name>/annotations/<annotation_status>")
@authorize
def annotations_status(dataset_name, annotation_status):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    after = None
    if "after" in flask.request.args:
        after = uuid.UUID(flask.request.args["after"])

    if annotation_status not in {'accepted', 'pending', 'deleted', 'rejected'}:
        return flask.jsonify({
        "error": "Invalid status.",
        "valid_statuses": ["accepted", "pending", "deleted", "rejected"],
    }), 400

    dataset = Dataset(dataset_directory)
    annotations = [annotation.serialize() for annotation in 
        dataset.state.annotations_by_status(annotation_status, after=after)]

    return flask.jsonify(annotations)

@app.delete(
    "/datasets/<dataset_name>/annotations"
    "/<annotation_uuid>/<int:annotation_version>")
@authorize
def annotations_delete(dataset_name, annotation_uuid, annotation_version):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    annotation = dataset.state.annotation(
        events.Identifier(annotation_uuid, annotation_version))

    if annotation is None:
        return flask.jsonify({"error": "Annotation not found."}), 404

    with lock:
        ade = events.AnnotationDeleteEvent(annotation.identifier())
        ade = dataset.linker.link(ade, flask.g.username)
        dataset.machine.process_event(ade)

    return flask.jsonify(annotation.identifier().serialize())

@app.get(
    "/datasets/<dataset_name>/annotations"
    "/<re('[0-9A-Fa-f-]{36}'):annotation_uuid>/<int:annotation_version>")
@authorize
def annotations_get(dataset_name, annotation_uuid, annotation_version):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    annotation = dataset.state.annotation(
        events.Identifier(annotation_uuid, annotation_version))

    if annotation is None:
        return flask.jsonify({"error": "Annotation not found."}), 404

    annotation_buf = dataset.depot.read(
        annotation.identifier(), 0, annotation.size)

    events_ = [event.serialize() for event in 
        dataset.state.events_by_annotation(
            events.Identifier(annotation.uuid, annotation.version))]

    objects = [object_.serialize() for object_ in 
        dataset.state.objects_by_annotation(annotation.uuid)]

    return flask.jsonify({
        "annotation": annotation.serialize(),
        "bytes": base64.b64encode(annotation_buf).decode(),
        "events": events_,
        "objects": objects,
    })

@cli.command("run")
def run():
    if not database_path.exists():
        print("Please initialize the application with the `init` command.")
        exit(1)

    app.run()

if __name__ == "__main__":
    cli()
