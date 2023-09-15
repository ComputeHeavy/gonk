'''
GET     /datasets/{name}/schemas/{name} - Latest
GET     /datasets/{name}/schemas/{name}/{version} - Details
PATCH   /datasets/{name}/schemas/{name} - Update

GET     /datasets/{name}/owners - List
    State get owners
    Link to usernames in app

PUT     /datasets/{name}/owners - Add
    OwnerAddEvent
DELETE  /datasets/{name}/owners - Delete
    OwnerRemoveEvent

POST    /datasets/{name}/objects - Create
    ObjectCreateEvent (signed)
    Object bytes
GET     /datasets/{name}/objects - List (All, Paged)
GET     /datasets/{name}/objects/{uuid}/{version} - Details

PATCH   /datasets/{name}/objects/{uuid} - Version
    ObjectUpdateEvent
    Object bytes

DELETE   /datasets/{name}/objects/{uuid}/{version} - Delete
    ObjectDeleteEvent

EVENTS
PENDING EVENTS

PUT     /datasets/{name}/events/{uuid}/accept
    ReviewAcceptEvent

PUT     /datasets/{name}/events/{uuid}/reject
    ReviewRejectEvent

POST    /datasets/{name}/objects/{uuid}/{version}/annotations - Create
    AnnotationCreateEvent
    Annotation bytes

GET     /datasets/{name}/objects/{uuid}/{version}/annotations - List
GET     /datasets/{name}/objects/{uuid}/{version}/annotations/{uuid}/{version} - Details
    
PATCH   /datasets/{name}/objects/{uuid}/{version}/annotations/{uuid} - Version
    AnnotationUpdateEvent
    Annotation bytes

DELETE   /datasets/{name}/objects/{uuid}/{version}/annotations/{uuid}/{version} - Delete
    AnnotationDeleteEvent

TAGS

GET     /dataset/{did} - Details
PATCH   /dataset/{did} - Rename
DELETE  /dataset/{did} - Delete

GET     /datasets/{did}/readme - Latest
GET     /datasets/{did}/readme/{version} - Details
POST    /datasets/{did}/readme - Create
PATCH   /datasets/{did}/readme - Update

==== DONE ====

POST    /datasets/{name} - Create
GET     /datasets - List

POST    /datasets/{name}/schemas - Create
GET     /datasets/{name}/schemas - List

'''

import fs
import core
import base64
import events
import sqlite
import integrity

import sys
import click
import flask
import string
import typing
import sqlite3
import hashlib
import pathlib
import secrets
import traceback 
import jsonschema
import multiprocessing

lock = multiprocessing.Lock() # TODO: lock per dataset 

root_directory = pathlib.Path("root")
datasets_directory = root_directory.joinpath("datasets")
database_path = root_directory.joinpath("gonk.db")

app = flask.Flask(__name__)

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

@cli.group("user")
def user():
    pass

@user.command("add")
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

@user.command("rekey")
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

@user.command("list")
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
        self.machine = core.Machine()
        self.depot = fs.Depot(dataset_directory)
        self.state = sqlite.State(dataset_directory, self.record_keeper)

        self.machine.register(core.FieldValidator())
        self.machine.register(integrity.HashChainValidator(self.record_keeper))
        self.machine.register(core.SchemaValidator(self.depot))
        self.machine.register(self.record_keeper)
        self.machine.register(self.state)

@app.post("/datasets/<dataset_name>")
@authorize
def datasets_create(dataset_name):
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
            "message": f"Dataset created.",
            "name": dataset_name,
        })

@app.get("/datasets")
@authorize
def datasets_list():
    return flask.jsonify({
        "datasets": [d.stem for d in datasets_directory.iterdir()]
    })

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
        return flask.jsonify({"error": "Missing key 'event'."}), 400

    if "schema" not in request_data:
        return flask.jsonify({"error": "Missing key 'schema'."}), 400

    schema_name = request_data["name"]
    schema_buf = base64.b64decode(request_data["schema"])

    if not core.is_schema(schema_name):
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

    return flask.jsonify({
            "message": f"Schema created.",
            "name": schema_name,
            "dataset": dataset_name,
        })

@app.get("/datasets/<dataset_name>/schemas")
@authorize
def schemas_list(dataset_name):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    schemas = [schema.serialize() for schema in dataset.state.schemas()]

    return flask.jsonify({
            "schemas": schemas,
        })

@app.get("/datasets/<dataset_name>/schemas/<schema_name>")
@app.get("/datasets/<dataset_name>/schemas/<schema_name>/<int:version>")
@authorize
def schemas_get(dataset_name, schema_name, version=-1):
    dataset_directory = datasets_directory.joinpath(dataset_name)
    if not dataset_directory.exists():
        return flask.jsonify({"error": "Dataset not found."}), 404

    dataset = Dataset(dataset_directory)
    schemas = {schema.name: schema for schema in dataset.state.schemas()}

    if schema_name not in schemas:
        return flask.jsonify({"error": "Schema not found."}), 404

    schema = schemas[schema_name]

    if version >= schema.versions:
        return flask.jsonify({"error": "Version does not exist."}), 404

    if version < 0:
        version = schema.versions-1

    id_ = events.Identifier(schema.uuid, version)
    schema_buf = b""
    while True:
        size = 5*1024
        buf = dataset.depot.read(id_, 0, size)
        schema_buf += buf
        if len(buf) < size:
            break

    return flask.jsonify({
            "schema": base64.b64encode(schema_buf).decode(),
        })

@cli.command("run")
def run():
    if not database_path.exists():
        print("Please initialize the application with the `init` command.")
        exit(1)

    app.run()

if __name__ == "__main__":
    cli()
