'''
PUT     /datasets/{name} - Create
GET     /datasets - List

POST    /datasets/{name}/schemas - Create
GET     /datasets/{name}/schemas - List
GET     /datasets/{name}/schemas/{name} - List
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
'''

import click
import flask
import string
import typing
import sqlite3
import hashlib
import pathlib
import secrets

root_directory = pathlib.Path("root")
database_path = root_directory.joinpath("gonk.db")

app = flask.Flask(__name__)

@app.before_request
def before_request():
    flask.g.con = sqlite3.connect(database_path)

@app.teardown_request
def teardown_request(error):
    if hasattr(flask.g, "con"):
        flask.g.con.close()

def authorize(endpoint):
    def wrapper(*args, **kwargs):
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
    return wrapper

@app.get("/")
@authorize
def index():
    return flask.jsonify({"message": "hello"})

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

@cli.command("run")
def run():
    if not database_path.exists():
        print("Please initialize the application with the `init` command.")
        exit(1)

    app.run()

if __name__ == "__main__":
    cli()


# @app.post("/datasets/{name}")
# def read_item(name: str):
#     allowed = set(string.ascii_letters + string.digits + '-')
#     if len(set(name).difference(allowed)) > 0:
#         raise fastapi.HTTPException(
#             status_code=400, 
#             detail="Only letters, numbers, and dashes (-) allowed.")

#     if name.startswith("-"):
#         raise fastapi.HTTPException(
#             status_code=400, 
#             detail="Names may not start with a dash.")

#     dataset_directory = root_directory.joinpath(name)

#     if dataset_directory.exists():
#         raise fastapi.HTTPException(
#             status_code=400, 
#             detail="Dataset already exists.")

#     if name in datasets:
#         raise fastapi.HTTPException(
#             status_code=400, 
#             detail="Dataset already loaded.")        

#     record_keeper = fs.RecordKeeper(dataset_directory)

#     datasets[name] = {
#         "machine": core.Machine(),
#         "depot": fs.Depot(dataset_directory),
#         "record_keeper": record_keeper,
#         "state": sqlite.State(dataset_directory, record_keeper),
#     }

#     machine.register(core.FieldValidator())
#     machine.register(sigs.SignatureValidator())
#     machine.register(core.SchemaValidator(datasets[name]["depot"]))
#     machine.register(datasets[name]["record_keeper"])
#     machine.register(datasets[name]["state"])

#     return {"name": name}