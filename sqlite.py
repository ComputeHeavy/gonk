import core
import uuid
import json
import pathlib
import sqlite3

class RecordKeeper(core.RecordKeeper):
    def __init__(self, parent_directory: pathlib.Path):
        super().__init__()
        if not parent_directory.exists():
            raise ValueError("parent directory does not exist")

        self.database_path = parent_directory.joinpath("rk.db")
        self.con = sqlite3.connect(self.database_path)

        cur = self.con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid text NOT NULL,
            event JSON NOT NULL
        )''')

    def add(self, event: core.EventT):
        event_data = event.serialize()
        event_data["type"] = event.__class__.__name__

        event_json = json.dumps(event_data)
        cur = self.con.cursor()
        cur.execute(
            "INSERT INTO events (uuid, json) VALUES (?, ?)",
            (event_data["uuid"], event_json))

    def read(self, uuid_: uuid.UUID) -> core.Event:
        cur = self.con.cursor()
        cur.execute("SELECT json FROM events WHERE uuid = ?", (str(uuid_),))

        res = cur.fetchone()
        if res is None:
            raise ValueError("event does not exist")

        event_json, = res
        event_data = json.loads(event_json)
        event = getattr(core, event_data["type"]).deserialize(event_data)

        return event

    def exists(self, uuid_: uuid.UUID) -> bool:
        cur = self.con.cursor()
        cur.execute("SELECT id FROM events WHERE uuid = ?", (str(uuid_),))

        res = cur.fetchone()
        if res is None:
            return False

        return True

    def next(self, uuid_: uuid.UUID | None) -> uuid.UUID | None:
        cur = self.con.cursor()
        if uuid_ is None:
            cur.execute("SELECT uuid FROM events WHERE id = 0")
            res = cur.fetchone()

            if res is None:
                return None

            next_, = res

            return uuid.UUID(next_)

        cur.execute("SELECT id FROM events WHERE uuid = ?", (str(uuid_)))
        res = cur.fetchone()
        
        if res is None:
            return None

        id_, = res
        cur.execute("SELECT uuid FROM events WHERE id = ?", (id_+1,))
        res = cur.fetchone()

        if res is None:
            return None

        next_, = res
        return uuid.UUID(next_)