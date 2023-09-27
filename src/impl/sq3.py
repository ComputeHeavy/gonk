# Copyright 2023 - Compute Heavy Industries Incorporated
# This work is released, distributed, and licensed under AGPLv3.

import uuid
import json
import enum
import typing
import pathlib
import sqlite3

from gonk.core import interfaces
from gonk.core import exceptions
from gonk.core import validators
from gonk.core import events

class RecordKeeper(interfaces.RecordKeeper):
    """SQLite backed RecordKeeper.

    Events are stored in a table, JSON serialized. ID primary key and
    event UUID are indexed."""
    def __init__(self, parent_directory: pathlib.Path):
        super().__init__()
        if not parent_directory.exists():
            raise ValueError("parent directory does not exist")

        self.database_path = parent_directory.joinpath("rk.db")

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL,
                event JSON NOT NULL
            );

            CREATE INDEX idx_events_uuid ON events(uuid);
        """)
        con.commit()
        con.close()

    def add(self, event: events.EventT):
        event_data = event.serialize()
        event_data["type"] = event.__class__.__name__

        event_json = json.dumps(event_data)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO events (uuid, event) VALUES (?, ?)",
            (event_data["uuid"], event_json))
        con.commit()
        con.close()

    def read(self, uuid_: uuid.UUID) -> events.Event:
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("SELECT event FROM events WHERE uuid = ?", (str(uuid_),))

        res = cur.fetchone()
        con.close()
        if res is None:
            raise ValueError("event does not exist")

        event_json, = res
        event_data = json.loads(event_json)
        event = getattr(events, event_data["type"]).deserialize(event_data)

        return event

    def exists(self, uuid_: uuid.UUID) -> bool:
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("SELECT id FROM events WHERE uuid = ?", (str(uuid_),))

        res = cur.fetchone()
        con.close()
        if res is None:
            return False

        return True

    def next(self, uuid_: uuid.UUID | None=None) -> uuid.UUID | None:
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        if uuid_ is None:
            cur.execute("SELECT uuid FROM events ORDER BY id LIMIT 1")
            res = cur.fetchone()

            if res is None:
                con.close()
                return None

            next_, = res
            con.close()
            return uuid.UUID(next_)

        cur.execute("SELECT id FROM events WHERE uuid = ?", (str(uuid_),))
        res = cur.fetchone()

        if res is None:
            con.close()
            return None

        id_, = res
        cur.execute("""SELECT uuid
                FROM events
                WHERE id > ?
                ORDER BY id
                LIMIT 1""",
            (id_,))
        res = cur.fetchone()

        if res is None:
            con.close()
            return None

        next_, = res
        con.close()
        return uuid.UUID(next_)

    def tail(self) -> uuid.UUID | None:
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("SELECT uuid FROM events ORDER BY id DESC LIMIT 1")
        res = cur.fetchone()

        if res is None:
            con.close()
            return None

        tail, = res
        con.close()
        return uuid.UUID(tail)

class StatusT(enum.Enum):
    """Enum for tracking object and annotation status in state.

    No status implies create accapted."""
    CREATE_PENDING = 1<<0
    """Create event pending."""
    CREATE_REJECTED = 1<<1
    """Create event rejected."""
    DELETE_PENDING = 1<<2
    """Delete event pending."""
    DELETE_ACCEPTED = 1<<3
    """Delete event accepted."""

class State(interfaces.State):
    """SQLite backed State.

    See parent :class:`gonk.core.interfaces.State` for more information."""
    def __init__(self,
        parent_directory: pathlib.Path,
        record_keeper: interfaces.RecordKeeper):

        super().__init__()
        if not parent_directory.exists():
            raise ValueError("parent directory does not exist")

        self.record_keeper = record_keeper

        self.database_path = parent_directory.joinpath("state.db")

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL,
                type TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event_review_link (
                event_uuid TEXT NOT NULL,
                review_uuid TEXT NOT NULL,
                accepted BOOL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS objects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL,
                version INTEGER NOT NULL,
                object JSON NOT NULL
            );

            CREATE TABLE IF NOT EXISTS schemas (
                name TEXT NOT NULL,
                uuid TEXT NOT NULL,
                version INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS object_event_link (
                object_uuid TEXT NOT NULL,
                object_version INTEGER NOT NULL,
                event_uuid TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS object_status (
                uuid TEXT NOT NULL,
                version INTEGER NOT NULL,
                status TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS annotations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL,
                version INTEGER NOT NULL,
                annotation JSON NOT NULL
            );

            CREATE TABLE IF NOT EXISTS object_annotation_link (
                object_uuid TEXT NOT NULL,
                object_version INTEGER NOT NULL,
                annotation_uuid TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS annotation_event_link (
                annotation_uuid TEXT NOT NULL,
                annotation_version INTEGER NOT NULL,
                event_uuid TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS annotation_status (
                uuid TEXT NOT NULL,
                version INTEGER NOT NULL,
                status TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS owners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner TEXT NOT NULL
            );
        """)

        cur.execute("""SELECT name 
            FROM sqlite_master 
            WHERE type='index'""")

        res = cur.fetchall()

        indices = {idx for idx, in res}

        statements = {
            "idx_events_uuid": 
                """CREATE INDEX idx_events_uuid 
                    ON events(uuid)""",
            "idx_objects_uuid": 
                """CREATE INDEX idx_objects_uuid 
                    ON objects(uuid)""",
            "idx_objects_object_hash": 
                """CREATE INDEX idx_objects_object_hash 
                    ON objects(object->>'$.hash')""",
            "idx_object_status_uuid": 
                """CREATE INDEX idx_object_status_uuid 
                    ON object_status(uuid)""",
            "idx_object_event_link_object_uuid": 
                """CREATE INDEX idx_object_event_link_object_uuid 
                    ON object_event_link(object_uuid)""",
            "idx_object_event_link_event_uuid": 
                """CREATE INDEX idx_object_event_link_event_uuid 
                    ON object_event_link(event_uuid)""",
            "idx_annotations_uuid": 
                """CREATE INDEX idx_annotations_uuid 
                    ON annotations(uuid)""",
            "idx_annotation_status_uuid": 
                """CREATE INDEX idx_annotation_status_uuid 
                    ON annotation_status(uuid)""",
        }

        for name, statement in statements.items():
            if name not in indices:
                cur.execute(statement)

        con.commit()
        con.close()

    def _accepted_to_review(self, type_, accepted):
        reviewables = [
            "ObjectCreateEvent", 
            "ObjectUpdateEvent", 
            "ObjectDeleteEvent",
            "AnnotationCreateEvent",
            "AnnotationUpdateEvent",
            "AnnotationDeleteEvent"]

        review = None
        if accepted is None:
            if type_ in reviewables:
                review = "PENDING"
        else:
            if bool(accepted):
                review = "ACCEPTED"
            else:
                review = "REJECTED"

        return review

    def events_by_object(self, identifier: events.Identifier):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT E.uuid, E.type, ERL.accepted 
                FROM events E
                INNER JOIN object_event_link OEL
                    ON E.uuid = OEL.event_uuid
                LEFT JOIN event_review_link ERL
                    ON E.uuid = ERL.event_uuid
                WHERE OEL.object_uuid = ?
                    AND OEL.object_version = ?
                ORDER BY E.id""",
            (str(identifier.uuid), identifier.version))

        res = cur.fetchall()
        con.close()

        return [interfaces.EventInfo(uuid.UUID(uu), type_, 
                self._accepted_to_review(type_, accepted)) 
            for uu, type_, accepted in res]

    def events_by_annotation(self, identifier: events.Identifier):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT E.uuid, E.type, ERL.accepted
                FROM events E
                INNER JOIN annotation_event_link AEL
                    ON E.uuid = AEL.event_uuid
                LEFT JOIN event_review_link ERL
                    ON E.uuid = ERL.event_uuid
                WHERE AEL.annotation_uuid = ?
                    AND AEL.annotation_version = ?
                ORDER BY E.id""",
            (str(identifier.uuid), identifier.version))

        res = cur.fetchall()
        con.close()

        return [interfaces.EventInfo(uuid.UUID(uu), type_, 
                self._accepted_to_review(type_, accepted)) 
            for uu, type_, accepted in res]

    def events_all(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where += """ WHERE E.id > (
                SELECT id 
                FROM events 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT E.uuid, E.type, ERL.accepted
            FROM events E
            LEFT JOIN event_review_link ERL
                ON E.uuid = ERL.event_uuid
            {where} 
            ORDER BY E.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [interfaces.EventInfo(uuid.UUID(uu), type_, 
                self._accepted_to_review(type_, accepted))
            for uu, type_, accepted in res]

    def annotations_all(self, 
        uuid_: None|uuid.UUID = None, after: None|uuid.UUID = None):

        if after is not None and uuid_ is not None:
            raise ValueError("only provide one of uuid_, after")

        params: tuple = tuple()
        where = ""
        if uuid_ is not None:
            where += "WHERE A.uuid = ?"
            params = (str(uuid_),)

        if after is not None:
            where += """WHERE A.id > (
                SELECT id 
                FROM annotations 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT DISTINCT A.uuid, 
                COUNT(A.version) OVER (PARTITION BY A.uuid),
                FIRST_VALUE(A.id) OVER (PARTITION BY A.uuid)
            FROM annotations A
            {where} 
            ORDER BY A.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [interfaces.AnnotationInfo(
            uuid.UUID(uu), vers) for uu, vers, _ in res]

    def annotations_by_object(self, object_identifier: events.Identifier):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT DISTINCT OAL.annotation_uuid,
                    COUNT(A.version) OVER (PARTITION BY A.uuid)
                FROM object_annotation_link OAL
                INNER JOIN annotations A
                    ON A.uuid = OAL.annotation_uuid
                WHERE OAL.object_uuid = ?
                    AND OAL.object_version = ?""", 
            (str(object_identifier.uuid), object_identifier.version))

        res = cur.fetchall()
        con.close()

        return [interfaces.AnnotationInfo(
            uuid.UUID(uu), vers) for uu, vers in res]

    def annotations_by_status(self, status: str, after: None|uuid.UUID = None):
        if status == "pending":
            return self._annotations_pending(after)
        elif status == "accepted":
            return self._annotations_accepted(after)
        elif status == "deleted":
            return self._annotations_deleted(after)
        elif status == "rejected":
            return self._annotations_rejected(after)
        else:
            raise ValueError(f"invalid status")

    def _annotations_accepted(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND A.id > (
                SELECT id 
                FROM annotations 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT A.uuid, A.version
            FROM annotations A
            LEFT JOIN annotation_status AST
                ON A.uuid = AST.uuid AND A.version = AST.version 
                    AND AST.status IN (
                        'CREATE_PENDING', 
                        'CREATE_REJECTED', 
                        'DELETE_ACCEPTED')
            WHERE AST.status IS NULL
                { where }
            ORDER BY A.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [events.Identifier(uuid.UUID(uuid_), version) 
            for uuid_, version, in res]

    def _annotations_pending(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """WHERE A.id > (
                SELECT id 
                FROM annotations 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT DISTINCT A.uuid, A.version
            FROM annotations A
            INNER JOIN annotation_status AST
                ON A.uuid = AST.uuid AND A.version = AST.version 
                    AND AST.status IN (
                        'CREATE_PENDING', 
                        'DELETE_PENDING')
            { where }
            ORDER BY A.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [events.Identifier(uuid.UUID(uuid_), version) 
            for uuid_, version, in res]

    def _annotations_deleted(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND A.id > (
                SELECT id 
                FROM annotations 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT A.uuid, A.version
            FROM annotations A
            JOIN annotation_status AST 
                ON A.uuid = AST.uuid 
                    AND A.version = AST.version
            WHERE AST.status = 'DELETE_ACCEPTED'
                { where }
            ORDER BY A.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [events.Identifier(uuid.UUID(uuid_), version) 
            for uuid_, version, in res]

    def _annotations_rejected(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND A.id > (
                SELECT id 
                FROM annotations 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT A.uuid, A.version
            FROM annotations A
            JOIN annotation_status AST 
                ON A.uuid = AST.uuid 
                    AND A.version = AST.version
            WHERE AS.status = 'CREATE_REJECTED'
                { where }
            ORDER BY A.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [events.Identifier(uuid.UUID(uuid_), version) 
            for uuid_, version, in res]

    def annotation(self, identifier: events.Identifier):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT A.annotation
                FROM annotations A
                WHERE A.uuid = ?
                    AND A.version = ?""", 
            (str(identifier.uuid), identifier.version))
        res = cur.fetchone()
        con.close()

        if res is None:
            return None

        annotation_json, = res
        annotation_data = json.loads(annotation_json)

        return events.Annotation.deserialize(annotation_data)

    def objects_all(self, 
        uuid_: None|uuid.UUID = None, after: None|uuid.UUID = None):

        if after is not None and uuid_ is not None:
            raise ValueError("only provide one of uuid_, after")

        params: tuple = tuple()
        where = "WHERE S.uuid IS NULL"
        if uuid_ is not None:
            where += " AND O.uuid = ?"
            params = (str(uuid_),)

        if after is not None:
            where += """ AND O.id > (
                SELECT id 
                FROM objects 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT DISTINCT O.uuid, 
                COUNT(O.version) OVER (PARTITION BY O.uuid),
                FIRST_VALUE(O.id) OVER (PARTITION BY O.uuid)
            FROM objects O
            LEFT JOIN schemas S
                ON O.uuid = S.uuid AND O.version = S.version
            {where} 
            ORDER BY O.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [interfaces.ObjectInfo(uuid.UUID(uu), ver) for uu, ver, _ in res]

    def objects_by_annotation(self, annotation_uuid: uuid.UUID):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT OAL.object_uuid, OAL.object_version
                FROM object_annotation_link OAL
                INNER JOIN objects O
                    ON OAL.object_uuid = O.uuid
                        AND OAL.object_version = O.version
                WHERE annotation_uuid = ?""", 
            (str(annotation_uuid),))

        res = cur.fetchall()
        con.close()

        return [events.Identifier(uuid.UUID(uuid_), version) 
            for uuid_, version in res]

    def objects_by_status(self, status: str, after: None|uuid.UUID = None):
        if status == "pending":
            return self._objects_pending(after)
        elif status == "accepted":
            return self._objects_accepted(after)
        elif status == "deleted":
            return self._objects_deleted(after)
        elif status == "rejected":
            return self._objects_rejected(after)
        else:
            raise ValueError(f"invalid status")

    def _objects_accepted(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND O.id > (
                SELECT id 
                FROM objects 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT O.uuid, O.version
            FROM objects O
            LEFT JOIN schemas S
                ON O.uuid = S.uuid AND O.version = S.version
            LEFT JOIN object_status OS
                ON O.uuid = OS.uuid AND O.version = OS.version 
                    AND OS.status IN (
                        'CREATE_PENDING', 
                        'CREATE_REJECTED', 
                        'DELETE_ACCEPTED')
            WHERE OS.status IS NULL
                AND S.uuid IS NULL
                { where }
            ORDER BY O.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [events.Identifier(uuid.UUID(uuid_), version) 
            for uuid_, version in res]

    def _objects_pending(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND O.id > (
                SELECT id 
                FROM objects 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT DISTINCT O.uuid, O.version
            FROM objects O
            LEFT JOIN schemas S
                ON O.uuid = S.uuid AND O.version = S.version
            INNER JOIN object_status OS
                ON O.uuid = OS.uuid AND O.version = OS.version 
                    AND OS.status IN (
                        'CREATE_PENDING', 
                        'DELETE_PENDING')
            WHERE S.uuid IS NULL
                { where }
            ORDER BY O.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [events.Identifier(uuid.UUID(uuid_), version) 
            for uuid_, version in res]

    def _objects_deleted(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND O.id > (
                SELECT id 
                FROM objects 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT O.uuid, O.version
            FROM objects O
            INNER JOIN object_status OS 
                ON O.uuid = OS.uuid 
                    AND O.version = OS.version
            WHERE OS.status = 'DELETE_ACCEPTED'
                { where }
            ORDER BY O.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [events.Identifier(uuid.UUID(uuid_), version) 
            for uuid_, version in res]

    def _objects_rejected(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND O.id > (
                SELECT id 
                FROM objects 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT O.uuid, O.version
            FROM objects O
            INNER JOIN object_status OS 
                ON O.uuid = OS.uuid 
                    AND O.version = OS.version
            WHERE OS.status = 'CREATE_REJECTED'
                { where }
            ORDER BY O.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [events.Identifier(uuid.UUID(uuid_), version) 
            for uuid_, version in res]

    def object(self, identifier: events.Identifier):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT O.object
                FROM objects O
                LEFT JOIN schemas S
                    ON O.uuid = S.uuid AND O.version = S.version
                WHERE O.uuid = ?
                    AND O.version = ?
                    AND S.uuid IS NULL""", 
            (str(identifier.uuid), identifier.version))
        res = cur.fetchone()
        con.close()

        if res is None:
            return None

        object_json, = res
        object_data = json.loads(object_json)

        return events.Object.deserialize(object_data)

    def schemas_all(self, name: None|str =None): 
        params: tuple = tuple()
        where = ""
        if name is not None:
            where = " WHERE name = ?"
            params = (name,)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT DISTINCT name, uuid, 
                COUNT(version) OVER (PARTITION BY uuid)
            FROM schemas""" + where, params)

        res = cur.fetchall()
        con.close()

        return [interfaces.SchemaInfo(name, uuid.UUID(uuid_), versions) 
            for name, uuid_, versions in res]

    def schemas_by_status(self, status: str, after: None|uuid.UUID = None):
        if status == "pending":
            return self._schemas_pending(after)
        elif status == "accepted":
            return self._schemas_accepted(after)
        elif status == "deprecated":
            return self._schemas_deprecated(after)
        elif status == "rejected":
            return self._schemas_rejected(after)
        else:
            raise ValueError(f"invalid status")

    def _schemas_accepted(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND O.id > (
                SELECT id 
                FROM objects 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT O.uuid, O.version, O.object->>'$.name'
            FROM objects O
            INNER JOIN schemas S
                ON O.uuid = S.uuid AND O.version = S.version
            LEFT JOIN object_status OS
                ON O.uuid = OS.uuid AND O.version = OS.version 
                    AND OS.status IN (
                        'CREATE_PENDING', 
                        'CREATE_REJECTED', 
                        'DELETE_ACCEPTED')
            WHERE OS.status IS NULL
                { where }
            ORDER BY O.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [interfaces.NamedIdentifier(uuid.UUID(uuid_), version, name) 
            for uuid_, version, name in res]

    def _schemas_pending(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """WHERE O.id > (
                SELECT id 
                FROM objects 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT DISTINCT O.uuid, O.version, O.object->>'$.name'
            FROM objects O
            INNER JOIN schemas S
                ON O.uuid = S.uuid AND O.version = S.version
            INNER JOIN object_status OS
                ON O.uuid = OS.uuid AND O.version = OS.version 
                    AND OS.status IN (
                        'CREATE_PENDING', 
                        'DELETE_PENDING')
            { where }
            ORDER BY O.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [interfaces.NamedIdentifier(uuid.UUID(uuid_), version, name) 
            for uuid_, version, name in res]

    def _schemas_deprecated(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND O.id > (
                SELECT id 
                FROM objects 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT O.uuid, O.version, O.object->>'$.name'
            FROM objects O
            INNER JOIN schemas S
                ON O.uuid = S.uuid AND O.version = S.version
            INNER JOIN object_status OS 
                ON O.uuid = OS.uuid 
                    AND O.version = OS.version
            WHERE OS.status = 'DELETE_ACCEPTED'
                { where }
            ORDER BY O.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [interfaces.NamedIdentifier(uuid.UUID(uuid_), version, name) 
            for uuid_, version, name in res]

    def _schemas_rejected(self, after: None|uuid.UUID = None):
        params: tuple = tuple()
        where = ""
        if after is not None:
            where = """AND O.id > (
                SELECT id 
                FROM objects 
                WHERE uuid = ?
                ORDER BY id 
                LIMIT 1)"""
            params += (str(after),)

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(f"""SELECT O.uuid, O.version, O.object->>'$.name'
            FROM objects O
            INNER JOIN schemas S
                ON O.uuid = S.uuid AND O.version = S.version
            INNER JOIN object_status OS 
                ON O.uuid = OS.uuid 
                    AND O.version = OS.version
            WHERE OS.status = 'CREATE_REJECTED'
                { where }
            ORDER BY O.id
            LIMIT 25""", params)

        res = cur.fetchall()
        con.close()

        return [interfaces.NamedIdentifier(uuid.UUID(uuid_), version, name) 
            for uuid_, version, name in res]

    def schema(self, name: str, version: int) -> None|events.Object:
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT O.object
                FROM schemas S
                INNER JOIN objects O
                    ON S.uuid = O.uuid AND S.version = O.version
                WHERE S.name = ?
                    AND S.version = ?""", 
            (name, version))
        res = cur.fetchone()
        con.close()

        if res is None:
            return None

        schema_json, = res
        schema_data = json.loads(schema_json)

        return events.Object.deserialize(schema_data)

    def owners(self): 
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT owner FROM owners ORDER BY id""")
        res = cur.fetchall()
        con.close()

        return [owner for owner, in res]

    def consume(self, event: events.EventT):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO events (uuid, type) VALUES (?, ?)",
            (str(event.uuid), event.__class__.__name__))
        con.commit()
        con.close()

        super().consume(event)

    def _consume_object_create(self, event: events.ObjectCreateEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""INSERT INTO objects
                (uuid, version, object)
                VALUES (?, ?, ?)""",
            (str(event.object.uuid),
                event.object.version,
                json.dumps(event.object.serialize())))

        if validators.is_schema(event.object.name):
            cur.execute("""INSERT INTO schemas
                    (name, uuid, version)
                    VALUES (?, ?, ?)""",
                (event.object.name,
                    str(event.object.uuid),
                    event.object.version))

        cur.execute("""INSERT INTO object_event_link
                (object_uuid, object_version, event_uuid)
                VALUES (?, ?, ?)""",
            (str(event.object.uuid), event.object.version, str(event.uuid)))

        cur.execute("""INSERT INTO object_status
                (uuid, version, status)
                VALUES (?, ?, ?)""",
            (str(event.object.uuid),
                event.object.version,
                StatusT.CREATE_PENDING.name))

        con.commit()
        con.close()

    def _consume_object_update(self, event: events.ObjectUpdateEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""INSERT INTO objects
                (uuid, version, object)
                VALUES (?, ?, ?)""",
            (str(event.object.uuid),
                event.object.version,
                json.dumps(event.object.serialize())))

        if validators.is_schema(event.object.name):
            cur.execute("""INSERT INTO schemas
                    (name, uuid, version)
                    VALUES (?, ?, ?)""",
                (event.object.name,
                    str(event.object.uuid),
                    event.object.version))

        cur.execute("""INSERT INTO object_event_link
                (object_uuid, object_version, event_uuid)
                VALUES (?, ?, ?)""",
            (str(event.object.uuid), event.object.version, str(event.uuid)))

        cur.execute("""INSERT INTO object_status
                (uuid, version, status)
                VALUES (?, ?, ?)""",
            (str(event.object.uuid),
                event.object.version,
                StatusT.CREATE_PENDING.name))

        con.commit()
        con.close()

    def _consume_object_delete(self, event: events.ObjectDeleteEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""INSERT INTO object_event_link
                (object_uuid, object_version, event_uuid)
                VALUES (?, ?, ?)""",
            (str(event.object_identifier.uuid),
                event.object_identifier.version,
                str(event.uuid)))

        cur.execute("""INSERT INTO object_status
                (uuid, version, status)
                VALUES (?, ?, ?)""",
            (str(event.object_identifier.uuid),
                event.object_identifier.version,
                StatusT.DELETE_PENDING.name))

        con.commit()
        con.close()

    def _consume_annotation_create(self, event: events.AnnotationCreateEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""INSERT INTO annotations
                (uuid, version, annotation)
                VALUES (?, ?, ?)""",
            (str(event.annotation.uuid),
                event.annotation.version,
                json.dumps(event.annotation.serialize())))

        for identifier in event.object_identifiers:
            cur.execute("""INSERT INTO object_annotation_link
                    (object_uuid, object_version, annotation_uuid)
                    VALUES (?, ?, ?)""",
                (str(identifier.uuid),
                    identifier.version,
                    str(event.annotation.uuid)))

        cur.execute("""INSERT INTO annotation_event_link
                (annotation_uuid, annotation_version, event_uuid)
                VALUES (?, ?, ?)""",
            (str(event.annotation.uuid),
                event.annotation.version,
                str(event.uuid)))

        cur.execute("""INSERT INTO annotation_status
                (uuid, version, status)
                VALUES (?, ?, ?)""",
            (str(event.annotation.uuid),
                event.annotation.version,
                StatusT.CREATE_PENDING.name))

        con.commit()
        con.close()

    def _consume_annotation_update(self, event: events.AnnotationUpdateEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""INSERT INTO annotations
                (uuid, version, annotation)
                VALUES (?, ?, ?)""",
            (str(event.annotation.uuid),
                event.annotation.version,
                json.dumps(event.annotation.serialize())))

        cur.execute("""INSERT INTO annotation_event_link
            (annotation_uuid, annotation_version, event_uuid)
            VALUES (?, ?, ?)""",
            (str(event.annotation.uuid),
                event.annotation.version,
                str(event.uuid)))

        cur.execute("""INSERT INTO annotation_status
            (uuid, version, status)
            VALUES (?, ?, ?)""",
            (str(event.annotation.uuid),
                event.annotation.version,
                StatusT.CREATE_PENDING.name))

        con.commit()
        con.close()

    def _consume_annotation_delete(self, event: events.AnnotationDeleteEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""INSERT INTO annotation_event_link
            (annotation_uuid, annotation_version, event_uuid)
            VALUES (?, ?, ?)""",
            (str(event.annotation_identifier.uuid),
                event.annotation_identifier.version,
                str(event.uuid)))

        cur.execute("""INSERT INTO annotation_status
            (uuid, version, status)
            VALUES (?, ?, ?)""",
            (str(event.annotation_identifier.uuid),
                event.annotation_identifier.version,
                StatusT.DELETE_PENDING.name))

        con.commit()
        con.close()

    def _consume_review_accept(self, event: events.ReviewAcceptEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""INSERT INTO event_review_link
                (event_uuid, review_uuid, accepted)
                VALUES (?, ?, ?)""",
            (str(event.event_uuid), str(event.uuid), True))

        target = self.record_keeper.read(event.event_uuid)
        if isinstance(target, events.ObjectEvent):
            if isinstance(target,
                (events.ObjectCreateEvent, events.ObjectUpdateEvent)):

                link_params = (str(target.object.uuid),
                    target.object.version,
                    str(event.uuid))

                cur.execute("""DELETE FROM object_status
                    WHERE uuid = ?
                        AND version = ?
                        AND status = ?""",
                    (str(target.object.uuid),
                        target.object.version,
                        StatusT.CREATE_PENDING.name))
            elif isinstance(target, events.ObjectDeleteEvent):
                link_params = (str(target.object_identifier.uuid),
                    target.object_identifier.version,
                    str(event.uuid))

                cur.execute("""DELETE FROM object_status
                    WHERE uuid = ?
                        AND version = ?
                        AND status = ?""",
                    (str(target.object_identifier.uuid),
                        target.object_identifier.version,
                        StatusT.DELETE_PENDING.name))

                cur.execute("""INSERT INTO object_status
                    (uuid, version, status)
                    VALUES (?, ?, ?)""",
                    (str(target.object_identifier.uuid),
                        target.object_identifier.version,
                        StatusT.DELETE_ACCEPTED.name))
            else:
                raise ValueError("unexpected object event type in accept")

            cur.execute("""INSERT INTO object_event_link
                (object_uuid, object_version, event_uuid)
                VALUES (?, ?, ?)""", link_params)
        elif isinstance(target, events.AnnotationEvent):
            if isinstance(target,
                (events.AnnotationCreateEvent, events.AnnotationUpdateEvent)):

                link_params = (str(target.annotation.uuid),
                    target.annotation.version,
                    str(event.uuid))

                cur.execute("""DELETE FROM annotation_status
                    WHERE uuid = ?
                        AND version = ?
                        AND status = ?""",
                    (str(target.annotation.uuid),
                        target.annotation.version,
                        StatusT.CREATE_PENDING.name))
            elif isinstance(target, events.AnnotationDeleteEvent):
                link_params = (str(target.annotation_identifier.uuid),
                    target.annotation_identifier.version,
                    str(event.uuid))

                cur.execute("""DELETE FROM annotation_status
                    WHERE uuid = ?
                        AND version = ?
                        AND status = ?""",
                    (str(target.annotation_identifier.uuid),
                        target.annotation_identifier.version,
                        StatusT.DELETE_PENDING.name))

                cur.execute("""INSERT INTO annotation_status
                    (uuid, version, status)
                    VALUES (?, ?, ?)""",
                    (str(target.annotation_identifier.uuid),
                        target.annotation_identifier.version,
                        StatusT.DELETE_ACCEPTED.name))
            else:
                raise ValueError("unexpected annotation event type in accept")

            cur.execute("""INSERT INTO annotation_event_link
                (annotation_uuid, annotation_version, event_uuid)
                VALUES (?, ?, ?)""", link_params)
        else:
            raise ValueError("unexpected event type in accept")
        
        con.commit()
        con.close()

    def _consume_review_reject(self, event: events.ReviewRejectEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""INSERT INTO event_review_link
            (event_uuid, review_uuid, accepted)
            VALUES (?, ?, ?)""",
            (str(event.event_uuid), str(event.uuid), False))

        target = self.record_keeper.read(event.event_uuid)
        if isinstance(target, events.ObjectEvent):
            if isinstance(target,
                (events.ObjectCreateEvent, events.ObjectUpdateEvent)):

                link_params = (str(target.object.uuid),
                    target.object.version,
                    str(event.uuid))

                cur.execute("""DELETE FROM object_status
                    WHERE uuid = ?
                        AND version = ?
                        AND status = ?""",
                    (str(target.object.uuid),
                        target.object.version,
                        StatusT.CREATE_PENDING.name))

                cur.execute("""INSERT INTO object_status
                    (uuid, version, status)
                    VALUES (?, ?, ?)""",
                    (str(target.object.uuid),
                        target.object.version,
                        StatusT.CREATE_REJECTED.name))
            elif isinstance(target, events.ObjectDeleteEvent):
                link_params = (str(target.object_identifier.uuid),
                    target.object_identifier.version,
                    str(event.uuid))

                cur.execute("""DELETE FROM object_status
                    WHERE uuid = ?
                        AND version = ?
                        AND status = ?""",
                    (str(target.object_identifier.uuid),
                        target.object_identifier.version,
                        StatusT.DELETE_PENDING.name))
            else:
                raise ValueError("unexpected object event type in reject")

            cur.execute("""INSERT INTO object_event_link
                (object_uuid, object_version, event_uuid)
                VALUES (?, ?, ?)""", link_params)
        elif isinstance(target, events.AnnotationEvent):
            if isinstance(target, (
                events.AnnotationCreateEvent, events.AnnotationUpdateEvent)):

                link_params = (str(target.annotation.uuid),
                    target.annotation.version,
                    str(event.uuid))

                cur.execute("""DELETE FROM annotation_status
                    WHERE uuid = ?
                        AND version = ?
                        AND status = ?""",
                    (str(target.annotation.uuid),
                        target.annotation.version,
                        StatusT.CREATE_PENDING.name))

                cur.execute("""INSERT INTO annotation_status
                    (uuid, version, status)
                    VALUES (?, ?, ?)""",
                    (str(target.annotation.uuid),
                        target.annotation.version,
                        StatusT.CREATE_REJECTED.name))
            elif isinstance(target, events.AnnotationDeleteEvent):
                link_params = (str(target.annotation_identifier.uuid),
                    target.annotation_identifier.version,
                    str(event.uuid))

                cur.execute("""DELETE FROM annotation_status
                    WHERE uuid = ?
                        AND version = ?
                        AND status = ?""",
                    (str(target.annotation_identifier.uuid),
                        target.annotation_identifier.version,
                        StatusT.DELETE_PENDING.name))
            else:
                raise ValueError("unexpected annotation event type in reject")

            cur.execute("""INSERT INTO annotation_event_link
                (annotation_uuid, annotation_version, event_uuid)
                VALUES (?, ?, ?)""", link_params)
        else:
            raise ValueError("unexpected event type in reject")

        con.commit()
        con.close()

    def _consume_owner_add(self, event: events.OwnerAddEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""INSERT INTO owners (owner) VALUES (?)""",
            (event.owner,))

        con.commit()
        con.close()

    def _consume_owner_remove(self, event: events.OwnerRemoveEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""DELETE FROM owners WHERE owner = ?""",
            (event.owner,))

        con.commit()
        con.close()

    def _validate_object_create(self, event: events.ObjectCreateEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        if validators.is_schema(event.object.name):
            cur.execute("""SELECT COUNT(*) FROM schemas WHERE name = ?""",
                (event.object.name,))
            count, = cur.fetchone()
            if count != 0:
                con.close()
                raise exceptions.ValidationError("schema name already in use")

        cur.execute("""SELECT COUNT(*)
                FROM objects
                WHERE uuid = ?""",
            (str(event.object.uuid),))

        count, = cur.fetchone()
        if count != 0:
            con.close()
            raise exceptions.ValidationError("object with UUID already exists")

        cur.execute("""SELECT uuid, version
                FROM objects
                WHERE object->>'$.hash' = ?
                    AND uuid != ?""",
            (event.object.hash, str(event.object.uuid)))

        res = cur.fetchone()
        con.close()

        if res is not None:
            dup_uuid, dup_version = res
            raise exceptions.ValidationError(
                f"duplicate hash detected in object {dup_uuid}:{dup_version}")

        if event.object.version != 0:
            raise exceptions.ValidationError(
                "object version must be zero in create event")

    def _validate_object_update(self, event: events.ObjectCreateEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT uuid, version
                FROM objects
                WHERE object->>'$.hash' = ?
                    AND uuid != ?""",
            (event.object.hash, str(event.object.uuid)))

        res = cur.fetchone()

        if res is not None:
            con.close()
            dup_uuid, dup_version = res
            raise exceptions.ValidationError(
                f"duplicate hash detected in object {dup_uuid}:{dup_version}")

        cur.execute("""SELECT object
                FROM objects
                WHERE uuid = ?""",
            (str(event.object.uuid),))

        versions_json = cur.fetchall()
        con.close()
        if len(versions_json) == 0:
            raise exceptions.ValidationError("no objects with UUID found")

        versions_data = [json.loads(ea) for ea, in versions_json]

        versions = [events.Object.deserialize(ea) for ea in versions_data]

        if validators.is_schema(versions[-1].name):
            if versions[-1].name != event.object.name:
                raise exceptions.ValidationError("schema names may not change")
        else:
            if validators.is_schema(event.object.name):
                raise exceptions.ValidationError("object may not become schema")

        if versions[-1].hash == event.object.hash:
            raise exceptions.ValidationError("object hash unchanged")

        if event.object.version != len(versions):
            raise exceptions.ValidationError(
                f"object version should be {len(versions)}")

    def _validate_object_delete(self, event: events.ObjectDeleteEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT COUNT(*)
                FROM objects
                WHERE uuid = ?
                    AND version = ?""",
            (str(event.object_identifier.uuid),
                event.object_identifier.version))

        count, = cur.fetchone()
        if count == 0:
            con.close()
            raise exceptions.ValidationError("object identifier not found")

        cur.execute("""SELECT status
                FROM object_status
                WHERE uuid = ?
                    AND version = ?""",
            (str(event.object_identifier.uuid),
                event.object_identifier.version))

        status = {getattr(StatusT, ea) for ea, in cur.fetchall()}
        con.close()
        if StatusT.CREATE_REJECTED in status:
            raise exceptions.ValidationError("cannot delete a rejected object")

        if StatusT.DELETE_PENDING in status:
            raise exceptions.ValidationError("object version pending deletion")

        if StatusT.DELETE_ACCEPTED in status:
            raise exceptions.ValidationError("object version already deleted")

    def _validate_annotation_create(self, event: events.AnnotationCreateEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT COUNT(*)
                FROM annotations
                WHERE uuid = ?""",
            (str(event.annotation.uuid),))

        count, = cur.fetchone()
        if count != 0:
            con.close()
            raise exceptions.ValidationError(
                "annotation with UUID already exists")

        if event.annotation.version != 0:
            con.close()
            raise exceptions.ValidationError(
                "annotation version must be zero in create event")

        for identifier in event.object_identifiers:
            cur.execute("""SELECT COUNT(*)
                    FROM objects
                    WHERE uuid = ?
                        AND version = ?""",
                (str(identifier.uuid), identifier.version))
            count, = cur.fetchone()
            if count == 0:
                con.close()
                raise exceptions.ValidationError("object identifier not found")

            cur.execute("""SELECT status
                FROM object_status
                WHERE uuid = ?
                    AND version = ?""",
                (str(identifier.uuid), identifier.version))

            status = {getattr(StatusT, ea) for ea, in cur.fetchall()}
            if StatusT.CREATE_REJECTED in status:
                con.close()
                raise exceptions.ValidationError(
                    "rejected objects cannot be annotated")

            if StatusT.DELETE_ACCEPTED in status:
                con.close()
                raise exceptions.ValidationError(
                    "deleted objects cannot be annotated")

            cur.execute("""SELECT COUNT(*) FROM schemas WHERE uuid = ?""",
                (str(identifier.uuid),))
            count, = cur.fetchone()
            con.close()
            if count != 0:
                raise exceptions.ValidationError("schemas can not be annotated")

    def _validate_annotation_update(self, event: events.AnnotationUpdateEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT uuid, version
                FROM annotations
                WHERE uuid = ?""",
            (str(event.annotation.uuid),))

        version_ids = cur.fetchall()
        con.close()
        if len(version_ids) == 0:
            raise exceptions.ValidationError("no annotations with UUID found")

        if event.annotation.version != len(version_ids):
            raise exceptions.ValidationError(
                f"annotation version should be {len(version_ids)}.")

    def _validate_annotation_delete(self, event: events.AnnotationDeleteEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT COUNT(*)
                FROM annotations
                WHERE uuid = ?
                    AND version = ?""",
            (str(event.annotation_identifier.uuid),
                event.annotation_identifier.version))

        count, = cur.fetchone()
        if count == 0:
            con.close()
            raise exceptions.ValidationError("annotation identifier not found")

        cur.execute("""SELECT status
                FROM annotation_status
                WHERE uuid = ?
                    AND version = ?""",
            (str(event.annotation_identifier.uuid),
                event.annotation_identifier.version))

        status = {getattr(StatusT, ea) for ea, in cur.fetchall()}
        con.close()
        if StatusT.CREATE_REJECTED in status:
            raise exceptions.ValidationError(
                "cannot delete a rejected annotation")

        if StatusT.DELETE_PENDING in status:
            raise exceptions.ValidationError(
                "annotation already pending deletion")

        if StatusT.DELETE_ACCEPTED in status:
            raise exceptions.ValidationError("annotation already deleted")

    def _validate_review(self,
        event: events.ReviewAcceptEvent|events.ReviewRejectEvent):

        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT COUNT(*)
                FROM event_review_link
                WHERE event_uuid = ?""",
            (str(event.event_uuid),))

        count, = cur.fetchone()
        if count != 0:
            con.close()            
            raise exceptions.ValidationError("event already reviewed")

        if not self.record_keeper.exists(event.event_uuid):
            con.close()            
            raise exceptions.ValidationError("no events with event UUID found")

        target_event = self.record_keeper.read(event.event_uuid)
        if not isinstance(target_event,
            (events.AnnotationEvent, events.ObjectEvent)):
            con.close()        
            raise exceptions.ValidationError(
                "review on non object or annotation event")

        if event.author is None:
            con.close()            
            raise exceptions.ValidationError("author is empty")

        cur.execute("""SELECT COUNT(*)
            FROM owners
            WHERE owner = ?""",
            (event.author,))
        count, = cur.fetchone()
        con.close()
        if count == 0:
            raise exceptions.ValidationError("review event from non-owner")

    def _validate_review_accept(self, event: events.ReviewAcceptEvent):
        self._validate_review(event)

    def _validate_review_reject(self, event: events.ReviewRejectEvent):
        self._validate_review(event)

    def _validate_owner_add(self, event: events.OwnerAddEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT owner FROM owners""")

        owners = {owner for owner, in cur.fetchall()}
        con.close()
        if len(owners) > 0:
            if event.owner in owners:
                raise exceptions.ValidationError("owner already present")

            if event.author not in owners:
                raise exceptions.ValidationError("only owners can add owners")
        else:
            if event.owner != event.author:
                raise exceptions.ValidationError(
                    "first owner add event must be self signed")

    def _validate_owner_remove(self, event: events.OwnerRemoveEvent):
        con = sqlite3.connect(self.database_path)
        cur = con.cursor()
        cur.execute("""SELECT id, owner FROM owners""")

        owners = cur.fetchall()
        con.close()
        if len(owners) == 0:
            raise exceptions.ValidationError("dataset has no owners to remove")

        ranks = {owner: id_ for id_, owner in owners}

        if event.author not in ranks:
            raise exceptions.ValidationError("only owners may remove owners")

        if event.owner not in ranks:
            raise exceptions.ValidationError("target key is not an owner")

        if len(ranks) == 1:
            raise exceptions.ValidationError(
                "removing owner would leave the dataset ownerless")

        target_rank = ranks[event.owner]
        signer_rank = ranks[event.author]

        if signer_rank > target_rank:
            raise exceptions.ValidationError(
                "cannot remove a higher ranking owner")
