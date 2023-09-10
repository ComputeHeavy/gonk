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
        cur.execute("""CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid text NOT NULL,
            event JSON NOT NULL
        )""")

    def add(self, event: core.EventT):
        event_data = event.serialize()
        event_data["type"] = event.__class__.__name__

        event_json = json.dumps(event_data)
        cur = self.con.cursor()
        cur.execute(
            "INSERT INTO events (uuid, event) VALUES (?, ?)",
            (event_data["uuid"], event_json))

    def read(self, uuid_: uuid.UUID) -> core.Event:
        cur = self.con.cursor()
        cur.execute("SELECT event FROM events WHERE uuid = ?", (str(uuid_),))

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

    def next(self, uuid_: uuid.UUID | None=None) -> uuid.UUID | None:
        cur = self.con.cursor()
        if uuid_ is None:
            cur.execute("SELECT uuid FROM events ORDER BY id LIMIT 1")
            res = cur.fetchone()

            if res is None:
                return None

            next_, = res

            return uuid.UUID(next_)

        cur.execute("SELECT id FROM events WHERE uuid = ?", (str(uuid_),))
        res = cur.fetchone()
        
        if res is None:
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
            return None

        next_, = res
        return uuid.UUID(next_)

class State(core.State):
    def __init__(self, 
        parent_directory: pathlib.Path, 
        record_keeper: core.RecordKeeper):

        super().__init__()
        if not parent_directory.exists():
            raise ValueError("parent directory does not exist")

        self.record_keeper = record_keeper

        self.database_path = parent_directory.joinpath("state.db")
        self.con = sqlite3.connect(self.database_path)

        cur = self.con.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS event_review_link (
                event_uuid TEXT NOT NULL,
                review_uuid TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS objects (
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
                public_key TEXT NOT NULL
            );
        """)

    def _consume_object_create(self, event: core.ObjectCreateEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO objects 
                (uuid, version, object) 
                VALUES (?, ?, ?)""",
            (str(event.object.uuid), 
                event.object.version,
                json.dumps(event.object.serialize())))

        if core.is_schema(event.object.name):
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
                core.StatusT.CREATE_PENDING.name))

    def _consume_object_update(self, event: core.ObjectUpdateEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO objects 
                (uuid, version, object) 
                VALUES (?, ?, ?)""",
            (str(event.object.uuid), 
                event.object.version,
                json.dumps(event.object.serialize())))

        if core.is_schema(event.object.name):
            cur.execute("""INSERT INTO schemas 
                    (name, uuid, version) 
                    VALUES (?, ?, ?)""",
                (event.object.name, 
                    str(event.object.uuid), 
                    event.object.version))   

        cur.execute("""INSERT INTO object_event_link 
                (object_uuid, object_version, event_uuid) 
                VALUES (?, ?, ?)""", 
            (str(event.object.uuid), event.object.version, event.uuid))
        
        cur.execute("""INSERT INTO object_status 
                (uuid, version, status) 
                VALUES (?, ?, ?)""", 
            (str(event.object.uuid), 
                event.object.version, 
                core.StatusT.CREATE_PENDING.name))

    def _consume_object_delete(self, event: core.ObjectDeleteEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO object_event_link 
                (object_uuid, object_version, event_uuid) 
                VALUES (?, ?, ?)""", 
            (str(event.object_identifier.uuid), 
                event.object_identifier.version, 
                event.uuid))
        
        cur.execute("""INSERT INTO object_status 
                (uuid, version, status) 
                VALUES (?, ?, ?)""", 
            (str(event.object_identifier.uuid), 
                event.object_identifier.version, 
                core.StatusT.DELETE_PENDING.name))

    def _consume_annotation_create(self, event: core.AnnotationCreateEvent):
        cur = self.con.cursor()
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
                    event.annotation.uuid))

        cur.execute("""INSERT INTO annotation_event_link 
                (annotation_uuid, annotation_version, event_uuid) 
                VALUES (?, ?, ?)""", 
            (str(event.annotation.uuid), event.annotation.version, event.uuid))

        cur.execute("""INSERT INTO annotation_status 
                (uuid, version, status) 
                VALUES (?, ?, ?)""", 
            (str(event.annotation.uuid), 
                event.annotation.version, 
                core.StatusT.CREATE_PENDING.name))

    def _consume_annotation_update(self, event: core.AnnotationUpdateEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO annotations 
                (uuid, version, annotation) 
                VALUES (?, ?, ?)""",
            (str(event.annotation.uuid), 
                event.annotation.version,
                json.dumps(event.annotation.serialize())))

        cur.execute("""INSERT INTO annotation_event_link 
            (annotation_uuid, annotation_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (str(event.annotation.uuid), event.annotation.version, event.uuid))

        cur.execute("""INSERT INTO annotation_status 
            (uuid, version, status) 
            VALUES (?, ?, ?)""", 
            (str(event.annotation.uuid), 
                event.annotation.version, 
                core.StatusT.CREATE_PENDING.name))

    def _consume_annotation_delete(self, event: core.AnnotationDeleteEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO annotation_event_link 
            (annotation_uuid, annotation_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (str(event.annotation_identifier.uuid), 
                event.annotation_identifier.version, 
                event.uuid))
        
        cur.execute("""INSERT INTO annotation_status 
            (uuid, version, status) 
            VALUES (?, ?, ?)""", 
            (str(event.annotation_identifier.uuid), 
                event.annotation_identifier.version, 
                core.StatusT.DELETE_PENDING.name))

    def _consume_review_accept(self, event: core.ReviewAcceptEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO event_review_link 
                (event_uuid, review_uuid) 
                VALUES (?, ?)""", 
            (str(event.event_uuid), str(event.uuid)))

        target = self.record_keeper.read(event.event_uuid)
        if isinstance(target, core.ObjectEvent):
            if isinstance(target, 
                (core.ObjectCreateEvent, core.ObjectUpdateEvent)):

                link_params = (str(target.object.uuid), 
                    target.object.version, 
                    str(event.uuid))

                cur.execute("""DELETE FROM object_status 
                    WHERE uuid = ? 
                        AND version = ? 
                        AND status = ?""", 
                    (str(target.object.uuid), 
                        target.object.version, 
                        core.StatusT.CREATE_PENDING.name))
            elif isinstance(target, core.ObjectDeleteEvent):
                link_parmas = (str(target.object_identifier.uuid), 
                    target.object_identifier.version, 
                    str(event.uuid))

                cur.execute("""DELETE FROM object_status 
                    WHERE uuid = ? 
                        AND version = ? 
                        AND status = ?""", 
                    (str(target.object_identifier.uuid), 
                        target.object_identifier.version, 
                        core.StatusT.DELETE_PENDING.name))

                cur.execute("""INSERT INTO object_status 
                    (uuid, version, status) 
                    VALUES (?, ?, ?)""", 
                    (str(target.object_identifier.uuid), 
                        target.object_identifier.version, 
                        core.StatusT.DELETE_ACCEPTED.name))
            else:
                raise ValueError("unexpected object event type in accept")

            cur.execute("""INSERT INTO object_event_link 
                (object_uuid, object_version, event_uuid) 
                VALUES (?, ?, ?)""", link_params)
        elif isinstance(target, core.AnnotationEvent):
            if isinstance(target, 
                (core.AnnotationCreateEvent, core.AnnotationUpdateEvent)):

                link_params = (str(target.annotation.uuid), 
                    target.annotation.version, 
                    str(event.uuid))

                cur.execute("""DELETE FROM annotation_status 
                    WHERE uuid = ? 
                        AND version = ? 
                        AND status = ?""", 
                    (str(target.annotation.uuid), 
                        target.annotation.version, 
                        core.StatusT.CREATE_PENDING.name))
            elif isinstance(target, core.AnnotationDeleteEvent):
                link_params = (str(target.annotation_identifier.uuid), 
                    target.annotation_identifier.version, 
                    str(event.uuid))

                cur.execute("""DELETE FROM annotation_status 
                    WHERE uuid = ? 
                        AND version = ? 
                        AND status = ?""", 
                    (str(target.annotation_identifier.uuid), 
                        target.annotation_identifier.version, 
                        core.StatusT.DELETE_PENDING.name))

                cur.execute("""INSERT INTO annotation_status 
                    (uuid, version, status) 
                    VALUES (?, ?, ?)""", 
                    (str(target.annotation_identifier.uuid), 
                        target.annotation_identifier.version, 
                        core.StatusT.DELETE_ACCEPTED.name))
            else:
                raise ValueError("unexpected annotation event type in accept")

            cur.execute("""INSERT INTO annotation_event_link 
                (annotation_uuid, annotation_version, event_uuid) 
                VALUES (?, ?, ?)""", link_params)
        else:
            raise ValueError("unexpected event type in accept")

    def _consume_review_reject(self, event: core.ReviewRejectEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO event_review_link 
            (event_uuid, review_uuid) 
            VALUES (?, ?)""", 
            (event.event_uuid, str(event.uuid)))

        target = self.record_keeper.read(event.event_uuid)
        if isinstance(target, core.ObjectEvent):
            if isinstance(target, 
                (core.ObjectCreateEvent, core.ObjectUpdateEvent)):

                link_params = (str(target.object.uuid), 
                    target.object.version, 
                    str(event.uuid))

                cur.execute("""DELETE FROM object_status 
                    WHERE uuid = ? 
                        AND version = ? 
                        AND status = ?""", 
                    (str(target.object.uuid), 
                        target.object.version, 
                        core.StatusT.CREATE_PENDING.name))

                cur.execute("""INSERT INTO object_status 
                    (uuid, version, status) 
                    VALUES (?, ?, ?)""", 
                    (str(target.object.uuid), 
                        target.object.version, 
                        core.StatusT.CREATE_REJECTED.name))
            elif isinstance(target, core.ObjectDeleteEvent):
                link_parmas = (str(target.object_identifier.uuid), 
                    target.object_identifier.version, 
                    str(event.uuid))

                cur.execute("""DELETE FROM object_status 
                    WHERE uuid = ? 
                        AND version = ? 
                        AND status = ?""", 
                    (str(target.object_identifier.uuid), 
                        target.object_identifier.version, 
                        core.StatusT.DELETE_PENDING.name))
            else:
                raise ValueError("unexpected object event type in reject")

            cur.execute("""INSERT INTO object_event_link 
                (object_uuid, object_version, event_uuid) 
                VALUES (?, ?, ?)""", link_params)
        elif isinstance(target, core.AnnotationEvent):
            if isinstance(target, (
                core.AnnotationCreateEvent, core.AnnotationUpdateEvent)):

                link_params = (str(target.annotation.uuid), 
                    target.annotation.version, 
                    str(event.uuid))

                cur.execute("""DELETE FROM annotation_status 
                    WHERE uuid = ? 
                        AND version = ? 
                        AND status = ?""", 
                    (str(target.annotation.uuid), 
                        target.annotation.version, 
                        core.StatusT.CREATE_PENDING.name))

                cur.execute("""INSERT INTO annotation_status 
                    (uuid, version, status) 
                    VALUES (?, ?, ?)""", 
                    (str(target.annotation.uuid), 
                        target.annotation.version, 
                        core.StatusT.CREATE_REJECTED.name))
            elif isinstance(target, core.AnnotationDeleteEvent):
                link_params = (str(target.annotation_identifier.uuid), 
                    target.annotation_identifier.version, 
                    str(event.uuid))

                cur.execute("""DELETE FROM annotation_status 
                    WHERE uuid = ? 
                        AND version = ? 
                        AND status = ?""", 
                    (str(target.annotation_identifier.uuid), 
                        target.annotation_identifier.version, 
                        core.StatusT.DELETE_PENDING.name))
            else:
                raise ValueError("unexpected annotation event type in reject")

            cur.execute("""INSERT INTO annotation_event_link 
                (annotation_uuid, annotation_version, event_uuid) 
                VALUES (?, ?, ?)""", link_params)
        else:
            raise ValueError("unexpected event type in reject")

    def _consume_owner_add(self, event: core.OwnerAddEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO owners (public_key) VALUES (?)""",
            (event.public_key.hex(),))

    def _consume_owner_remove(self, event: core.OwnerRemoveEvent):
        cur = self.con.cursor()
        cur.execute("""DELETE FROM owners WHERE public_key = ?""",
            (event.public_key.hex(),))

    def _validate_object_create(self, event: core.ObjectCreateEvent):
        cur = self.con.cursor()
        if core.is_schema(event.object.name):
            cur.execute("""SELECT COUNT(*) FROM schemas WHERE name = ?""",
                (event.object.name,))
            count, = cur.fetchone()
            if count != 0:
                raise core.ValidationError("schema name already in use")

        cur.execute("""SELECT COUNT(*) 
                FROM objects 
                WHERE uuid = ?""",
            (str(event.object.uuid),))

        count, = cur.fetchone()
        if count != 0:
            raise core.ValidationError("object with UUID already exists")

        if event.object.version != 0:
            raise core.ValidationError(
                "object version must be zero in create event")

    def _validate_object_update(self, event: core.ObjectCreateEvent):
        cur = self.con.cursor()
        cur.execute("""SELECT object 
                FROM objects 
                WHERE uuid = ?""", 
            (str(event.object.uuid),))

        versions_json = cur.fetchall()
        if len(versions_json) == 0:
            raise core.ValidationError("no objects with UUID found")

        versions_data = [json.loads(ea) for ea, in versions_json]
        
        versions = [core.Object.deserialize(ea) for ea in versions_data]

        if core.is_schema(versions[-1].name):
            if versions[-1].name != event.object.name:
                raise core.ValidationError("schema names may not change")
        else:
            if core.is_schema(event.object.name):
                raise core.ValidationError("object may not become schema")

        if event.object.version != len(versions):
            raise core.ValidationError(
                f"object version should be {len(versions)}")

    def _validate_object_delete(self, event: core.ObjectDeleteEvent):
        cur = self.con.cursor()
        cur.execute("""SELECT COUNT(*) FROM schemas WHERE uuid = ?""",
            (str(event.object_identifier.uuid),))
        
        count, = cur.fetchone()
        if count != 0:
            raise core.ValidationError("schemas can not be deleted")

        cur.execute("""SELECT COUNT(*)
                FROM objects 
                WHERE uuid = ? 
                    AND version = ?""", 
            (str(event.object_identifier.uuid), 
                event.object_identifier.version))

        count, = cur.fetchone()
        if count == 0:
            raise core.ValidationError("object identifier not found")

        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(event.object_identifier.uuid), 
                event.object_identifier.version))

        status = set([getattr(core.StatusT, ea) for ea, in cur.fetchall()])
        if core.StatusT.CREATE_REJECTED in status:
            raise core.ValidationError("cannot delete a rejected object")

        if core.StatusT.DELETE_PENDING in status:
            raise core.ValidationError("object version pending deletion")

        if core.StatusT.DELETE_ACCEPTED in status:
            raise core.ValidationError("object version already deleted")

    def _validate_annotation_create(self, event: core.AnnotationCreateEvent):
        cur = self.con.cursor()
        cur.execute("""SELECT COUNT(*) 
                FROM annotations 
                WHERE uuid = ?""",
            (str(event.annotation.uuid),))

        count, = cur.fetchone()
        if count != 0:
            raise core.ValidationError("annotation with UUID already exists")

        if event.annotation.version != 0:
            raise core.ValidationError(
                "annotation version must be zero in create event")

        for identifier in event.object_identifiers:
            cur.execute("""SELECT COUNT(*)
                    FROM objects 
                    WHERE uuid = ?
                        AND version = ?""", 
                (str(identifier.uuid), identifier.version))
            count, = cur.fetchone()
            if count == 0:
                raise core.ValidationError("object identifier not found")

            cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
                (str(identifier.uuid), identifier.version))

            status = set([getattr(core.StatusT, ea) for ea, in cur.fetchall()])
            if core.StatusT.CREATE_REJECTED in status:
                raise core.ValidationError(
                    "rejected objects cannot be annotated")

            if core.StatusT.DELETE_ACCEPTED in status:
                raise core.ValidationError(
                    "deleted objects cannot be annotated")

            cur.execute("""SELECT COUNT(*) FROM schemas WHERE uuid = ?""",
                (str(identifier.uuid),))
            count, = cur.fetchone()
            if count != 0:
                raise core.ValidationError("schemas can not be deleted")

    def _validate_annotation_update(self, event: core.AnnotationUpdateEvent):
        cur = self.con.cursor()
        cur.execute("""SELECT uuid, version 
                FROM annotations
                WHERE uuid = ?""", 
            (str(event.annotation.uuid),))

        version_ids = cur.fetchall()
        if len(version_ids) == 0:
            raise core.ValidationError("no annotations with UUID found")

        if event.annotation.version != len(version_ids):
            raise core.ValidationError(
                f"annotation version should be {len(version_ids)}.")

    def _validate_annotation_delete(self, event: core.AnnotationDeleteEvent):
        cur = self.con.cursor()
        cur.execute("""SELECT COUNT(*)
                FROM annotations 
                WHERE uuid = ?
                    AND version = ?""", 
            (str(event.annotation_identifier.uuid), 
                event.annotation_identifier.version))

        count, = cur.fetchone()
        if count == 0:
            raise core.ValidationError("annotation identifier not found")

        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(event.annotation_identifier.uuid), 
                event.annotation_identifier.version))

        status = set([getattr(core.StatusT, ea) for ea, in cur.fetchall()])
        if core.StatusT.CREATE_REJECTED in status:
            raise core.ValidationError("cannot delete a rejected annotation")

        if core.StatusT.DELETE_PENDING in status:
            raise core.ValidationError("annotation already pending deletion")

        if core.StatusT.DELETE_ACCEPTED in status:
            raise core.ValidationError("annotation already deleted")

    def _validate_review(self, 
        event: core.ReviewAcceptEvent|core.ReviewRejectEvent):

        cur = self.con.cursor()
        cur.execute("""SELECT COUNT(*)
                FROM event_review_link 
                WHERE event_uuid = ?""", 
            (str(event.event_uuid),))

        count, = cur.fetchone()
        if count != 0:
            raise core.ValidationError("event already reviewed")

        if not self.record_keeper.exists(event.event_uuid):
            raise core.ValidationError("no events with event UUID found")
    
        target_event = self.record_keeper.read(event.event_uuid)
        if not isinstance(target_event, 
            (core.AnnotationEvent, core.ObjectEvent)):
            raise core.ValidationError(
                "review on non object or annotation event")

        if event.signer is None:
            raise core.ValidationError("signer is empty")

        cur.execute("""SELECT COUNT(*) 
            FROM owners 
            WHERE public_key = ?""",
            (event.signer.hex(),))
        count, = cur.fetchone()
        if count == 0:
            raise core.ValidationError("review event from non-owner")

    def _validate_review_accept(self, event: core.ReviewAcceptEvent):
        self._validate_review(event)

    def _validate_review_reject(self, event: core.ReviewRejectEvent):
        self._validate_review(event)
        
    def _validate_owner_add(self, event: core.OwnerAddEvent):
        cur = self.con.cursor()
        cur.execute("""SELECT public_key FROM owners""")

        owners = {public_key for public_key, in cur.fetchall()}
        if len(owners) > 0:
            if event.public_key in owners:
                raise core.ValidationError("owner already present")

            if event.signer not in owners:
                raise core.ValidationError("only owners can add owners")
        else:
            if event.public_key != event.signer:
                raise core.ValidationError(
                    "first owner add event must be self signed")

    def _validate_owner_remove(self, event: core.OwnerRemoveEvent):
        cur = self.con.cursor()
        cur.execute("""SELECT id, public_key FROM owners""")

        owners = cur.fetchall()
        if len(owners) == 0:
            raise core.ValidationError("dataset has no owners to remove")

        ranks = {bytes.fromhex(public_key): id_ for id_, public_key in owners}

        if event.signer not in ranks:
            raise core.ValidationError("only owners may remove owners")

        if event.public_key not in ranks:
            raise core.ValidationError("target key is not an owner")
        
        if len(ranks) == 1:
            raise core.ValidationError(
                "removing owner would leave the dataset ownerless")

        target_rank = ranks[event.public_key]
        signer_rank = ranks[event.signer]

        if signer_rank > target_rank:
            raise core.ValidationError("cannot remove a higher ranking owner")
