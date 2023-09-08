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

class State:
    def __init__(self, parent_directory: pathlib.Path):
        super().__init__()
        if not parent_directory.exists():
            raise ValueError("parent directory does not exist")

        self.database_path = parent_directory.joinpath("state.db")
        self.con = sqlite3.connect(self.database_path)

        cur = self.con.cursor()
        cur.executescript('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL,
                event JSON NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event_review_link (
                event_uuid TEXT NOT NULL,
                review_uuid TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS schemas (
                name TEXT NOT NULL,
                uuid TEXT NOT NULL,
                version INTEGER NOT NULL,
            );

            CREATE TABLE IF NOT EXISTS object_event_link (
                object_uuid TEXT NOT NULL,
                object_version INTEGER NOT NULL,
                event_uuid TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS object_status (
                object_uuid TEXT NOT NULL,
                object_version INTEGER NOT NULL,
                status TEXT NOT NULL
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
                annotation_uuid TEXT NOT NULL,
                annotation_version INTEGER NOT NULL,
                status TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS owners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                public_key TEXT NOT NULL
            );
        ''')

    def _store_event(self, event: core.EventT):
        cur = self.con.cursor()
        cur.execute(
            '''INSERT INTO events (uuid, event) VALUES (?, ?)''',
            (str(event.uuid), json.dumps(event.serialize())))

    def consume(self, event: core.EventT):
        super().consume(event)
        self._store_event(event)

    def _consume_object_create(self, event: ObjectCreateEvent):
        cur = self.con.cursor()
        if core.is_schema(event.object.name):
            cur.execute(
                "INSERT INTO schemas (name, uuid, version) VALUES (?, ?, ?)",
                (event.object.name, event.object.uuid, event.object.version))   

        cur.execute("""INSERT INTO object_event_link 
            (object_uuid, object_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (event.object.uuid, event.object.version, event.uuid))
        
        cur.execute("""INSERT INTO object_status 
            (object_uuid, object_version, status) 
            VALUES (?, ?, ?)""", 
            (event.object.uuid, 
                event.object.version, 
                StatusT.CREATE_PENDING.name))

    def _consume_object_update(self, event: ObjectUpdateEvent):
        cur = self.con.cursor()
        if core.is_schema(event.object.name):
            cur.execute(
                "INSERT INTO schemas (name, uuid, version) VALUES (?, ?, ?)",
                (event.object.name, event.object.uuid, event.object.version))   

        cur.execute("""INSERT INTO object_event_link 
            (object_uuid, object_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (event.object.uuid, event.object.version, event.uuid))
        
        cur.execute("""INSERT INTO object_status 
            (object_uuid, object_version, status) 
            VALUES (?, ?, ?)""", 
            (event.object.uuid, 
                event.object.version, 
                StatusT.CREATE_PENDING.name))

    def _consume_object_delete(self, event: ObjectDeleteEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO object_event_link 
            (object_uuid, object_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (event.object_identifier.uuid, 
                event.object_identifier.version, 
                event.uuid))
        
        cur.execute("""INSERT INTO object_status 
            (object_uuid, object_version, status) 
            VALUES (?, ?, ?)""", 
            (event.object_identifier.uuid, 
                event.object_identifier.version, 
                StatusT.DELETE_PENDING.name))

    def _consume_annotation_create(self, event: AnnotationCreateEvent):
        cur = self.con.cursor()
        for identifier in event.object_identifiers:
            cur.execute("""INSERT INTO object_annotation_link 
                (object_uuid, object_version, annotation_uuid) 
                VALUES (?, ?, ?)""", 
                (identifier.uuid, 
                    identifier.version, 
                    event.annotation.uuid))

        cur.execute("""INSERT INTO annotation_event_link 
            (annotation_uuid, annotation_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (event.annotation.uuid, event.annotation.version, event.uuid))

        cur.execute("""INSERT INTO annotation_status 
            (annotation_uuid, annotation_version, status) 
            VALUES (?, ?, ?)""", 
            (event.annotation.uuid, 
                event.annotation.version, 
                StatusT.CREATE_PENDING.name))

    def _consume_annotation_update(self, event: AnnotationUpdateEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO annotation_event_link 
            (annotation_uuid, annotation_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (event.annotation.uuid, event.annotation.version, event.uuid))

        cur.execute("""INSERT INTO annotation_status 
            (annotation_uuid, annotation_version, status) 
            VALUES (?, ?, ?)""", 
            (event.annotation.uuid, 
                event.annotation.version, 
                StatusT.CREATE_PENDING.name))

    def _consume_annotation_delete(self, event: AnnotationDeleteEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO annotation_event_link 
            (annotation_uuid, annotation_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (event.annotation_identifier.uuid, 
                event.annotation_identifier.version, 
                event.uuid))
        
        cur.execute("""INSERT INTO annotation_status 
            (annotation_uuid, annotation_version, status) 
            VALUES (?, ?, ?)""", 
            (event.annotation_identifier.uuid, 
                event.annotation_identifier.version, 
                StatusT.DELETE_PENDING.name))

    def _consume_review_accept(self, event: ReviewAcceptEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO event_review_link 
            (event_uuid, review_uuid) 
            VALUES (?, ?)""", 
            (event.event_uuid, event.uuid))

        cur.execute(
            "SELECT json FROM events WHERE uuid = ?", 
            (str(event.event_uuid),))

        res = cur.fetchone()
        if res is None:
            raise ValueError("event does not exist")

        target_json, = res
        target_data = json.loads(target_json)
        target = getattr(core, target_data["type"]).deserialize(target_data)

        if isinstance(target, ObjectEvent):
            if isinstance(target, ObjectCreateEvent, ObjectUpdateEvent):
                link_params = (target.object.uuid, 
                    target.object.version, 
                    event.uuid)

                cur.execute("""DELETE FROM object_status 
                    WHERE object_uuid = ? 
                        AND object_version = ? 
                        AND status = ?""", 
                    (target.object.uuid, 
                        target.object.version, 
                        StatusT.CREATE_PENDING.name))
            elif isinstance(target, ObjectDeleteEvent):
                link_parmas = (target.object_identifier.uuid, 
                    target.object_identifier.version, 
                    event.uuid)

                cur.execute("""DELETE FROM object_status 
                    WHERE object_uuid = ? 
                        AND object_version = ? 
                        AND status = ?""", 
                    (target.object_identifier.uuid, 
                        target.object_identifier.version, 
                        StatusT.DELETE_PENDING.name))

                cur.execute("""INSERT INTO object_status 
                    (object_uuid, object_version, status) 
                    VALUES (?, ?, ?)""", 
                    (event.object_identifier.uuid, 
                        event.object_identifier.version, 
                        StatusT.DELETE_ACCEPTED.name))
            else:
                raise ValueError("unexpected object event type in accept")

            cur.execute("""INSERT INTO object_event_link 
                (object_uuid, object_version, event_uuid) 
                VALUES (?, ?, ?)""", link_params)
        elif isinstance(target, AnnotationEvent):
            if isinstance(target, AnnotationCreateEvent, AnnotationUpdateEvent):
                link_params = (target.annotation.uuid, 
                    target.annotation.version, 
                    event.uuid)

                cur.execute("""DELETE FROM annotation_status 
                    WHERE annotation_uuid = ? 
                        AND annotation_version = ? 
                        AND status = ?""", 
                    (target.annotation.uuid, 
                        target.annotation.version, 
                        StatusT.CREATE_PENDING.name))
            elif isinstance(target, ObjectDeleteEvent):
                link_params = (target.annotation_identifier.uuid, 
                    target.annotation_identifier.version, 
                    event.uuid)

                cur.execute("""DELETE FROM annotation_status 
                    WHERE annotation_uuid = ? 
                        AND annotation_version = ? 
                        AND status = ?""", 
                    (target.annotation_identifier.uuid, 
                        target.annotation_identifier.version, 
                        StatusT.DELETE_PENDING.name))

                cur.execute("""INSERT INTO annotation_status 
                    (annotation_uuid, annotation_version, status) 
                    VALUES (?, ?, ?)""", 
                    (event.annotation_identifier.uuid, 
                        event.annotation_identifier.version, 
                        StatusT.DELETE_ACCEPTED.name))
            else:
                raise ValueError("unexpected annotation event type in accept")

            cur.execute("""INSERT INTO annotation_event_link 
                (annotation_uuid, annotation_version, event_uuid) 
                VALUES (?, ?, ?)""", link_params)
        else:
            raise ValueError("unexpected event type in accept")

    def _consume_review_reject(self, event: ReviewRejectEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO event_review_link 
            (event_uuid, review_uuid) 
            VALUES (?, ?)""", 
            (event.event_uuid, event.uuid))

        cur.execute("""SELECT json FROM events WHERE uuid = ?""", 
            (str(event.event_uuid),))

        res = cur.fetchone()
        if res is None:
            raise ValueError("event does not exist")

        target_json, = res
        target_data = json.loads(target_json)
        target = getattr(core, target_data["type"]).deserialize(target_data)

        if isinstance(target, ObjectEvent):
            if isinstance(target, ObjectCreateEvent, ObjectUpdateEvent):
                link_params = (target.object.uuid, 
                    target.object.version, 
                    event.uuid)

                cur.execute("""DELETE FROM object_status 
                    WHERE object_uuid = ? 
                        AND object_version = ? 
                        AND status = ?""", 
                    (target.object.uuid, 
                        target.object.version, 
                        StatusT.CREATE_PENDING.name))

                cur.execute("""INSERT INTO object_status 
                    (object_uuid, object_version, status) 
                    VALUES (?, ?, ?)""", 
                    (event.object.uuid, 
                        event.object.version, 
                        StatusT.CREATE_REJECTED.name))
            elif isinstance(target, ObjectDeleteEvent):
                link_parmas = (target.object_identifier.uuid, 
                    target.object_identifier.version, 
                    event.uuid)

                cur.execute("""DELETE FROM object_status 
                    WHERE object_uuid = ? 
                        AND object_version = ? 
                        AND status = ?""", 
                    (target.object_identifier.uuid, 
                        target.object_identifier.version, 
                        StatusT.DELETE_PENDING.name))
            else:
                raise ValueError("unexpected object event type in reject")

            cur.execute("""INSERT INTO object_event_link 
                (object_uuid, object_version, event_uuid) 
                VALUES (?, ?, ?)""", link_params)
        elif isinstance(target, AnnotationEvent):
            if isinstance(target, AnnotationCreateEvent, AnnotationUpdateEvent):
                link_params = (target.annotation.uuid, 
                    target.annotation.version, 
                    event.uuid)

                cur.execute("""DELETE FROM annotation_status 
                    WHERE annotation_uuid = ? 
                        AND annotation_version = ? 
                        AND status = ?""", 
                    (target.annotation.uuid, 
                        target.annotation.version, 
                        StatusT.CREATE_PENDING.name))

                cur.execute("""INSERT INTO annotation_status 
                    (annotation_uuid, annotation_version, status) 
                    VALUES (?, ?, ?)""", 
                    (event.annotation.uuid, 
                        event.annotation.version, 
                        StatusT.CREATE_REJECTED.name))
            elif isinstance(target, ObjectDeleteEvent):
                link_params = (target.annotation_identifier.uuid, 
                    target.annotation_identifier.version, 
                    event.uuid)

                cur.execute("""DELETE FROM annotation_status 
                    WHERE annotation_uuid = ? 
                        AND annotation_version = ? 
                        AND status = ?""", 
                    (target.annotation_identifier.uuid, 
                        target.annotation_identifier.version, 
                        StatusT.DELETE_PENDING.name))
            else:
                raise ValueError("unexpected annotation event type in reject")

            cur.execute("""INSERT INTO annotation_event_link 
                (annotation_uuid, annotation_version, event_uuid) 
                VALUES (?, ?, ?)""", link_params)
        else:
            raise ValueError("unexpected event type in reject")

    def _consume_owner_add(self, event: OwnerAddEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO owners (public_key) VALUES (?)""",
            (event.public_key.hex()))

    def _consume_owner_remove(self, event: OwnerRemoveEvent):
        cur = self.con.cursor()
        cur.execute("""DELETE FROM owners WHERE public_key = ?""",
            (event.public_key.hex()))

    ## HERE

    def _validate_object_create(self, event: ObjectCreateEvent):
        if is_schema(event.object.name):
            if self.state.schema_exists(name=event.object.name):
                raise ValidationError("schema name already in use")

        if self.state.object_exists(uuid_=event.object.uuid):
            raise ValidationError("UUID already exists in object store")

        if event.object.version != 0:
            raise ValidationError(
                "object version must be zero in create event")

    def _validate_object_update(self, event: ObjectCreateEvent):
        if not self.state.object_exists(uuid_=event.object.uuid):
            raise ValidationError("UUID not found in object store")

        if is_schema(event.object.name):
            if not self.state.schema_exists(name=event.object.name):
                raise ValidationError("schema name does not exist")

        versions = self.state.object_versions(uuid_=event.object.uuid)
        if versions[-1].uuid != event.object.uuid:
            raise ValidationError("unexpected UUID for schema name")

        if is_schema(versions[-1].name):
            if versions[-1].name != event.object.name:
                raise ValidationError("schema names may not change")

        if event.object.version != len(versions):
            raise ValidationError(
                f"object version should be {len(versions)}")

    def _validate_object_delete(self, event: ObjectDeleteEvent):
        if self.state.schema_exists(uuid_=event.object_identifier.uuid):
            raise ValidationError("schemas can not be deleted")

        identifier = event.object_identifier
        if not self.state.object_exists(identifier=identifier):
            raise ValidationError("object identifier not found")

        status = self.state.object_status(identifier=identifier)
        if StatusT.CREATE_REJECTED in status:
            raise ValidationError("cannot delete a rejected object")

        if StatusT.DELETE_PENDING in status:
            raise ValidationError("object version pending deletion")

        if StatusT.DELETE_ACCEPTED in status:
            raise ValidationError("object version already deleted")

    def _validate_annotation_create(self, event: AnnotationCreateEvent):
        if self.state.annotation_exists(uuid_=event.annotation.uuid):
            raise ValidationError(
                "UUID already exists in annotation store")

        if event.annotation.version != 0:
            raise ValidationError(
                "annotation version must be zero in create event")

        for identifier in event.object_identifiers:
            if not self.state.object_exists(identifier=identifier):
                raise ValidationError(
                    "object identifier not found in object store")

            status = self.state.object_status(identifier=identifier)
            if StatusT.CREATE_REJECTED in status:
                raise ValidationError(
                    "rejected objects cannot be annotated")

            if StatusT.DELETE_ACCEPTED in status:
                raise ValidationError(
                    "deleted objects cannot be annotated")

            if self.state.schema_exists(identifier=identifier):
                raise ValidationError("schemas can not be annotated")

    def _validate_annotation_update(self, event: AnnotationUpdateEvent):
        if not self.state.annotation_exists(uuid_=event.annotation.uuid):
            raise ValidationError("UUID not found in annotation store")

        versions = self.state.annotation_versions(uuid_=event.annotation.uuid)
        if event.annotation.version != len(versions):
            return f"Annotation version should be {len(versions)}."

    def _validate_annotation_delete(self, event: AnnotationDeleteEvent):
        identifier = event.annotation_identifier
        if not self.state.annotation_exists(identifier=identifier):
            raise ValidationError("annotation identifier not found")

        status = self.state.annotation_status(identifier=identifier)
        if StatusT.CREATE_REJECTED in status:
            raise ValidationError("cannot delete a rejected annotation")

        if StatusT.DELETE_PENDING in status:
            raise ValidationError("annotation already pending deletion")

        if StatusT.DELETE_ACCEPTED in status:
            raise ValidationError("annotation already deleted")

    def _validate_review_accept(self, event: ReviewAcceptEvent):
        if not self.state.event_pending(uuid_=event.event_uuid):
            raise ValidationError("target event not pending")

        if not self.state.owner_exists(public_key=event.signer):
            raise ValidationError("review event from non-owner")

    def _validate_review_reject(self, event: ReviewRejectEvent):
        if not self.state.event_pending(uuid_=event.event_uuid):
            raise ValidationError("target event not pending")

        if not self.state.owner_exists(public_key=event.signer):
            raise ValidationError("review event from non-owner")

    def _validate_owner_add(self, event: OwnerAddEvent):
        if self.state.owner_exists(public_key=event.public_key):
            raise ValidationError("owner already present")

        if len(self.state.owners()) > 0:
            if not self.state.owner_exists(public_key=event.signer):
                raise ValidationError("only owners can add owners")
        else:
            if event.public_key != event.signer:
                raise ValidationError(
                    "first owner add event must be self signed")

    def _validate_owner_remove(self, event: OwnerRemoveEvent):
        if not self.state.owner_exists(public_key=event.public_key):
            raise ValidationError("owner not present")

        if not self.state.owner_exists(public_key=event.signer):
            raise ValidationError("only owners can remove owners")

        owners = self.state.owners()
        if len(owners) == 1:
            raise ValidationError(
                "removing owner would leave the dataset ownerless")

        target_rank = owners.index(event.public_key)
        actor_rank = owners.index(event.signer)

        if actor_rank > target_rank:
            raise ValidationError("cannot remove a higher ranking owner")

    