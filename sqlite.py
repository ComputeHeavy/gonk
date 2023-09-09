import core
import uuid
import json
import pathlib
import sqlite3

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
                (event.object.name, 
                    str(event.object.uuid), 
                    event.object.version))   

        cur.execute("""INSERT INTO object_event_link 
            (object_uuid, object_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (str(event.object.uuid), event.object.version, event.uuid))
        
        cur.execute("""INSERT INTO object_status 
            (object_uuid, object_version, status) 
            VALUES (?, ?, ?)""", 
            (str(event.object.uuid), 
                event.object.version, 
                StatusT.CREATE_PENDING.name))

    def _consume_object_update(self, event: ObjectUpdateEvent):
        cur = self.con.cursor()
        if core.is_schema(event.object.name):
            cur.execute(
                "INSERT INTO schemas (name, uuid, version) VALUES (?, ?, ?)",
                (event.object.name, 
                    str(event.object.uuid), 
                    event.object.version))   

        cur.execute("""INSERT INTO object_event_link 
            (object_uuid, object_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (str(event.object.uuid), event.object.version, event.uuid))
        
        cur.execute("""INSERT INTO object_status 
            (object_uuid, object_version, status) 
            VALUES (?, ?, ?)""", 
            (str(event.object.uuid), 
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
            (str(event.annotation.uuid), event.annotation.version, event.uuid))

        cur.execute("""INSERT INTO annotation_status 
            (annotation_uuid, annotation_version, status) 
            VALUES (?, ?, ?)""", 
            (str(event.annotation.uuid), 
                event.annotation.version, 
                StatusT.CREATE_PENDING.name))

    def _consume_annotation_update(self, event: AnnotationUpdateEvent):
        cur = self.con.cursor()
        cur.execute("""INSERT INTO annotation_event_link 
            (annotation_uuid, annotation_version, event_uuid) 
            VALUES (?, ?, ?)""", 
            (str(event.annotation.uuid), event.annotation.version, event.uuid))

        cur.execute("""INSERT INTO annotation_status 
            (annotation_uuid, annotation_version, status) 
            VALUES (?, ?, ?)""", 
            (str(event.annotation.uuid), 
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
                    (str(event.object.uuid), 
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
                    (str(event.annotation.uuid), 
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

    def validate(self, event):
        super().validate(event)
        ## TODO: validate no replay

    def _validate_object_create(self, event: ObjectCreateEvent):
        cur = self.con.cursor()
        if core.is_schema(event.object.name):
            cur.execute('''SELECT COUNT(*) FROM schemas WHERE name = ?''',
                (event.object.name,))
            count, = cur.fetchone()
            if count != 0:
                raise ValidationError("schema name already in use")

        cur.execute('''SELECT COUNT(*) 
            FROM events 
            WHERE event->>'$.object.uuid' = ?''',
            (str(event.object.uuid),))

        count, = cur.fetchone()
        if count != 0:
            raise ValidationError("object with UUID already exists")

        if event.object.version != 0:
            raise ValidationError(
                "object version must be zero in create event")

    def _validate_object_update(self, event: ObjectCreateEvent):
        cur = self.con.cursor()
        cur.execute('''SELECT event 
            FROM events 
            WHERE event->>'$.object.uuid' = ?''', 
            (str(event.object.uuid),))
        versions_json = cur.fetchall()
        if len(versions_json) == 0:
            raise ValidationError("no objects with UUID found")

        versions_data = [json.loads(ea) for ea, in versions_json]
        
        versions = [getattr(core, ea["type"]).deserialize(ea).object 
            for ea in versions_data]

        if is_schema(versions[-1].name):
            if versions[-1].name != event.object.name:
                raise ValidationError("schema names may not change")
        else:
            if is_schema(event.object.name):
                raise ValidationError("object may not become schema")

        if event.object.version != len(versions):
            raise ValidationError(
                f"object version should be {len(versions)}")

    def _validate_object_delete(self, event: ObjectDeleteEvent):
        cur = self.con.cursor()
        cur.execute('''SELECT COUNT(*) FROM schemas WHERE uuid = ?''',
            (str(event.object_identifier.uuid),))
        
        count, = cur.fetchone()
        if count != 0:
            raise ValidationError("schemas can not be deleted")

        cur.execute('''SELECT COUNT(*)
            FROM events 
            WHERE event->>'$.object.uuid' = ?
                AND event->>'$.object.version' = ?''', 
            (str(event.object_identifier.uuid), 
                event.object_identifier.version))

        count, = cur.fetchone()
        if count == 0:
            raise ValidationError("object identifier not found")

        cur.execute('''SELECT status
            FROM object_status 
            WHERE object_uuid = ? 
                AND object_version = ?''',
            (str(event.object_identifier.uuid), 
                event.object_identifier.version))

        status = set([getattr(core.StatusT, ea) for ea, in cur.fetchall()])
        if core.StatusT.CREATE_REJECTED in status:
            raise ValidationError("cannot delete a rejected object")

        if core.StatusT.DELETE_PENDING in status:
            raise ValidationError("object version pending deletion")

        if core.StatusT.DELETE_ACCEPTED in status:
            raise ValidationError("object version already deleted")

    def _validate_annotation_create(self, event: AnnotationCreateEvent):
        cur = self.con.cursor()
        cur.execute('''SELECT COUNT(*) 
            FROM events 
            WHERE event->>'$.annotation.uuid' = ?''',
            (str(event.annotation.uuid),))

        count, = cur.fetchone()
        if count != 0:
            raise ValidationError("annotation with UUID already exists")

        if event.annotation.version != 0:
            raise ValidationError(
                "annotation version must be zero in create event")

        for identifier in event.object_identifiers:
            cur.execute('''SELECT COUNT(*)
                FROM events 
                WHERE event->>'$.object.uuid' = ?
                    AND event->>'$.object.version' = ?''', 
                (str(identifier.uuid), identifier.version))
            count, = cur.fetchone()
            if count == 0:
                raise ValidationError("object identifier not found")

            cur.execute('''SELECT status
                FROM object_status 
                WHERE object_uuid = ? 
                    AND object_version = ?''',
                (str(identifier.uuid), identifier.version))

            status = set([getattr(core.StatusT, ea) for ea, in cur.fetchall()])
            if StatusT.CREATE_REJECTED in status:
                raise ValidationError(
                    "rejected objects cannot be annotated")

            if StatusT.DELETE_ACCEPTED in status:
                raise ValidationError(
                    "deleted objects cannot be annotated")

            cur.execute('''SELECT COUNT(*) FROM schemas WHERE uuid = ?''',
                (str(identifier.uuid),))
            count, = cur.fetchone()
            if count != 0:
                raise ValidationError("schemas can not be deleted")

    def _validate_annotation_update(self, event: AnnotationUpdateEvent):
        cur = self.con.cursor()
        cur.execute('''SELECT event 
            FROM events 
            WHERE event->>'$.annotation.uuid' = ?''', 
            (str(event.annotation.uuid),))

        versions_json = cur.fetchall()
        if len(versions_json) == 0:
            raise ValidationError("no annotations with UUID found")

        if event.annotation.version != len(versions_json):
            raise ValidationError(
                f"annotation version should be {len(versions_json)}.")

    def _validate_annotation_delete(self, event: AnnotationDeleteEvent):
        cur = self.con.cursor()
        cur.execute('''SELECT COUNT(*)
            FROM events 
            WHERE event->>'$.annotation.uuid' = ?
                AND event->>'$.annotation.version' = ?''', 
            (str(event.annotation_identifier.uuid), 
                event.annotation_identifier.version))

        count, = cur.fetchone()
        if count == 0:
            raise ValidationError("annotation identifier not found")

        cur.execute('''SELECT status
            FROM annotation_status 
            WHERE annotation_uuid = ? 
                AND annotation_version = ?''',
            (str(event.annotation_identifier.uuid), 
                event.annotation_identifier.version))

        status = set([getattr(core.StatusT, ea) for ea, in cur.fetchall()])
        if StatusT.CREATE_REJECTED in status:
            raise ValidationError("cannot delete a rejected annotation")

        if StatusT.DELETE_PENDING in status:
            raise ValidationError("annotation already pending deletion")

        if StatusT.DELETE_ACCEPTED in status:
            raise ValidationError("annotation already deleted")

    def _validate_review(self, event: ReviewAcceptEvent):
        cur = self.con.cursor()
        cur.execute('''SELECT COUNT(*)
            FROM event_review_link 
            WHERE event_uuid = ?''', 
            (str(event.event_uuid),))

        count, = cur.fetchone()
        if count != 0:
            raise ValidationError("event already reviewed")

        cur.execute('''SELECT event 
            FROM events 
            WHERE event->>'$.uuid' = ?''', 
            (str(event.event_uuid),))

        res = cur.fetchone()
        if res is None:
            raise ValidationError("no events with event UUID found")

        target_json, = res
        target_data = json.loads(target_json)
        target_event = getattr(
            core, target_data["type"]).deserialize(target_data)

        if not isinstance(target_event, (AnnotationEvent, ObjectEvent)):
            raise ValidationError("review on non object or annotation event")

        cur.execute('''SELECT COUNT(*) 
            FROM owners 
            WHERE public_key = ?''',
            (event.signer.hex(),))
        count, = cur.fetchone()
        if count == 0:
            raise ValidationError("review event from non-owner")

    def _validate_review_accept(self, event: ReviewAcceptEvent):
        self._validate_review(event)

    def _validate_review_reject(self, event: ReviewRejectEvent):
        self._validate_review(event)
        
    def _validate_owner_add(self, event: OwnerAddEvent):
        cur = self.con.cursor()
        cur.execute('''SELECT public_key FROM owners''')

        owners = {public_key for public_key, in cur.fetchall()}
        if len(owners) == 0:
            raise ValidationError("dataset has no owners to remove")

        if len(owners) > 0:
            if event.public_key in owners:
                raise ValidationError("owner already present")

            if event.signer not in owners:
                raise ValidationError("only owners can add owners")
        else:
            if event.public_key != event.signer:
                raise ValidationError(
                    "first owner add event must be self signed")

    def _validate_owner_remove(self, event: OwnerRemoveEvent):
        cur = self.con.cursor()
        cur.execute('''SELECT id, public_key FROM owners''')

        owners = cur.fetchall()
        if len(owners) == 0:
            raise ValidationError("dataset has no owners to remove")

        ranks = {bytes.fromhex(public_key): id_ for id_, public_key in owners}

        if event.signer not in ranks:
            raise ValidationError("only owners may remove owners")

        if event.public_key not in ranks:
            raise ValidationError("target key is not an owner")
        
        if len(ranks) == 1:
            raise ValidationError(
                "removing owner would leave the dataset ownerless")

        target_rank = ranks[event.public_key]
        signer_rank = ranks[event.signer]

        if signer_rank > target_rank:
            raise ValidationError("cannot remove a higher ranking owner")
