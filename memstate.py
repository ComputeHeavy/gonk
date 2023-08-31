import core
import uuid
import nacl

'''
Object Create
    Pending
        Update OK
        Annotation OK
    Accepted
        Update OK
        Deletion OK
        Annotation OK
    Rejected
        Update OK
    
Object Update
    Pending 
        Update OK
        Annotation OK
    Accepted
        Update OK
        Deletion OK
        Annotation OK
    Rejected
        Update OK

Object Delete
    Pending
        Update OK
        Annotation OK
    Accepted
        Update OK
    Rejected
        Update OK
        Deletion OK
        Annotation OK

Annotation Create
    Pending
        Update OK
    Accepted
        Update OK
        Deletion OK
    Rejected
        Update OK

Annotation Update
    Pending
        Update OK
    Accepted
        Update OK
        Deletion OK
    Rejected
        Update OK

Annotation Delete
    Pending
        Update OK
    Accepted
        Update OK
    Rejected
        Update OK
        Deletion OK
'''

class IdentifierUUIDLink:
    def __init__(self):
        self.forward: dict[Identifier, list[uuid.UUID]] = {}
        self.reverse: dict[uuid.UUID, list[Identifier]] = {}

    def add(self, identifier: Identifer, uuid_: uuid.UUID):
        if identifier not in self.forward:
            self.forward[identifier] = []

        if uuid_ not in self.reverse:
            self.reverse[uuid_] = []

        self.forward[identifier].append(uuid_)
        self.reverse[uuid_].append(identifier)

class TagT(enum.Enum):
    CREATE_PENDING = 1<<0
    CREATE_REJECTED = 1<<1
    DELETE_PENDING = 1<<2
    DELETE_ACCEPTED = 1<<3

class State(core.State):
    def __init__(self, record_keeper: RecordKeeper):
        super().__init__()
        self.record_keeper = record_keeper

        self.schemas: dict[str, uuid.UUID] = dict()
        self.objects: dict[uuid.UUID, list[core.Object]] = dict()
        self.annotations: dict[uuid.UUID, list[core.Annotation]] = dict()
        self.object_annotation_link: IdentifierUUIDLink = IdentifierUUIDLink()

        self.pending_events: set[uuid.UUID] = set()
        self.entity_status: dict[core.Identifier, set[TagT]] = dict()
        self.entity_event_link: IdentifierUUIDLink = IdentifierUUIDLink()

        self.owners: set[nacl.signing.VerifyKey] = set()

    def _validate_object_create(self, event: core.ObjectCreateEvent):
        if core.is_schema(event.object.name):
            if event.object.name in self.schemas:
                raise core.ValidationError("schema name already in use")

        if event.object.uuid in self.objects:
            raise core.ValidationError("UUID already exists in object store")

        if event.object.version != 0:
            raise core.ValidationError(
                "object version must be zero in create event")

    def _validate_object_update(self, event: core.ObjectCreateEvent):
        if core.is_schema(event.object.name):
            if event.object.name not in self.schemas:
                raise core.ValidationError("schema name does not exist")

            if self.schemas[event.object.name] != event.object.uuid:
                raise core.ValidationError("unexpected UUID for schema name")

        if event.object.uuid not in self.objects:
            raise core.ValidationError("UUID not found in object store")

        expected_version = len(self.objects[event.object.uuid])
        if event.object.version != expected_version:
            raise core.ValidationError(
                f"object version should be {expected_version}.")

    def _validate_object_delete(self, event: core.ObjectDeleteEvent):
        if event.object_identifier.uuid in self.schemas.values():
            raise core.ValidationError("schemas can not be deleted")

        identifier = event.object_identifier
        if identifier.uuid not in self.objects:
            raise core.ValidationError("object identifier not found")

        if len(self.objects[identifier.uuid]) <= identifier.version:
            raise core.ValidationError("version does not exist")

        if identifier in self.deleted_objects:
            raise core.ValidationError("object version already deleted")

    def _validate_annotation_create(self, event: core.AnnotationCreateEvent):

        if event.annotation.uuid in self.annotations:
            raise core.ValidationError(
                "UUID already exists in annotation store")

        if event.annotation.version != 0:
            raise core.ValidationError(
                "annotation version must be zero in create event")

        if event.annotation.uuid in self.object_annotation_link.reverse:
            raise core.ValidationError(
                "annotation identifier already linked to objects")

        for identifier in event.object_identifiers:
            if identifier.uuid not in self.objects:
                raise core.ValidationError(
                    "object identifier not found in object store")

            if len(self.objects[identifier.uuid]) <= identifier.version:
                raise core.ValidationError("version does not exist")

            if identifier in self.deleted_objects:
                raise core.ValidationError(
                    "deleted objects cannot be annotated")

            if identifier.uuid in self.schemas.values():
                raise core.ValidationError("schemas can not be annotated")

    def _validate_annotation_update(self, event: core.AnnotationUpdateEvent):
        if event.annotation.uuid not in self.annotations:
            raise core.ValidationError("UUID not found in annotation store")

        expected_version = len(self.annotations[event.annotation.uuid])
        if event.annotation.version != expected_version:
            return f"Annotation version should be {expected_version}."

        if event.annotation.uuid not in self.object_annotation_link.reverse:
            raise core.ValidationError(
                "annotation identifier not linked to any objects")

        for identifier in self.object_annotation_link.reverse[
            event.annotation.uuid]:
            if identifier in self.deleted_objects:
                raise core.ValidationError("annotating a deleted object")

    def _validate_annotation_delete(self, event: core.AnnotationDeleteEvent):
        identifier = event.annotation_identifier
        if identifier.uuid not in self.annotations:
            raise core.ValidationError("annotation identifier not found")

        if len(self.annotations[identifier.uuid]) <= identifier.version:
            raise core.ValidationError("version does not exist")

        if identifier in self.deleted_annotations:
            raise core.ValidationError("annotation version already deleted")

    def _validate_review_accept(self, event: ReviewAcceptEvent):
        raise NotImplementedError("unimplemented method")

    def _validate_review_reject(self, event: ReviewRejectEvent):
        raise NotImplementedError("unimplemented method")

    def _validate_owner_add(self, event: OwnerAddEvent):
        raise NotImplementedError("unimplemented method")

    def _validate_owner_remove(self, event: OwnerRemoveEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_object_create(self, event: core.ObjectCreateEvent):
        self.objects[event.object.uuid] = []
        self.objects[event.object.uuid].append(event.object)

        if core.is_schema(event.object.name):
            self.schemas[event.object.name] = event.object.uuid

        self.pending_events.add(event.uuid)
        self.entity_status[event.object.identifier()] = set(
            [TagT.CREATE_PENDING])
        self.entity_event_link.add(event.object.identifier(), event.uuid)

    def _consume_object_update(self, event: core.ObjectUpdateEvent):
        self.objects[event.object.uuid].append(event.object)
        self.pending_events.add(event.uuid)
        self.entity_status[event.object.identifier()] = set(
            [TagT.CREATE_PENDING])
        self.entity_event_link.add(event.object.identifier(), event.uuid)

    def _consume_object_delete(self, event: core.ObjectDeleteEvent):
        self.pending_events.add(event.uuid)
        self.entity_status[event.object.identifier()].add(TagT.DELETE_PENDING)
        self.entity_event_link.add(event.object.identifier(), event.uuid)

    def _consume_annotation_create(self, event: core.AnnotationCreateEvent):
        self.annotations[event.annotation.uuid] = []
        self.annotations[event.annotation.uuid].append(event.annotation)
    
        for identifier in event.object_identifiers:
            self.object_annotation_link.add(identifier, event.annotation.uuid)

        self.pending_events.add(event.uuid)
        self.entity_status[event.annotation.identifier()] = set(
            [TagT.CREATE_PENDING])
        self.entity_event_link.add(event.annotation.identifier(), event.uuid)

    def _consume_annotation_update(self, event: core.AnnotationUpdateEvent):
        self.annotations[event.annotation.uuid].append(event.annotation)
        self.pending_events.add(event.uuid)
        self.entity_status[event.annotation.identifier()] = set(
            [TagT.CREATE_PENDING])
        self.entity_event_link.add(event.annotation.identifier(), event.uuid)

    def _consume_annotation_delete(self, event: core.AnnotationDeleteEvent):
        self.pending_events.add(event.uuid)
        self.entity_status[event.annotation.identifier()].add(
            TagT.DELETE_PENDING)
        self.entity_event_link.add(event.annotation.identifier(), event.uuid)

    def _consume_review_accept(self, event: ReviewAcceptEvent):
        self.pending_events.remove(event.event_uuid)

        target = self.record_keeper.read(event.event_uuid)
        if target.action in [core.ActionT.CREATE, core.ActionT.UPDATE]:
            if isinstance(target, core.ObjectEvent):
                entity_identifier = target.object.identifier()
            elif isinstance(target, core.AnnotationEvent):
                entity_identifier = target.annotation.identifier()
            else:
                raise TypeError("target event has unexpected type")

            self.entity_status[entity_identifier].remove(CREATE_PENDING)
        elif target.action == core.ActionT.DELETE:
            if isinstance(target, core.ObjectEvent):
                entity_identifier = target.object_identifier
            elif isinstance(target, core.AnnotationEvent):
                entity_identifier = target.annotation_identifier
            else:
                raise TypeError("target event has unexpected type")

            self.entity_status[entity_identifier].remove(DELETE_PENDING)
            self.entity_status[entity_identifier].add(DELETE_ACCEPTED)
        else:
            raise ValueError("target event has unexpected action")

        self.entity_event_link.add(entity_identifier, event.uuid)

    def _consume_review_reject(self, event: ReviewRejectEvent):
        self.pending_events.remove(event.event_uuid)

        target = self.record_keeper.read(event.event_uuid)
        if target.action in [core.ActionT.CREATE, core.ActionT.UPDATE]:
            if isinstance(target, core.ObjectEvent):
                entity = target.object
            elif isinstance(target, core.AnnotationEvent):
                entity = target.annotation
            else:
                raise TypeError("target event has unexpected type")

            self.entity_status[entity.identifier()].remove(CREATE_PENDING)
            self.entity_status[entity.identifier()].add(CREATE_REJECTED)
        elif target.action == core.ActionT.DELETE:
            if isinstance(target, core.ObjectEvent):
                identifier = target.object_identifier
            elif isinstance(target, core.AnnotationEvent):
                identifier = target.annotation_identifier
            else:
                raise TypeError("target event has unexpected type")

            self.entity_status[identifier].remove(DELETE_PENDING)
        else:
            raise ValueError("target event has unexpected action")

    def _consume_owner_add(self, event: OwnerAddEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_owner_remove(self, event: OwnerRemoveEvent):
        raise NotImplementedError("unimplemented method")
