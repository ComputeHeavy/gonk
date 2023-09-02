import enum
import uuid
import datetime
import struct
import json
import jsonschema
import nacl
import typing
from nacl import signing

def tsnow() -> str:
    return f"{datetime.datetime.utcnow().isoformat('T')}Z"

def is_schema(name: str) -> bool:
    return name.startswith("schema-")

class ValidationError(Exception):
    pass

class StorageError(Exception):
    pass

### Enums ###
class ActionT(enum.Enum):
    CREATE = 1<<0
    UPDATE = 1<<1
    DELETE = 1<<2

class DecisionT(enum.Enum):
    ACCEPT = 1<<0
    REJECT = 1<<1

class OwnerActionT(enum.Enum):
    ADD = 1<<0
    REMOVE = 1<<1

class HashTypeT(enum.Enum):
    SHA256 = 1<<0

class StatusT(enum.Enum):
    CREATE_PENDING = 1<<0
    CREATE_REJECTED = 1<<1
    DELETE_PENDING = 1<<2
    DELETE_ACCEPTED = 1<<3

### Data Containers ###
class Identifier:
    def __init__(self, uuid_: uuid.UUID, version: int):
        self.uuid = uuid_
        self.version = version

    def __hash__(self):
        return hash((self.uuid, self.version))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.uuid == other.uuid and self.version == other.version

    def __ne__(self, other):
        return not self.__eq__(other)

    def signature_bytes(self) -> bytes:
        return b"".join([
            self.uuid.bytes,
            struct.pack("<Q", self.version),
        ])

    def __repr__(self):
        return f"Identifier({self.uuid}, {self.version})"

class Object:
    def __init__(self, name: str, format_: str, size: int, hash_type: HashTypeT, 
        hash_: str, uuid_: typing.Optional[uuid.UUID] = None, version: int = 0):
        if uuid_ is None:
            uuid_ = uuid.uuid4()

        self.uuid = uuid_
        self.version = version
        self.name = name
        self.format = format_
        self.size = size
        self.hash_type = hash_type
        self.hash = hash_

    def __copy__(self):
        return Object(self.name, self.format, self.size, self.hash_type, 
          self.hash, self.uuid, self.version)

    def identifier(self):
        return Identifier(self.uuid, self.version)

    def signature_bytes(self) -> bytes:
        return b"".join([
            self.uuid.bytes,
            struct.pack("<Q", self.version),
            self.name.encode(),
            self.format.encode(),
            struct.pack("<Q", self.size),
            struct.pack("<B", self.hash_type.value),
            bytes.fromhex(self.hash),
        ])

class Annotation:
    def __init__(self, schema: Identifier, size: int, hash_type: HashTypeT, 
        hash_: str, uuid_: typing.Optional[uuid.UUID] = None, version: int = 0):
        if uuid_ is None:
            uuid_ = uuid.uuid4()

        self.uuid = uuid_
        self.version = version
        self.schema = schema
        self.size = size
        self.hash_type = hash_type
        self.hash = hash_

    def __copy__(self):
        return Annotation(self.schema, self.size, self.hash_type, self.hash, 
            self.uuid, self.version)

    def identifier(self):
        return Identifier(self.uuid, self.version)

    def signature_bytes(self) -> bytes:
        return b"".join([
            self.uuid.bytes,
            struct.pack("<Q", self.version),
            self.schema.signature_bytes(),
            struct.pack("<Q", self.size),
            struct.pack("<B", self.hash_type.value),
            bytes.fromhex(self.hash),
        ])

### Events ###
class Event:
    def __init__(self):
        self.timestamp = tsnow()
        self.uuid = uuid.uuid4()
        self.signature = b""
        self.signer: nacl.signing.VerifyKey = None

    def signature_bytes(self) -> bytes:
        raise NotImplementedError("unimplemented method")

    def _signature_bytes(self) -> bytes:
        return b"".join([
            self.timestamp.encode(),
            self.uuid.bytes,
        ])

### Object Events ###
class ObjectEvent(Event):
    def __init__(self, action: ActionT):
        super().__init__()
        self.action = action

    def signature_bytes(self) -> bytes:
        return b"".join([
            super()._signature_bytes(),
            struct.pack("<B", self.action.value),
        ])

class ObjectCreateEvent(ObjectEvent):
    def __init__(self, object_: Object):
        super().__init__(ActionT.CREATE)
        self.object = object_

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.object.signature_bytes(),
        ])

class ObjectUpdateEvent(ObjectEvent):
    def __init__(self, object_: Object):
        super().__init__(ActionT.UPDATE)
        self.object = object_

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.object.signature_bytes(),
        ])

class ObjectDeleteEvent(ObjectEvent):
    def __init__(self, object_identifier: Identifier):
        super().__init__(ActionT.DELETE)
        self.object_identifier = object_identifier

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.object_identifier.signature_bytes(),
        ])

### Annotation Events ###
class AnnotationEvent(Event):
    def __init__(self, action: ActionT):
        super().__init__()
        self.action = action

    def signature_bytes(self) -> bytes:
        return b"".join([
            super()._signature_bytes(),
            struct.pack("<B", self.action.value),
        ])

class AnnotationCreateEvent(AnnotationEvent):
    def __init__(self, object_identifiers: list[Identifier], 
        annotation: Annotation):
        super().__init__(ActionT.CREATE)
        self.object_identifiers = object_identifiers
        self.annotation = annotation

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            *[ea.signature_bytes() for ea in self.object_identifiers],
            self.annotation.signature_bytes(),
        ])

class AnnotationUpdateEvent(AnnotationEvent):
    def __init__(self, annotation: Annotation):
        super().__init__(ActionT.UPDATE)
        self.annotation = annotation

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.annotation.signature_bytes(),
        ])

class AnnotationDeleteEvent(AnnotationEvent):
    def __init__(self, annotation_identifier: Identifier):
        super().__init__(ActionT.DELETE)
        self.annotation_identifier = annotation_identifier

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.annotation_identifier.signature_bytes(),
        ])

### Review Events ###

class ReviewEvent(Event):
    def __init__(self, decision: DecisionT):
        super().__init__()
        self.decision = decision

    def signature_bytes(self) -> bytes:
        return b"".join([
            super()._signature_bytes(),
            struct.pack("<B", self.decision.value),
        ])

class ReviewAcceptEvent(ReviewEvent):
    def __init__(self, event_uuid: uuid.UUID):
        super().__init__(DecisionT.ACCEPT)
        self.event_uuid = event_uuid

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.event_uuid.bytes,
        ])

class ReviewRejectEvent(ReviewEvent):
    def __init__(self, event_uuid: uuid.UUID):
        super().__init__(DecisionT.REJECT)
        self.event_uuid = event_uuid

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.event_uuid.bytes,
        ])

### Ownership Events ###

class OwnerEvent(Event):
    def __init__(self, public_key: nacl.signing.VerifyKey, 
        action: OwnerActionT):
        super().__init__()
        self.public_key = public_key
        self.action = action

    def signature_bytes(self) -> bytes:
        return b"".join([
            super()._signature_bytes(),
            bytes(self.public_key),
            struct.pack("<B", self.action.value),
        ])

class OwnerAddEvent(OwnerEvent):
    def __init__(self, public_key: nacl.signing.VerifyKey):
        super().__init__(public_key, OwnerActionT.ADD)
        
class OwnerRemoveEvent(OwnerEvent):
    def __init__(self, public_key: nacl.signing.VerifyKey):
        super().__init__(public_key, OwnerActionT.REMOVE)

### Machine ###
class Machine:
    def __init__(self):
        self.validators: list[Validator] = []
        self.consumers: list[Consumer] = []

    def process_event(self, event):
        for v in self.validators:
            v.validate(event)

        for c in self.consumers:
            c.consume(event)

    def register(self, c):
        if isinstance(c, Validator):
            self.validators.append(c)

        if isinstance(c, Consumer):
            self.consumers.append(c)

class Validator:
    def validate(self, event):
        raise NotImplementedError("unimplemented method")

class Consumer:
    def consume(self, event):
        raise NotImplementedError("unimplemented method")

### Record Keeper (Events) ###
class RecordKeeper(Consumer):
    def consume(self, event: Event):
        self.add(event)

    def add(self, event: Event):
        raise NotImplementedError("unimplemented method")

    def read(self, uuid_: uuid.UUID) -> Event:
        raise NotImplementedError("unimplemented method")

    def exists(self, uuid_: uuid.UUID) -> bool:
        raise NotImplementedError("unimplemented method")

### Depot (Objects) ###
class Depot:
    def reserve(self, identifier: Identifier, size: int):
        raise NotImplementedError("unimplemented method")

    def write(self, identifier: Identifier, offset: int, buf: bytes):
        raise NotImplementedError("unimplemented method")

    def finalize(self, identifier: Identifier):
        raise NotImplementedError("unimplemented method")

    def read(self, identifier: Identifier, offset: int, size: int):
        raise NotImplementedError("unimplemented method")

    def purge(self, identifier: Identifier):
        raise NotImplementedError("unimplemented method")

### State ###
class Page:
    def __init__(self, items: list, page: int, page_size: int):
        self.page = page
        self.page_size = page_size
        self.items = items

class State:
    def object_exists(self, identifier: typing.Optional[Identifier] = None, 
        uuid_: typing.Optional[uuid.UUID] = None):
        raise NotImplementedError("unimplemented method")

    def objects(self, identifier: typing.Optional[Identifier] = None, 
        uuid_: typing.Optional[uuid.UUID] = None, 
        annotation: typing.Optional[uuid.UUID] = None, 
        page: typing.Optional[int] = None, 
        page_size: typing.Optional[int] = None):
        raise NotImplementedError("unimplemented method")

    def object_status(self, identifier: typing.Optional[Identifier] = None):
        raise NotImplementedError("unimplemented method")

    def object_versions(self, uuid_: typing.Optional[uuid.UUID] = None):
        raise NotImplementedError("unimplemented method")

    def schema_exists(self, identifier: typing.Optional[Identifier] = None, 
        uuid_: typing.Optional[uuid.UUID] = None, 
        name: typing.Optional[str] = None):
        raise NotImplementedError("unimplemented method")

    def annotation_exists(self, identifier: typing.Optional[Identifier] = None, 
        uuid_: typing.Optional[uuid.UUID] = None):
        raise NotImplementedError("unimplemented method")

    def annotations(self, identifier: typing.Optional[Identifier] = None, 
        uuid_: typing.Optional[uuid.UUID] = None, 
        object_: typing.Optional[Identifier] = None, 
        page: typing.Optional[int] = None, 
        page_size: typing.Optional[int] = None):
        raise NotImplementedError("unimplemented method")

    def annotation_status(self, identifier: Identifier):
        raise NotImplementedError("unimplemented method")

    def annotation_versions(self, uuid_: typing.Optional[uuid.UUID] = None):
        raise NotImplementedError("unimplemented method")

    def owner_exists(self, public_key: typing.Optional[bytes] = None):
        raise NotImplementedError("unimplemented method")

    def owners(self):
        raise NotImplementedError("unimplemented method")

    def event_pending(self, uuid_: uuid.UUID):
        raise NotImplementedError("unimplemented method")

class StateValidator(Validator):
    def __init__(self, state: State):
        super().__init__()
        self.state = state

    def validate(self, event: Event):
        handler: dict[typing.Type[Event], 
            typing.Callable[[typing.Any], None]] = {
            ObjectCreateEvent: self._validate_object_create,
            ObjectUpdateEvent: self._validate_object_update,
            ObjectDeleteEvent: self._validate_object_delete,
            AnnotationCreateEvent: self._validate_annotation_create,
            AnnotationUpdateEvent: self._validate_annotation_update,
            AnnotationDeleteEvent: self._validate_annotation_delete,
            ReviewAcceptEvent: self._validate_review_accept,
            ReviewRejectEvent: self._validate_review_reject,
            OwnerAddEvent: self._validate_owner_add,
            OwnerRemoveEvent: self._validate_owner_remove,
        }

        if type(event) not in handler:
            raise NotImplementedError("unhandled event type in validate")

        handler[type(event)](event)

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

        # TODO: function naming
        objects = self.state.objects(annotation=event.annotation.uuid)
        for object_ in objects:
            status = self.state.object_status(identifier=object_.identifier())
            if StatusT.CREATE_REJECTED in status:
                raise ValidationError(
                    "rejected objects cannot be annotated")

            if StatusT.DELETE_ACCEPTED in status:
                raise ValidationError(
                    "deleted objects cannot be annotated")

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

        objects = self.state.objects(annotation=identifier.uuid)
        for object_ in objects:
            object_status = self.state.object_status(
                identifier=object_.identifier())

            if StatusT.CREATE_REJECTED in object_status:
                raise ValidationError(
                    "rejected objects cannot be annotated")

            if StatusT.DELETE_ACCEPTED in object_status:
                raise ValidationError(
                    "deleted objects cannot be annotated")

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

class StateConsumer(Consumer):
    def __init__(self, state: State):
        super().__init__()
        self.state: State = state

    def consume(self, event: Event):
        handler: dict[typing.Type[Event], 
            typing.Callable[[typing.Any], None]] = {
            ObjectCreateEvent: self._consume_object_create,
            ObjectUpdateEvent: self._consume_object_update,
            ObjectDeleteEvent: self._consume_object_delete,
            AnnotationCreateEvent: self._consume_annotation_create,
            AnnotationUpdateEvent: self._consume_annotation_update,
            AnnotationDeleteEvent: self._consume_annotation_delete,
            ReviewAcceptEvent: self._consume_review_accept,
            ReviewRejectEvent: self._consume_review_reject,
            OwnerAddEvent: self._consume_owner_add,
            OwnerRemoveEvent: self._consume_owner_remove,
        }

        if type(event) not in handler:
            raise NotImplementedError("unhandled event type in consume")

        handler[type(event)](event)

    def _consume_object_create(self, event: ObjectCreateEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_object_update(self, event: ObjectUpdateEvent):
       raise NotImplementedError("unimplemented method")

    def _consume_object_delete(self, event: ObjectDeleteEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_annotation_create(self, event: AnnotationCreateEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_annotation_update(self, event: AnnotationUpdateEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_annotation_delete(self, event: AnnotationDeleteEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_review_accept(self, event: ReviewAcceptEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_review_reject(self, event: ReviewRejectEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_owner_add(self, event: OwnerAddEvent):
        raise NotImplementedError("unimplemented method")

    def _consume_owner_remove(self, event: OwnerRemoveEvent):
        raise NotImplementedError("unimplemented method")

class FieldValidator(Validator):
    def __init__(self):
        super().__init__()

    def validate(self, event: Event):
        handler: dict[typing.Type[Event], 
            typing.Callable[[typing.Any], None]] = {
            ObjectCreateEvent: self._validate_object,
            ObjectUpdateEvent: self._validate_object,
            AnnotationCreateEvent: self._validate_annotation,
            AnnotationUpdateEvent: self._validate_annotation,
        }

        if type(event) not in handler:
            return

        handler[type(event)](event)

    def _validate_object(self, event: ObjectCreateEvent|ObjectUpdateEvent):
        object_ = event.object
        if not isinstance(object_.uuid, uuid.UUID):
            raise ValidationError("UUID is not of type uuid.UUID")

        if object_.version < 0:
            raise ValidationError("version must be a non-negative integer")

        if len(object_.name) == 0:
            raise ValidationError("object name cannot be empty")

        if len(object_.format) == 0:
            raise ValidationError("object format cannot be empty")

        if object_.size < 0:
            raise ValidationError("size must be a non-negative integer")

        if object_.hash_type != HashTypeT.SHA256:
            raise ValidationError("hash type must be SHA256")

        if len(object_.hash) != 64:
            raise ValidationError("hash should be a hex encoded SHA256")

    def _validate_annotation(self, 
        event: AnnotationCreateEvent|AnnotationUpdateEvent):
        annotation = event.annotation
        if not isinstance(annotation.uuid, uuid.UUID):
            raise ValidationError("UUID is not of type uuid.UUID")

        if annotation.version < 0:
            raise ValidationError("version must be a positive integer")

        if annotation.size < 0:
            raise ValidationError("size must be a non-negative integer")

        if not isinstance(annotation.schema, Identifier):
            raise ValidationError("schema must be an identifier")

        if annotation.hash_type != HashTypeT.SHA256:
            raise ValidationError("hash type must be SHA256")

        if len(annotation.hash) != 64:
            raise ValidationError("hash should be a hex encoded SHA256")

class SchemaValidator(Validator, Consumer):
    def __init__(self, depot: Depot):
        super().__init__()
        self.depot: Depot = depot
        self.schemas: set[Identifier] = set()

    def validate(self, event: Event):
        handler: dict[typing.Type[Event], typing.Callable[[typing.Any], None]] = {
            ObjectCreateEvent: self._validate_object_create,
            ObjectUpdateEvent: self._validate_object_update,
            AnnotationCreateEvent: self._validate_annotation_create,
            AnnotationUpdateEvent: self._validate_annotation_update,
        }

        if type(event) not in handler:
            return

        handler[type(event)](event)

    def _validate_object(self, object_):
        if not is_schema(object_.name):
            return

        if object_.format != "application/schema+json":
            return

        bs = self.depot.read(object_.identifier(), 0, object_.size)
        schema = json.loads(bs.decode())
        try:
            jsonschema.protocols.Validator.check_schema(schema)
        except jsonschema.exceptions.SchemaError as error:
            raise ValidationError("invalid JSON schema") from error

    def _validate_annotation(self, annotation):
        if annotation.schema not in self.schemas:
            return 

        schema_bs = bytes()
        off = 0
        chunk = 1024*5
        while True:
            buf = self.depot.read(annotation.schema, off, chunk)
            schema_bs += buf
            off += len(buf)
            if len(buf) < chunk:
                break

        schema = json.loads(schema_bs.decode())

        annotation_bs = self.depot.read(
            annotation.identifier(), 0, annotation.size)
        instance = json.loads(annotation_bs.decode())

        try:
            jsonschema.validate(instance, schema)
        except jsonschema.exceptions.ValidationError as error:
            raise ValidationError("annotation does not match schema") from error

    def _validate_object_create(self, event: ObjectCreateEvent):
        self._validate_object(event.object)

    def _validate_object_update(self, event: ObjectCreateEvent):
        self._validate_object(event.object)

    def _validate_annotation_create(
        self, event: AnnotationCreateEvent):
        self._validate_annotation(event.annotation)

    def _validate_annotation_update(
        self, event: AnnotationUpdateEvent):
        self._validate_annotation(event.annotation)

    def consume(self, event: Event):
        handler: dict[typing.Type[Event], 
            typing.Callable[[typing.Any], None]] = {
            ObjectCreateEvent: self._consume_object_create,
            ObjectUpdateEvent: self._consume_object_update,
        }

        if type(event) not in handler:
            return

        handler[type(event)](event)

    def _consume_object(self, object_: Object):
        if not is_schema(object_.name):
            return

        if object_.format != "application/schema+json":
            return

        self.schemas.add(object_.identifier())

    def _consume_object_create(self, event: ObjectCreateEvent):
        self._consume_object(event.object)

    def _consume_object_update(self, event: ObjectUpdateEvent):
        self._consume_object(event.object)
