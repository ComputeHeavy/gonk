import enum
import json
import uuid
import struct
import typing
import datetime
import jsonschema

def tsnow() -> str:
    return f"{datetime.datetime.utcnow().isoformat('T')}Z"

'''
json dump
json load
json has a type field to indicate class
class has from_json and serializable methods
'''

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

    def dump(self):
        return {
            "uuid": str(self.uuid),
            "version": self.version,
            "name": self.name,
            "format": self.format,
            "size": self.size,
            "hash_type": self.hash_type.value,
            "hash": self.hash,
        }

    def load(d: dict):
        jsonschema.validate(instance=d, schema=self.schema())
        return Object(
            d["name"], 
            d["format"], 
            d["size"], 
            HashTypeT(d["hash_type"]), 
            d["hash"], 
            uuid.UUID(d["uuid"]), 
            d["version"])

    def schema():
        return {
            "type": "object",
            "properties": {
                "uuid": {
                    "type": "string",
                    "format": "uuid",
                },
                "version": {
                    "type": "integer",
                    "minimum": 0,
                },
                "name": {
                    "type": "string"
                },
                "format": {
                    "type": "string"
                },
                "size": {
                    "type": "integer"
                    "minimum": 0,
                },
                "hash_type": {
                    "type": "integer"
                },
                "hash": {
                    "type": "string",
                    "minLength": 64,
                    "maxLength": 64,
                    "pattern": "^[0-9a-fA-F]{64}$"
                }
            },
            "required": [
                "uuid",
                "version"
                "name",
                "format",
                "size",
                "hash",
                "hash_type",
            ],
        }

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

    def dump(self):
        return {
            "uuid": str(self.uuid),
            "version": self.version,
            "schema": self.schema,
            "size": self.size,
            "hash_type": self.hash_type.value,
            "hash": self.hash,
        }

    def load(d: dict):
        jsonschema.validate(instance=d, schema=self.schema())
        return Annotation(
            d["schema"], 
            d["size"], 
            HashTypeT(d["hash_type"]), 
            d["hash"], 
            uuid.UUID(d["uuid"]), 
            d["version"])

    def schema():
        return {
            "type": "object",
            "properties": {
                "uuid": {
                    "type": "string",
                    "format": "uuid",
                },
                "version": {
                    "type": "integer",
                    "minimum": 0,
                },
                "schema": {
                    "type": "string"
                },
                "size": {
                    "type": "integer"
                    "minimum": 0,
                },
                "hash_type": {
                    "type": "integer"
                },
                "hash": {
                    "type": "string",
                    "minLength": 64,
                    "maxLength": 64,
                    "pattern": "^[0-9a-fA-F]{64}$"
                }
            },
            "required": [
                "uuid",
                "version"
                "schema",
                "size",
                "hash",
                "hash_type",
            ],
        }

### Events ###
class Event:
    def __init__(self):
        self.timestamp: str = tsnow()
        self.uuid: uuid.UUID = uuid.uuid4()
        self.signature: bytes = b""
        self.signer: typing.Optional[bytes] = None

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
    def __init__(self, public_key: bytes, action: OwnerActionT):
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
    def __init__(self, public_key: bytes):
        super().__init__(public_key, OwnerActionT.ADD)
        
class OwnerRemoveEvent(OwnerEvent):
    def __init__(self, public_key: bytes):
        super().__init__(public_key, OwnerActionT.REMOVE)