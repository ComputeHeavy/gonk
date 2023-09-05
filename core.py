import uuid
import enum
import json
import struct
import typing
import datetime
import jsonschema


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

    def dump(self):
        return {
            "uuid": str(self.uuid),
            "version": self.version,
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return Identifier(uuid.UUID(d["uuid"]), d["version"])

    @staticmethod
    def schema(relative=""):
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
            },
            "required": [
                "uuid",
                "version",
            ],
        }

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

    def __copy__(self):
        return Object(self.name, self.format, self.size, self.hash_type, 
          self.hash, self.uuid, self.version)

    def __eq__(self, other):
        return self.uuid == other.uuid and \
            self.version == other.version and \
            self.name == other.name and \
            self.format == other.format and \
            self.size == other.size and \
            self.hash_type == other.hash_type and \
            self.hash == other.hash

    def __ne__(self, other):
        return not self.__eq__(other)

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

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return Object(
            d["name"], 
            d["format"], 
            d["size"], 
            HashTypeT(d["hash_type"]), 
            d["hash"], 
            uuid.UUID(d["uuid"]), 
            d["version"])

    @staticmethod
    def schema(relative=""):
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
                    "type": "integer",
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
                "version",
                "name",
                "format",
                "size",
                "hash",
                "hash_type",
            ],
        }

class Annotation:
    def __init__(self, schema_: Identifier, size: int, hash_type: HashTypeT, 
        hash_: str, uuid_: typing.Optional[uuid.UUID] = None, version: int = 0):
        if uuid_ is None:
            uuid_ = uuid.uuid4()

        self.uuid = uuid_
        self.version = version
        self.schema_ = schema_
        self.size = size
        self.hash_type = hash_type
        self.hash = hash_

    def __copy__(self):
        return Annotation(self.schema_, self.size, self.hash_type, self.hash, 
            self.uuid, self.version)

    def identifier(self):
        return Identifier(self.uuid, self.version)

    def signature_bytes(self) -> bytes:
        return b"".join([
            self.uuid.bytes,
            struct.pack("<Q", self.version),
            self.schema_.signature_bytes(),
            struct.pack("<Q", self.size),
            struct.pack("<B", self.hash_type.value),
            bytes.fromhex(self.hash),
        ])

    def __eq__(self, other):
        return self.uuid == other.uuid and \
            self.version == other.version and \
            self.schema_ == other.schema_ and \
            self.size == other.size and \
            self.hash_type == other.hash_type and \
            self.hash == other.hash

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return {
            "uuid": str(self.uuid),
            "version": self.version,
            "schema": self.schema_.dump(),
            "size": self.size,
            "hash_type": self.hash_type.value,
            "hash": self.hash,
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return Annotation(
            Identifier.load(d["schema"]), 
            d["size"], 
            HashTypeT(d["hash_type"]), 
            d["hash"], 
            uuid.UUID(d["uuid"]), 
            d["version"])

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "identifier": Identifier.schema("/definitions/identifier"),
            },
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
                    "$ref": f"#{relative}/definitions/identifier",
                },
                "size": {
                    "type": "integer",
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
                "version",
                "schema",
                "size",
                "hash",
                "hash_type",
            ],
        }

### Events ###
class Event:
    def __init__(self, 
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):

        if uuid_ is None:
            uuid_ = uuid.uuid4()

        if timestamp is None:
            timestamp = tsnow()

        self.uuid: uuid.UUID = uuid_
        self.timestamp: str = timestamp
        self.signature: typing.Optional[bytes] = signature
        self.signer: typing.Optional[bytes] = signer

    def signature_bytes(self) -> bytes:
        raise NotImplementedError("unimplemented method")

    def _signature_bytes(self) -> bytes:
        return b"".join([
            self.uuid.bytes,
            self.timestamp.encode(),
        ])

    def __eq__(self, other):
        return self.uuid == other.uuid and \
            self.timestamp == other.timestamp and \
            self.signature == other.signature and \
            self.signer == other.signer 

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        if self.signature is None:
            raise ValueError("signature is not set")
        
        if self.signer is None:
            raise ValueError("signature is not set")

        return {
            "uuid": str(self.uuid),
            "timestamp": self.timestamp,
            "signature": self.signature.hex(),
            "signer": self.signer.hex(),
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return Event(uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "properties": {
                "uuid": {
                    "type": "string",
                    "format": "uuid",
                },
                "timestamp": {
                    "type": "string"
                },
                "signature": {
                    "type": "string",
                    "minLength": 128,
                    "maxLength": 128,
                    "pattern": "^[0-9a-fA-F]{128}$"
                },
                "signer": {
                    "type": "string",
                    "minLength": 64,
                    "maxLength": 64,
                    "pattern": "^[0-9a-fA-F]{64}$"
                },
            },
            "required": [
                "uuid",
                "timestamp",
                "signature",
                "signer",
            ],
        }

'''
json dump
json load
json has a type field to indicate class
class has from_json and serializable methods
'''

### Object Events ###
class ObjectEvent(Event):
    def __init__(self, 
        action: ActionT,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(uuid_, timestamp, signature, signer)
        self.action = action

    def signature_bytes(self) -> bytes:
        return b"".join([
            super()._signature_bytes(),
            struct.pack("<B", self.action.value),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.action == other.action 

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "action": self.action.value,
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ObjectEvent(ActionT(d["action"]), 
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "event": Event.schema("/definitions/event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/event" }
            ],
            "properties": {
                "action": {
                    "type": "integer"
                },
            },
            "required": [
                "action",
            ],
        }

class ObjectCreateEvent(ObjectEvent):
    def __init__(self, 
        object_: Object,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(ActionT.CREATE, uuid_, timestamp, signature, signer)
        self.object = object_

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.object.signature_bytes(),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.object == other.object

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "object": self.object.dump(),
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ObjectCreateEvent(Object.load(d["object"]),
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "object": Object.schema("/definitions/object"),
                "object_event": ObjectEvent.schema("/definitions/object_event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/object_event" }
            ],
            "properties": {
                "object": {
                    "$ref": f"#{relative}/definitions/object",
                }
            },
            "required": [
                "object",
            ],
        }

class ObjectUpdateEvent(ObjectEvent):
    def __init__(self, 
        object_: Object,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(ActionT.UPDATE, uuid_, timestamp, signature, signer)
        self.object = object_

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.object.signature_bytes(),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.object == other.object

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "object": self.object.dump(),
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ObjectUpdateEvent(Object.load(d["object"]),
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "object": Object.schema("/definitions/object"),
                "object_event": ObjectEvent.schema("/definitions/object_event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/object_event" }
            ],
            "properties": {
                "object": {
                    "$ref": f"#{relative}/definitions/object",
                }
            },
            "required": [
                "object",
            ],
        }

class ObjectDeleteEvent(ObjectEvent):
    def __init__(self, 
        object_identifier: Identifier,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(ActionT.DELETE, uuid_, timestamp, signature, signer)
        self.object_identifier = object_identifier

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.object_identifier.signature_bytes(),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.object_identifier == other.object_identifier

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "object_identifier": self.object_identifier.dump(),
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ObjectDeleteEvent(Identifier.load(d["object_identifier"]),
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "identifier": Identifier.schema("/definitions/identifier"),
                "object_event": ObjectEvent.schema("/definitions/object_event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/object_event" }
            ],
            "properties": {
                "object_identifier": {
                    "$ref": f"#{relative}/definitions/identifier",
                }
            },
            "required": [
                "object_identifier",
            ],
        }

### Annotation Events ###
class AnnotationEvent(Event):
    def __init__(self, 
        action: ActionT,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(uuid_, timestamp, signature, signer)
        self.action = action

    def signature_bytes(self) -> bytes:
        return b"".join([
            super()._signature_bytes(),
            struct.pack("<B", self.action.value),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.action == other.action 

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "action": self.action.value,
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return AnnotationEvent(ActionT(d["action"]), 
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "event": Event.schema("/definitions/event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/event" }
            ],
            "properties": {
                "action": {
                    "type": "integer"
                },
            },
            "required": [
                "action",
            ],
        }

class AnnotationCreateEvent(AnnotationEvent):
    def __init__(self, 
        object_identifiers: list[Identifier], 
        annotation: Annotation,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(ActionT.CREATE, uuid_, timestamp, signature, signer)
        self.object_identifiers = object_identifiers
        self.annotation = annotation

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            *[ea.signature_bytes() for ea in self.object_identifiers],
            self.annotation.signature_bytes(),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.object_identifiers == other.object_identifiers and \
            self.annotation == other.annotation

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "annotation": self.annotation.dump(),
            "object_identifiers": [ea.dump() for ea in self.object_identifiers],
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return AnnotationCreateEvent(
            [Identifier.load(ea) for ea in d["object_identifiers"]],
            Annotation.load(d["annotation"]),
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "annotation": Annotation.schema("/definitions/annotation"),
                "annotation_event": 
                    ObjectEvent.schema("/definitions/annotation_event"),
                "identifier": Identifier.schema("/definitions/identifier"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/annotation_event" }
            ],
            "properties": {
                "annotation": {
                    "$ref": f"#{relative}/definitions/annotation",
                },
                "object_identifiers": {
                    "type": "array",
                    "items": {
                        "$ref": f"#{relative}/definitions/identifier",
                    },
                },
            },
            "required": [
                "annotation",
                "object_identifiers",
            ],
        }

class AnnotationUpdateEvent(AnnotationEvent):
    def __init__(self, 
        annotation: Annotation,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(ActionT.UPDATE, uuid_, timestamp, signature, signer)
        self.annotation = annotation

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.annotation.signature_bytes(),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.annotation == other.annotation

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "annotation": self.annotation.dump(),
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return AnnotationUpdateEvent(
            Annotation.load(d["annotation"]),
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "annotation": Annotation.schema("/definitions/annotation"),
                "annotation_event": 
                    ObjectEvent.schema("/definitions/annotation_event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/annotation_event" }
            ],
            "properties": {
                "annotation": {
                    "$ref": f"#{relative}/definitions/annotation",
                },
            },
            "required": [
                "annotation",
            ],
        }

class AnnotationDeleteEvent(AnnotationEvent):
    def __init__(self, 
        annotation_identifier: Identifier,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(ActionT.DELETE, uuid_, timestamp, signature, signer)
        self.annotation_identifier = annotation_identifier

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.annotation_identifier.signature_bytes(),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.annotation_identifier == other.annotation_identifier

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "annotation_identifier": self.annotation_identifier.dump(),
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return AnnotationDeleteEvent(
            Identifier.load(d["annotation_identifier"]),
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "identifier": Identifier.schema("/definitions/identifier"),
                "annotation_event": 
                    AnnotationEvent.schema("/definitions/annotation_event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/annotation_event" }
            ],
            "properties": {
                "annotation_identifier": {
                    "$ref": f"#{relative}/definitions/identifier",
                }
            },
            "required": [
                "annotation_identifier",
            ],
        }

### Review Events ###
class ReviewEvent(Event):
    def __init__(self, 
        decision: DecisionT,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(uuid_, timestamp, signature, signer)
        self.decision = decision

    def signature_bytes(self) -> bytes:
        return b"".join([
            super()._signature_bytes(),
            struct.pack("<B", self.decision.value),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.decision == other.decision 

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "decision": self.decision.value,
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ReviewEvent(
            DecisionT(d["decision"]), 
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "event": Event.schema("/definitions/event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/event" }
            ],
            "properties": {
                "decision": {
                    "type": "integer"
                },
            },
            "required": [
                "decision",
            ],
        }

class ReviewAcceptEvent(ReviewEvent):
    def __init__(self, 
        event_uuid: uuid.UUID,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(DecisionT.ACCEPT, uuid_, timestamp, signature, signer)
        self.event_uuid = event_uuid

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.event_uuid.bytes,
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.event_uuid == other.event_uuid 

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "event_uuid": str(self.event_uuid),
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ReviewAcceptEvent(
            uuid.UUID(d["event_uuid"]), 
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "review_event": ReviewEvent.schema("/definitions/review_event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/review_event" }
            ],
            "properties": {
                "event_uuid": {
                    "type": "string",
                    "format": "uuid",
                },
            },
            "required": [
                "event_uuid",
            ],
        }

class ReviewRejectEvent(ReviewEvent):
    def __init__(self, 
        event_uuid: uuid.UUID,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(DecisionT.REJECT, uuid_, timestamp, signature, signer)
        self.event_uuid = event_uuid

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.event_uuid.bytes,
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.event_uuid == other.event_uuid 

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "event_uuid": str(self.event_uuid),
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ReviewRejectEvent(
            uuid.UUID(d["event_uuid"]), 
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "review_event": ReviewEvent.schema("/definitions/review_event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/review_event" }
            ],
            "properties": {
                "event_uuid": {
                    "type": "string",
                    "format": "uuid",
                },
            },
            "required": [
                "event_uuid",
            ],
        }

### Ownership Events ###

class OwnerEvent(Event):
    def __init__(self, 
        public_key: bytes, 
        owner_action: OwnerActionT,
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(uuid_, timestamp, signature, signer)
        self.public_key = public_key
        self.owner_action = owner_action

    def signature_bytes(self) -> bytes:
        return b"".join([
            super()._signature_bytes(),
            self.public_key,
            struct.pack("<B", self.owner_action.value),
        ])

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.public_key == other.public_key and \
            self.owner_action == other.owner_action 

    def __ne__(self, other):
        return not self.__eq__(other)

    def dump(self):
        return super().dump() | {
            "public_key": self.public_key.hex(),
            "owner_action": self.owner_action.value,
        }

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return OwnerEvent(
            bytes.fromhex(d["public_key"]),
            OwnerActionT(d["owner_action"]),
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

    @staticmethod
    def schema(relative=""):
        return {
            "type": "object",
            "definitions": {
                "event": Event.schema("/definitions/event"),
            },
            "allOf": [
                { "$ref": f"#{relative}/definitions/event" }
            ],
            "properties": {
                "owner_action": {
                    "type": "integer"
                },
                "public_key": {
                    "type": "string",
                    "minLength": 64,
                    "maxLength": 64,
                    "pattern": "^[0-9a-fA-F]{64}$"
                },
            },
            "required": [
                "public_key",
                "owner_action",
            ],
        }

class OwnerAddEvent(OwnerEvent):
    def __init__(self, 
        public_key: bytes, 
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(
            public_key, 
            OwnerActionT.ADD, 
            uuid_, 
            timestamp, 
            signature, 
            signer)

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return OwnerAddEvent(
            bytes.fromhex(d["public_key"]),
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

class OwnerRemoveEvent(OwnerEvent):
    def __init__(self, 
        public_key: bytes, 
        uuid_: typing.Optional[uuid.UUID] = None, 
        timestamp: typing.Optional[str] = None,
        signature: typing.Optional[bytes] = None,
        signer: typing.Optional[bytes] = None):
        super().__init__(
            public_key, 
            OwnerActionT.REMOVE, 
            uuid_, 
            timestamp, 
            signature, 
            signer)

    @classmethod
    def load(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return OwnerRemoveEvent(
            bytes.fromhex(d["public_key"]),
            uuid.UUID(d["uuid"]),
            d["timestamp"],
            bytes.fromhex(d["signature"]), 
            bytes.fromhex(d["signer"]))

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

    def next(self, uuid_: uuid.UUID | None) -> uuid.UUID | None:
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
        handler: dict[type[Event], 
            typing.Callable[[typing.Any], typing.Any]] = {
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
        handler: dict[typing.Type[Event], 
            typing.Callable[[typing.Any], None]] = {
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
