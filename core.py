import abc
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

    def serialize(self):
        return {
            "uuid": str(self.uuid),
            "version": self.version,
        }

    @classmethod
    def deserialize(cls, d: dict):
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

    def serialize(self):
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
    def deserialize(cls, d: dict):
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

    def serialize(self):
        return {
            "uuid": str(self.uuid),
            "version": self.version,
            "schema": self.schema_.serialize(),
            "size": self.size,
            "hash_type": self.hash_type.value,
            "hash": self.hash,
        }

    @classmethod
    def deserialize(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return Annotation(
            Identifier.deserialize(d["schema"]), 
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

    def serialize(self):
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
    def deserialize(cls, d: dict):
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
json serialize
json deserialize
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

    def serialize(self):
        return super().serialize() | {
            "action": self.action.value,
        }

    @classmethod
    def deserialize(cls, d: dict):
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

    def serialize(self):
        return super().serialize() | {
            "object": self.object.serialize(),
        }

    @classmethod
    def deserialize(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ObjectCreateEvent(Object.deserialize(d["object"]),
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

    def serialize(self):
        return super().serialize() | {
            "object": self.object.serialize(),
        }

    @classmethod
    def deserialize(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ObjectUpdateEvent(Object.deserialize(d["object"]),
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

    def serialize(self):
        return super().serialize() | {
            "object_identifier": self.object_identifier.serialize(),
        }

    @classmethod
    def deserialize(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return ObjectDeleteEvent(Identifier.deserialize(d["object_identifier"]),
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

    def serialize(self):
        return super().serialize() | {
            "action": self.action.value,
        }

    @classmethod
    def deserialize(cls, d: dict):
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

    def serialize(self):
        return super().serialize() | {
            "annotation": self.annotation.serialize(),
            "object_identifiers": [ea.serialize() for ea in self.object_identifiers],
        }

    @classmethod
    def deserialize(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return AnnotationCreateEvent(
            [Identifier.deserialize(ea) for ea in d["object_identifiers"]],
            Annotation.deserialize(d["annotation"]),
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

    def serialize(self):
        return super().serialize() | {
            "annotation": self.annotation.serialize(),
        }

    @classmethod
    def deserialize(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return AnnotationUpdateEvent(
            Annotation.deserialize(d["annotation"]),
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

    def serialize(self):
        return super().serialize() | {
            "annotation_identifier": self.annotation_identifier.serialize(),
        }

    @classmethod
    def deserialize(cls, d: dict):
        jsonschema.validate(instance=d, schema=cls.schema())
        return AnnotationDeleteEvent(
            Identifier.deserialize(d["annotation_identifier"]),
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

    def serialize(self):
        return super().serialize() | {
            "decision": self.decision.value,
        }

    @classmethod
    def deserialize(cls, d: dict):
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

    def serialize(self):
        return super().serialize() | {
            "event_uuid": str(self.event_uuid),
        }

    @classmethod
    def deserialize(cls, d: dict):
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

    def serialize(self):
        return super().serialize() | {
            "event_uuid": str(self.event_uuid),
        }

    @classmethod
    def deserialize(cls, d: dict):
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

    def serialize(self):
        return super().serialize() | {
            "public_key": self.public_key.hex(),
            "owner_action": self.owner_action.value,
        }

    @classmethod
    def deserialize(cls, d: dict):
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
    def deserialize(cls, d: dict):
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
    def deserialize(cls, d: dict):
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
        registered = False

        if isinstance(c, Validator):
            self.validators.append(c)
            registered = True


        if isinstance(c, Consumer):
            self.consumers.append(c)
            registered = True

        if not registered:
            raise ValueError("not a consumer or validator")

class Validator(abc.ABC):
    @abc.abstractmethod
    def validate(self, event):
        raise NotImplementedError("unimplemented method")

class Consumer(abc.ABC):
    @abc.abstractmethod
    def consume(self, event):
        raise NotImplementedError("unimplemented method")

### Record Keeper ###
class RecordKeeper(Consumer, Validator, abc.ABC):
    def validate(self, event: Event):
        if self.exists(event.uuid):
            raise ValidationError("event UUID already exists")

    def consume(self, event: Event):
        self.add(event)

    @abc.abstractmethod
    def add(self, event: Event):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def read(self, uuid_: uuid.UUID) -> Event:
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def exists(self, uuid_: uuid.UUID) -> bool:
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def next(self, uuid_: uuid.UUID | None) -> uuid.UUID | None:
        raise NotImplementedError("unimplemented method")

### Depot ###
class Depot(abc.ABC):
    @abc.abstractmethod
    def reserve(self, identifier: Identifier, size: int):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def write(self, identifier: Identifier, offset: int, buf: bytes):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def finalize(self, identifier: Identifier):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def read(self, identifier: Identifier, offset: int, size: int):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def purge(self, identifier: Identifier):
        raise NotImplementedError("unimplemented method")

### State ###
class State(Validator, Consumer, abc.ABC):
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

    @abc.abstractmethod
    def _validate_object_create(self, event: ObjectCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_object_update(self, event: ObjectCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_object_delete(self, event: ObjectDeleteEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_annotation_create(self, event: AnnotationCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_annotation_update(self, event: AnnotationUpdateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_annotation_delete(self, event: AnnotationDeleteEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_review_accept(self, event: ReviewAcceptEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_review_reject(self, event: ReviewRejectEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_owner_add(self, event: OwnerAddEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_owner_remove(self, event: OwnerRemoveEvent):
        raise NotImplementedError("unimplemented method")

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

    @abc.abstractmethod
    def _consume_object_create(self, event: ObjectCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_object_update(self, event: ObjectUpdateEvent):
       raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_object_delete(self, event: ObjectDeleteEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_annotation_create(self, event: AnnotationCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_annotation_update(self, event: AnnotationUpdateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_annotation_delete(self, event: AnnotationDeleteEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_review_accept(self, event: ReviewAcceptEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_review_reject(self, event: ReviewRejectEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_owner_add(self, event: OwnerAddEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
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

### Types ###

EventT = typing.TypeVar("EventT", bound=Event)