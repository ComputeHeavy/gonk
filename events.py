import uuid
import enum
import typing
import struct
import datetime
import jsonschema

def tsnow() -> str:
    return f"{datetime.datetime.utcnow().isoformat('T')}Z"

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return Identifier(uuid.UUID(data["uuid"]), data["version"])

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return Object(
            data["name"],
            data["format"],
            data["size"],
            HashTypeT(data["hash_type"]),
            data["hash"],
            uuid.UUID(data["uuid"]),
            data["version"])

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return Annotation(
            Identifier.deserialize(data["schema"]),
            data["size"],
            HashTypeT(data["hash_type"]),
            data["hash"],
            uuid.UUID(data["uuid"]),
            data["version"])

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return Event(uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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

EventT = typing.TypeVar("EventT", bound=Event)

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return ObjectEvent(ActionT(data["action"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return ObjectCreateEvent(Object.deserialize(data["object"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return ObjectUpdateEvent(Object.deserialize(data["object"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return ObjectDeleteEvent(Identifier.deserialize(data["object_identifier"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return AnnotationEvent(ActionT(data["action"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return AnnotationCreateEvent(
            [Identifier.deserialize(ea) for ea in data["object_identifiers"]],
            Annotation.deserialize(data["annotation"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return AnnotationUpdateEvent(
            Annotation.deserialize(data["annotation"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return AnnotationDeleteEvent(
            Identifier.deserialize(data["annotation_identifier"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return ReviewEvent(
            DecisionT(data["decision"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return ReviewAcceptEvent(
            uuid.UUID(data["event_uuid"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return ReviewRejectEvent(
            uuid.UUID(data["event_uuid"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return OwnerEvent(
            bytes.fromhex(data["public_key"]),
            OwnerActionT(data["owner_action"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return OwnerAddEvent(
            bytes.fromhex(data["public_key"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))

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
    def deserialize(cls, data: dict):
        jsonschema.validate(instance=data, schema=cls.schema())
        return OwnerRemoveEvent(
            bytes.fromhex(data["public_key"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["signature"]),
            bytes.fromhex(data["signer"]))