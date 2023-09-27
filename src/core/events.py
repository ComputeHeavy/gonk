# Copyright 2023 - Compute Heavy Industries Incorporated
# This work is released, distributed, and licensed under AGPLv3.

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
    """Enum for object and annotation event actions."""
    CREATE = 1<<0
    """Create event."""
    UPDATE = 1<<1
    """Update event."""
    DELETE = 1<<2
    """Delete event."""

class DecisionT(enum.Enum):
    """Enum for review event decisions."""
    ACCEPT = 1<<0
    """Event accepted."""
    REJECT = 1<<1
    """Event rejected."""

class OwnerActionT(enum.Enum):
    """Enum for owner event actions."""
    ADD = 1<<0
    """Owner added."""
    REMOVE = 1<<1
    """Owner removed."""

class HashTypeT(enum.Enum):
    """Enum for object and annotation hash types."""
    SHA256 = 1<<0
    """SHA256 hash is best hash."""

### Data Containers ###
class Identifier:
    """Identifies objects and annotations with a UUID and version number."""
    def __init__(self, uuid_: uuid.UUID, version: int):
        self.uuid: uuid.UUID = uuid_
        """Object or annotation's UUID."""
        self.version: int = version
        """Object or annotation's version."""

    def signature_bytes(self) -> bytes:
        """Return a byte-based representation for signing or hashing."""
        return b"".join([
            self.uuid.bytes,
            struct.pack("<Q", self.version),
        ])

    def serialize(self) -> dict:
        """Serialize instance to dictionary."""
        return {
            "uuid": str(self.uuid),
            "version": self.version,
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        """Deserialize dictionary to instance."""
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(uuid.UUID(data["uuid"]), data["version"])

    @staticmethod
    def schema(relative="") -> dict:
        """Returns the JSON Schema for validating the serialized class.

        Args:
            relative: A base path for nested references.
        """
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

    def __hash__(self):
        return hash((self.uuid, self.version))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.uuid == other.uuid and self.version == other.version

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f"Identifier({self.uuid}, {self.version})"

class Object:
    """Metadata container for an object in the 
        :class:`gonk.core.interfaces.Depot`."""
    def __init__(self, name: str, format_: str, size: int, hash_type: HashTypeT,
        hash_: str, uuid_: typing.Optional[uuid.UUID] = None, version: int = 0):
        if uuid_ is None:
            uuid_ = uuid.uuid4()

        self.uuid: uuid.UUID = uuid_
        """Object's UUID."""
        self.version: int = version
        """Object's version."""
        self.name: str = name
        """Object's filename."""
        self.format: str = format_
        """Object's mimetype."""
        self.size: int = size
        """Object size in bytes."""
        self.hash_type: HashTypeT = hash_type
        """HashTypeT of the hash."""
        self.hash: str = hash_
        """Hex encoded hash of the object."""

    def identifier(self) -> Identifier:
        """Return the object identifier."""
        return Identifier(self.uuid, self.version)

    def signature_bytes(self) -> bytes:
        """Return a byte-based representation for signing or hashing."""
        return b"".join([
            self.uuid.bytes,
            struct.pack("<Q", self.version),
            self.name.encode(),
            self.format.encode(),
            struct.pack("<Q", self.size),
            struct.pack("<B", self.hash_type.value),
            bytes.fromhex(self.hash),
        ])

    def serialize(self) -> dict:
        """Serialize instance to dictionary."""
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
    def deserialize(cls, data: dict) -> typing.Self:
        """Deserialize dictionary to instance."""
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            data["name"],
            data["format"],
            data["size"],
            HashTypeT(data["hash_type"]),
            data["hash"],
            uuid.UUID(data["uuid"]),
            data["version"])

    @staticmethod
    def schema(relative="") -> dict:
        """Returns the JSON Schema for validating the serialized class.

        Args:
            relative: A base path for nested references.
        """
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

class Annotation:
    """Metadata container for an annotation in the 
        :class:`gonk.core.interfaces.Depot`."""
    def __init__(self, schema_: Identifier, size: int, hash_type: HashTypeT,
        hash_: str, uuid_: typing.Optional[uuid.UUID] = None, version: int = 0):
        if uuid_ is None:
            uuid_ = uuid.uuid4()

        self.uuid: uuid.UUID = uuid_ 
        """Annotation's UUID."""
        self.version: int = version
        """Annotation's version."""
        self.schema_: Identifier = schema_
        """Schema identifier."""
        self.size: int = size
        """Annotation size in bytes."""
        self.hash_type: HashTypeT = hash_type
        """Annotation's hash type."""
        self.hash: str = hash_
        """Hex encoded hash."""

    def identifier(self) -> Identifier:
        """Return the identifier for this annotation."""
        return Identifier(self.uuid, self.version)

    def signature_bytes(self) -> bytes:
        """Return a byte-based representation for signing or hashing."""
        return b"".join([
            self.uuid.bytes,
            struct.pack("<Q", self.version),
            self.schema_.signature_bytes(),
            struct.pack("<Q", self.size),
            struct.pack("<B", self.hash_type.value),
            bytes.fromhex(self.hash),
        ])

    def serialize(self) -> dict:
        """Serialize instance to dictionary."""
        return {
            "uuid": str(self.uuid),
            "version": self.version,
            "schema": self.schema_.serialize(),
            "size": self.size,
            "hash_type": self.hash_type.value,
            "hash": self.hash,
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        """Deserialize dictionary to instance."""
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            Identifier.deserialize(data["schema"]),
            data["size"],
            HashTypeT(data["hash_type"]),
            data["hash"],
            uuid.UUID(data["uuid"]),
            data["version"])

    @staticmethod
    def schema(relative="") -> dict:
        """Returns the JSON Schema for validating the serialized class.

        Args:
            relative: A base path for nested references.
        """
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

    def __eq__(self, other):
        return self.uuid == other.uuid and \
            self.version == other.version and \
            self.schema_ == other.schema_ and \
            self.size == other.size and \
            self.hash_type == other.hash_type and \
            self.hash == other.hash

    def __ne__(self, other):
        return not self.__eq__(other)

    def __copy__(self):
        return Annotation(self.schema_, self.size, self.hash_type, self.hash,
            self.uuid, self.version)

### Events ###
class Event:
    """Parent class for all event types."""
    def __init__(self,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        if uuid_ is None:
            uuid_ = uuid.uuid4()

        if timestamp is None:
            timestamp = tsnow()

        self.uuid: uuid.UUID = uuid_
        """Event's UUID."""
        self.timestamp: str = timestamp
        """Event's timestamp."""
        self.integrity: typing.Optional[bytes] = integrity
        """Byte field for hashes or signatures."""
        self.author: typing.Optional[str] = author
        """String field for the event's author."""
    def signature_bytes(self) -> bytes:
        """Return a byte-based representation for signing or hashing."""
        return b"".join([
            self.uuid.bytes,
            self.timestamp.encode(),
        ])

    def serialize(self) -> dict:
        """Serialize instance to dictionary."""
        integrity = None
        if self.integrity is not None:
            integrity = self.integrity.hex()

        return {
            "uuid": str(self.uuid),
            "timestamp": self.timestamp,
            "integrity": integrity,
            "author": self.author,
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        """Deserialize dictionary to instance."""
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
        """Returns the JSON Schema for validating the serialized class.

        Args:
            relative: A base path for nested references.
        """
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
                "integrity": {
                    "type": "string",
                    "minLength": 32,
                    "maxLength": 256,
                    "pattern": "^[0-9a-fA-F]{32,256}$"
                },
                "author": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 512,
                },
            },
            "required": [
                "uuid",
                "timestamp",
                "integrity",
                "author",
            ],
        }

    def __eq__(self, other):
        return self.uuid == other.uuid and \
            self.timestamp == other.timestamp and \
            self.integrity == other.integrity and \
            self.author == other.author

    def __ne__(self, other):
        return not self.__eq__(other)

EventT = typing.TypeVar("EventT", bound=Event)

### Object Events ###
class ObjectEvent(Event):
    """Parent class for object-specific events (create, delete, update)."""
    def __init__(self,
        action: ActionT,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        super().__init__(uuid_, timestamp, integrity, author)
        self.action: ActionT = action
        """The event action."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            struct.pack("<B", self.action.value),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "action": self.action.value,
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(ActionT(data["action"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.action == other.action

    def __ne__(self, other):
        return not self.__eq__(other)

class ObjectCreateEvent(ObjectEvent):
    """Event used for object creation."""
    def __init__(self,
        object_: Object,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):
        super().__init__(ActionT.CREATE, uuid_, timestamp, integrity, author)

        self.object: Object = object_
        """The :class:`Object` being created."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.object.signature_bytes(),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "object": self.object.serialize(),
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(Object.deserialize(data["object"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.object == other.object

    def __ne__(self, other):
        return not self.__eq__(other)

class ObjectUpdateEvent(ObjectEvent):
    """Event used for object updates."""
    def __init__(self,
        object_: Object,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):
        
        super().__init__(ActionT.UPDATE, uuid_, timestamp, integrity, author)
        self.object: Object = object_
        """The updated version of the :class:`Object`."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.object.signature_bytes(),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "object": self.object.serialize(),
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(Object.deserialize(data["object"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.object == other.object

    def __ne__(self, other):
        return not self.__eq__(other)

class ObjectDeleteEvent(ObjectEvent):
    """Event used for object deletion."""
    def __init__(self,
        object_identifier: Identifier,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        super().__init__(ActionT.DELETE, uuid_, timestamp, integrity, author)
        self.object_identifier: Identifier = object_identifier
        """The deleted :class:`Object`'s :class:`Identifier`."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.object_identifier.signature_bytes(),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "object_identifier": self.object_identifier.serialize(),
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            Identifier.deserialize(data["object_identifier"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.object_identifier == other.object_identifier

    def __ne__(self, other):
        return not self.__eq__(other)

### Annotation Events ###
class AnnotationEvent(Event):
    """Parent class for annotation-specific events (create, update, delete)."""
    def __init__(self,
        action: ActionT,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        super().__init__(uuid_, timestamp, integrity, author)
        self.action: ActionT = action
        """The event action."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            struct.pack("<B", self.action.value),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "action": self.action.value,
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(ActionT(data["action"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.action == other.action

    def __ne__(self, other):
        return not self.__eq__(other)

class AnnotationCreateEvent(AnnotationEvent):
    """Event used for creating an annotation."""
    def __init__(self,
        object_identifiers: list[Identifier],
        annotation: Annotation,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        super().__init__(ActionT.CREATE, uuid_, timestamp, integrity, author)
        self.object_identifiers: list[Identifier] = object_identifiers
        """Object :class:`Identifier`\s that this annotation references."""
        self.annotation: Annotation = annotation
        """The :class:`Annotation` being created."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            *[ea.signature_bytes() for ea in self.object_identifiers],
            self.annotation.signature_bytes(),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "annotation": self.annotation.serialize(),
            "object_identifiers": [ea.serialize() 
                for ea in self.object_identifiers],
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            [Identifier.deserialize(ea) for ea in data["object_identifiers"]],
            Annotation.deserialize(data["annotation"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.object_identifiers == other.object_identifiers and \
            self.annotation == other.annotation

    def __ne__(self, other):
        return not self.__eq__(other)

class AnnotationUpdateEvent(AnnotationEvent):
    """Event used for updating an annotation."""
    def __init__(self,
        annotation: Annotation,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        super().__init__(ActionT.UPDATE, uuid_, timestamp, integrity, author)
        self.annotation = annotation
        """The :class:`Annotation` being updated."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.annotation.signature_bytes(),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "annotation": self.annotation.serialize(),
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            Annotation.deserialize(data["annotation"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.annotation == other.annotation

    def __ne__(self, other):
        return not self.__eq__(other)

class AnnotationDeleteEvent(AnnotationEvent):
    """Event used for deleting an annotation."""
    def __init__(self,
        annotation_identifier: Identifier,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        super().__init__(ActionT.DELETE, uuid_, timestamp, integrity, author)
        self.annotation_identifier: Identifier = annotation_identifier
        """The :class:`Identifier` of the annotation being deleted."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.annotation_identifier.signature_bytes(),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "annotation_identifier": self.annotation_identifier.serialize(),
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            Identifier.deserialize(data["annotation_identifier"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.annotation_identifier == other.annotation_identifier

    def __ne__(self, other):
        return not self.__eq__(other)

### Review Events ###
class ReviewEvent(Event):
    """Parent class for review events."""
    def __init__(self,
        decision: DecisionT,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        super().__init__(uuid_, timestamp, integrity, author)
        self.decision: DecisionT = decision
        """The decision of the review (accept, reject)."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            struct.pack("<B", self.decision.value),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "decision": self.decision.value,
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            DecisionT(data["decision"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.decision == other.decision

    def __ne__(self, other):
        return not self.__eq__(other)

class ReviewAcceptEvent(ReviewEvent):
    """Event used for accepting pending object and annotation events."""
    def __init__(self,
        event_uuid: uuid.UUID,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        super().__init__(DecisionT.ACCEPT, uuid_, timestamp, integrity, author)
        self.event_uuid: uuid.UUID = event_uuid
        """The change being accepted."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.event_uuid.bytes,
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "event_uuid": str(self.event_uuid),
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            uuid.UUID(data["event_uuid"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.event_uuid == other.event_uuid

    def __ne__(self, other):
        return not self.__eq__(other)

class ReviewRejectEvent(ReviewEvent):
    """Event used for rejecting pending object and annotation events."""
    def __init__(self,
        event_uuid: uuid.UUID,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):
    
        super().__init__(DecisionT.REJECT, uuid_, timestamp, integrity, author)
        self.event_uuid: uuid.UUID = event_uuid
        """The change being rejected."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.event_uuid.bytes,
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "event_uuid": str(self.event_uuid),
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            uuid.UUID(data["event_uuid"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.event_uuid == other.event_uuid

    def __ne__(self, other):
        return not self.__eq__(other)

### Ownership Events ###
class OwnerEvent(Event):
    """Parent class for owner events."""
    def __init__(self,
        owner: str,
        owner_action: OwnerActionT,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):

        super().__init__(uuid_, timestamp, integrity, author)
        self.owner: str = owner
        """The owner affected."""
        self.owner_action: OwnerActionT = owner_action
        """Whether the owner is being added or removed."""

    def signature_bytes(self) -> bytes:
        return b"".join([
            super().signature_bytes(),
            self.owner.encode(),
            struct.pack("<B", self.owner_action.value),
        ])

    def serialize(self) -> dict:
        return super().serialize() | {
            "owner": self.owner,
            "owner_action": self.owner_action.value,
        }

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            data["owner"],
            OwnerActionT(data["owner_action"]),
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

    @staticmethod
    def schema(relative="") -> dict:
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
                "owner": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 512,
                },
            },
            "required": [
                "owner",
                "owner_action",
            ],
        }

    def __eq__(self, other):
        return super().__eq__(other) and \
            self.owner == other.owner and \
            self.owner_action == other.owner_action

    def __ne__(self, other):
        return not self.__eq__(other)

class OwnerAddEvent(OwnerEvent):
    """Event used for adding an owner."""
    def __init__(self,
        owner: str,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):
        super().__init__(
            owner,
            OwnerActionT.ADD,
            uuid_,
            timestamp,
            integrity,
            author)

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            data["owner"],
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])

class OwnerRemoveEvent(OwnerEvent):
    """Event used for removing an owner."""
    def __init__(self,
        owner: str,
        uuid_: typing.Optional[uuid.UUID] = None,
        timestamp: typing.Optional[str] = None,
        integrity: typing.Optional[bytes] = None,
        author: typing.Optional[str] = None):
        super().__init__(
            owner,
            OwnerActionT.REMOVE,
            uuid_,
            timestamp,
            integrity,
            author)

    @classmethod
    def deserialize(cls, data: dict) -> typing.Self:
        jsonschema.validate(instance=data, schema=cls.schema())
        return cls(
            data["owner"],
            uuid.UUID(data["uuid"]),
            data["timestamp"],
            bytes.fromhex(data["integrity"]),
            data["author"])