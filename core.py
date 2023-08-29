import enum
import uuid
import datetime

def tsnow():
    return f'{datetime.datetime.utcnow().isoformat("T")}Z'

### Enums ###
class ActionT(enum.Enum):
    CREATE = 1<<0
    UPDATE = 1<<1
    DELETE = 1<<2

class HashTypeT(enum.Enum):
    SHA256 = 1<<0

### Data Containers ###
class Object:
    # def __init__(
    # self, name: str, format_: str, size: int, hash_type: HashTypeT, 
    #   hash_: str, uuid_: uuid.UUID = uuid.uuid4(), version: int = 0):
    def __init__(self, name: str, uuid_: uuid.UUID = uuid.uuid4(), 
        version: int = 0):
        self.uuid = uuid_
        self.version = version
        self.name = name
        # self.format = format_
        # self.size = size
        # self.hash_type = hash_type
        # self.hash = hash_

    def __copy__(self):
        # return Object(self.name, self.format, self.size, self.hash_type, 
        #   self.hash, self.uuid, self.version)
        return Object(self.name, self.uuid, self.version)

    def versioned_copy(self):
        copy = self.__copy__()
        copy.version += 1
        return copy

    def identifier(self):
        return Identifier(self.uuid, self.version)

class Annotation:
    # def __init__(self, schema: Identifier, hash_type: HashTypeT, hash_: str, 
    #   uuid_: uuid.UUID = uuid.uuid4(), version: int = 0):
    def __init__(self, uuid_: uuid.UUID = uuid.uuid4(), version: int = 0):
        self.uuid = uuid_
        self.version = version
        # self.schema = schema
        # self.hash_type = hash_type
        # self.hash = hash_

    def __copy__(self):
        # return Annotation(self.schema, self.hash_type, 
        #   self.hash, self.uuid, self.version)
        return Annotation(self.uuid, self.version)

    def versioned_copy(self):
        copy = self.__copy__()
        copy.version += 1
        return copy

    def identifier(self):
        return Identifier(self.uuid, self.version)

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

### Events ###
class Event:
    def __init__(self):
        self.timestamp = tsnow()
        self.uuid = uuid.uuid4()

### Object Events ###
class ObjectEvent(Event):
    def __init__(self, action: ActionT):
        super().__init__()
        self.action = action

class ObjectCreateEvent(ObjectEvent):
    def __init__(self, object_: Object):
        super().__init__(ActionT.CREATE)
        self.object = object_

class ObjectUpdateEvent(ObjectEvent):
    def __init__(self, object_: Object):
        super().__init__(ActionT.UPDATE)
        self.object = object_

class ObjectDeleteEvent(ObjectEvent):
    def __init__(self, object_identifier: Identifier):
        super().__init__(ActionT.DELETE)
        self.object_identifier = object_identifier

### Annotation Events ###
class AnnotationEvent(Event):
    def __init__(self, action: ActionT):
        super().__init__()
        self.action = action

class AnnotationCreateEvent(AnnotationEvent):
    def __init__(
        self, object_identifiers: list[Identifier], annotation: Annotation):
        super().__init__(ActionT.CREATE)
        self.object_identifiers = object_identifiers
        self.annotation = annotation

class AnnotationUpdateEvent(AnnotationEvent):
    def __init__(self, annotation: Annotation):
        super().__init__(ActionT.UPDATE)
        self.annotation = annotation

class AnnotationDeleteEvent(AnnotationEvent):
    def __init__(self, annotation_identifier: Identifier):
        super().__init__(ActionT.DELETE)
        self.annotation_identifier = annotation_identifier

### Record Keeper (Events) ###
class RecordKeeper:
    def add(event: Event):
        raise Exception("Unimplemented method.")

    def read(self, uuid_: uuid.UUID) -> Event:
        raise Exception("Unimplemented method.")

    def exists(self, uuid_: uuid.UUID) -> bool:
        raise Exception("Unimplemented method.")

### Depot (Objects) ###
class Depot:
    def reserve(identifier: str, size: int):
        raise Exception("Unimplemented method.")

    def write(identifier: str, offset: int, buf: bytes):
        raise Exception("Unimplemented method.")

    def finalize(identifier: str):
        raise Exception("Unimplemented method.")

    def read(identifier: str, offset: int, size: int):
        raise Exception("Unimplemented method.")

    def purge(identifier: str):
        raise Exception("Unimplemented method.")

### Machine ###
class Machine:
    def __init__(self, record_keeper: RecordKeeper):
        self.validators: list[Validator] = []
        self.record_keeper: RecordKeeper = record_keeper
        self.consumers: list[Consumer] = []

    def process_event(self, event):
        for v in self.validators:
            res = v.validate(event)
            if res is not None:
                raise Exception(res)

        self.record_keeper.add(event)

        for c in self.consumers:
            res = c.consume(event)
            if res is not None:
                raise Exception(res)

    def register(self, c):
        if isinstance(c, Validator):
            self.validators.append(c)

        if isinstance(c, Consumer):
            self.consumers.append(c)

class Validator:
    def validate(self, event):
        raise Exception("Unimplemented validate method.")

class Consumer:
    def consume(self, event):
        raise Exception("Unimplemented consume method.")

### State ###
class State(Validator, Consumer):
    def validate(self, event: Event) -> str:
        handler: dict[type, callable[[Event], str]] = {
            ObjectCreateEvent: self._validate_object_create,
            ObjectUpdateEvent: self._validate_object_update,
            ObjectDeleteEvent: self._validate_object_delete,
            AnnotationCreateEvent: self._validate_annotation_create,
            AnnotationUpdateEvent: self._validate_annotation_update,
            AnnotationDeleteEvent: self._validate_annotation_delete,
        }

        if type(event) not in handler:
            return 'Unhandled event type in validate.'

        return handler[type(event)](event)

    def consume(self, event: Event) -> str | None:
        handler: dict[type, callable[[Event], str]] = {
            ObjectCreateEvent: self._consume_object_create,
            ObjectUpdateEvent: self._consume_object_update,
            ObjectDeleteEvent: self._consume_object_delete,
            AnnotationCreateEvent: self._consume_annotation_create,
            AnnotationUpdateEvent: self._consume_annotation_update,
            AnnotationDeleteEvent: self._consume_annotation_delete,
        }

        if type(event) not in handler:
            return 'Unhandled event type in consume.'

        return handler[type(event)](event)

    def _validate_object_create(self, event: ObjectCreateEvent) -> str | None:
        raise Exception("Unimplemented method.")

    def _validate_object_update(self, event: ObjectCreateEvent) -> str | None:
        raise Exception("Unimplemented method.")


    def _validate_object_delete(self, event: ObjectDeleteEvent) -> str | None:
        raise Exception("Unimplemented method.")

    def _validate_annotation_create(
        self, event: AnnotationCreateEvent) -> str | None:
        raise Exception("Unimplemented method.")

    def _validate_annotation_update(
        self, event: AnnotationUpdateEvent) -> str | None:
        raise Exception("Unimplemented method.")

    def _validate_annotation_delete(
        self, event: AnnotationDeleteEvent) -> str | None:
        raise Exception("Unimplemented method.")

    def _consume_object_create(self, event: ObjectCreateEvent) -> str | None:
        raise Exception("Unimplemented method.")

    def _consume_object_update(self, event: ObjectUpdateEvent) -> str | None:
       raise Exception("Unimplemented method.")

    def _consume_object_delete(self, event: ObjectDeleteEvent) -> str | None:
        raise Exception("Unimplemented method.")

    def _consume_annotation_create(
        self, event: AnnotationCreateEvent) -> str | None:
        raise Exception("Unimplemented method.")

    def _consume_annotation_update(
        self, event: AnnotationUpdateEvent) -> str | None:
        raise Exception("Unimplemented method.")

    def _consume_annotation_delete(
        self, event: AnnotationDeleteEvent) -> str | None:
        raise Exception("Unimplemented method.")