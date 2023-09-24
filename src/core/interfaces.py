import abc
import json
import uuid
import typing
import jsonschema
import multiprocessing

from gonk.core import events
from gonk.core import exceptions

### Machine ###
class Machine:
    def __init__(self):
        self.validators: list[Validator] = []
        self.consumers: list[Consumer] = []
        self.lock = multiprocessing.Lock()

    def process_event(self, event):
        with self.lock:
            for validator in self.validators:
                validator.validate(event)

            for consumer in self.consumers:
                consumer.consume(event)

    def register(self, worker):
        registered = False

        if isinstance(worker, Validator):
            self.validators.append(worker)
            registered = True


        if isinstance(worker, Consumer):
            self.consumers.append(worker)
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
    def validate(self, event: events.EventT):
        if self.exists(event.uuid):
            raise exceptions.ValidationError("event UUID already exists")

    def consume(self, event: events.EventT):
        self.add(event)

    @abc.abstractmethod
    def add(self, event: events.EventT):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def read(self, uuid_: uuid.UUID) -> events.Event:
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def exists(self, uuid_: uuid.UUID) -> bool:
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def next(self, uuid_: uuid.UUID | None) -> uuid.UUID | None:
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def tail(self) -> uuid.UUID | None:
        raise NotImplementedError("unimplemented method")

### Depot ###
class Depot(abc.ABC):
    @abc.abstractmethod
    def exists(self, identifier: events.Identifier):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def reserve(self, identifier: events.Identifier, size: int):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def write(self, identifier: events.Identifier, offset: int, buf: bytes):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def finalize(self, identifier: events.Identifier):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def read(self, identifier: events.Identifier, offset: int, size: int):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def purge(self, identifier: events.Identifier):
        raise NotImplementedError("unimplemented method")

### State ###
class SchemaInfo:
    def __init__(self, name: str, uuid_: uuid.UUID, versions: int):
        self.name = name
        self.uuid = uuid_
        self.versions = versions

    def serialize(self):
        return {
            "name": self.name,
            "uuid": str(self.uuid),
            "versions": self.versions,
        }

class ObjectInfo:
    def __init__(self, uuid_: uuid.UUID, versions: int):
        self.uuid = uuid_
        self.versions = versions

    def serialize(self):
        return {
            "uuid": str(self.uuid),
            "versions": self.versions,
        }

class AnnotationInfo:
    def __init__(self, uuid_: uuid.UUID, versions: int):
        self.uuid = uuid_
        self.versions = versions

    def serialize(self):
        return {
            "uuid": str(self.uuid),
            "versions": self.versions,
        }

class EventInfo:
    def __init__(self, uuid_: uuid.UUID, type_: str, review: None|str):
        self.uuid = uuid_
        self.type = type_
        self.review = review

    def serialize(self):
        return {
            "uuid": str(self.uuid),
            "type": self.type,
            "review": self.review,
        }

    def __repr__(self):
        return f"EventInfo({self.uuid}, {self.type}, {self.review})"

class State(Validator, Consumer, abc.ABC):
    def validate(self, event: events.EventT):
        handler: dict[type[events.Event],
            typing.Callable[[typing.Any], None]] = {
            events.ObjectCreateEvent: self._validate_object_create,
            events.ObjectUpdateEvent: self._validate_object_update,
            events.ObjectDeleteEvent: self._validate_object_delete,
            events.AnnotationCreateEvent: self._validate_annotation_create,
            events.AnnotationUpdateEvent: self._validate_annotation_update,
            events.AnnotationDeleteEvent: self._validate_annotation_delete,
            events.ReviewAcceptEvent: self._validate_review_accept,
            events.ReviewRejectEvent: self._validate_review_reject,
            events.OwnerAddEvent: self._validate_owner_add,
            events.OwnerRemoveEvent: self._validate_owner_remove,
        }

        if type(event) not in handler:
            raise NotImplementedError("unhandled event type in validate")

        handler[type(event)](event)

    @abc.abstractmethod
    def events_by_object(self, identifier: events.Identifier):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def events_by_annotation(self, identifier: events.Identifier):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def events_all(self, after: None|uuid.UUID = None):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def annotations_all(self, 
        uuid_: None|uuid.UUID = None, after: None|uuid.UUID = None):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def annotations_by_object(self, object_identifier: events.Identifier):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def annotations_by_status(self, status: str, after: None|uuid.UUID = None):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def annotation(self, identifier: events.Identifier):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def objects_all(self, 
        uuid_: None|uuid.UUID = None, after: None|uuid.UUID = None):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def objects_by_annotation(self, annotation_uuid: uuid.UUID):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def objects_by_status(self, status: str, after: None|uuid.UUID = None):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def object(self, identifier: events.Identifier):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def schemas_all(self, name: None|str =None):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def schema(self, name: str, version: int):
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def owners(self) -> list[str]:
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_object_create(self, event: events.ObjectCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_object_update(self, event: events.ObjectCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_object_delete(self, event: events.ObjectDeleteEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_annotation_create(self, event: events.AnnotationCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_annotation_update(self, event: events.AnnotationUpdateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_annotation_delete(self, event: events.AnnotationDeleteEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_review_accept(self, event: events.ReviewAcceptEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_review_reject(self, event: events.ReviewRejectEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_owner_add(self, event: events.OwnerAddEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_owner_remove(self, event: events.OwnerRemoveEvent):
        raise NotImplementedError("unimplemented method")

    def consume(self, event: events.EventT):
        handler: dict[typing.Type[events.Event],
            typing.Callable[[typing.Any], None]] = {
            events.ObjectCreateEvent: self._consume_object_create,
            events.ObjectUpdateEvent: self._consume_object_update,
            events.ObjectDeleteEvent: self._consume_object_delete,
            events.AnnotationCreateEvent: self._consume_annotation_create,
            events.AnnotationUpdateEvent: self._consume_annotation_update,
            events.AnnotationDeleteEvent: self._consume_annotation_delete,
            events.ReviewAcceptEvent: self._consume_review_accept,
            events.ReviewRejectEvent: self._consume_review_reject,
            events.OwnerAddEvent: self._consume_owner_add,
            events.OwnerRemoveEvent: self._consume_owner_remove,
        }

        if type(event) not in handler:
            raise NotImplementedError("unhandled event type in consume")

        handler[type(event)](event)

    @abc.abstractmethod
    def _consume_object_create(self, event: events.ObjectCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_object_update(self, event: events.ObjectUpdateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_object_delete(self, event: events.ObjectDeleteEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_annotation_create(self, event: events.AnnotationCreateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_annotation_update(self, event: events.AnnotationUpdateEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_annotation_delete(self, event: events.AnnotationDeleteEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_review_accept(self, event: events.ReviewAcceptEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_review_reject(self, event: events.ReviewRejectEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_owner_add(self, event: events.OwnerAddEvent):
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_owner_remove(self, event: events.OwnerRemoveEvent):
        raise NotImplementedError("unimplemented method")

