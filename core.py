import abc
import json
import uuid
import typing
import threading
import jsonschema

import events

def is_schema(name: str) -> bool:
    return name.startswith("schema-")

class ValidationError(Exception):
    pass

class StorageError(Exception):
    pass

### Machine ###
class Machine:
    def __init__(self):
        self.validators: list[Validator] = []
        self.consumers: list[Consumer] = []
        self.lock = threading.Lock()

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
            raise ValidationError("event UUID already exists")

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

### Depot ###
class Depot(abc.ABC):
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
class State(Validator, Consumer, abc.ABC):
    def validate(self, event: events.EventT):
        handler: dict[type[events.Event],
            typing.Callable[[typing.Any], typing.Any]] = {
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

class FieldValidator(Validator):
    def validate(self, event: events.EventT):
        handler: dict[typing.Type[events.Event],
            typing.Callable[[typing.Any], None]] = {
            events.ObjectCreateEvent: self._validate_object,
            events.ObjectUpdateEvent: self._validate_object,
            events.AnnotationCreateEvent: self._validate_annotation,
            events.AnnotationUpdateEvent: self._validate_annotation,
        }

        if type(event) not in handler:
            return

        handler[type(event)](event)

    def _validate_object(self,
        event: events.ObjectCreateEvent|events.ObjectUpdateEvent):

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

        if object_.hash_type != events.HashTypeT.SHA256:
            raise ValidationError("hash type must be SHA256")

        if len(object_.hash) != 64:
            raise ValidationError("hash should be a hex encoded SHA256")

    def _validate_annotation(self,
        event: events.AnnotationCreateEvent|events.AnnotationUpdateEvent):
        annotation = event.annotation
        if not isinstance(annotation.uuid, uuid.UUID):
            raise ValidationError("UUID is not of type uuid.UUID")

        if annotation.version < 0:
            raise ValidationError("version must be a positive integer")

        if annotation.size < 0:
            raise ValidationError("size must be a non-negative integer")

        if not isinstance(annotation.schema, events.Identifier):
            raise ValidationError("schema must be an identifier")

        if annotation.hash_type != events.HashTypeT.SHA256:
            raise ValidationError("hash type must be SHA256")

        if len(annotation.hash) != 64:
            raise ValidationError("hash should be a hex encoded SHA256")

class SchemaValidator(Validator, Consumer):
    def __init__(self, depot: Depot):
        super().__init__()
        self.depot: Depot = depot
        self.schemas: set[events.Identifier] = set()

    def validate(self, event: events.EventT):
        handler: dict[typing.Type[events.Event],
            typing.Callable[[typing.Any], None]] = {
            events.ObjectCreateEvent: self._validate_object_create,
            events.ObjectUpdateEvent: self._validate_object_update,
            events.AnnotationCreateEvent: self._validate_annotation_create,
            events.AnnotationUpdateEvent: self._validate_annotation_update,
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

    def _validate_object_create(self, event: events.ObjectCreateEvent):
        self._validate_object(event.object)

    def _validate_object_update(self, event: events.ObjectCreateEvent):
        self._validate_object(event.object)

    def _validate_annotation_create(
        self, event: events.AnnotationCreateEvent):
        self._validate_annotation(event.annotation)

    def _validate_annotation_update(
        self, event: events.AnnotationUpdateEvent):
        self._validate_annotation(event.annotation)

    def consume(self, event: events.EventT):
        handler: dict[typing.Type[events.Event],
            typing.Callable[[typing.Any], None]] = {
            events.ObjectCreateEvent: self._consume_object_create,
            events.ObjectUpdateEvent: self._consume_object_update,
        }

        if type(event) not in handler:
            return

        handler[type(event)](event)

    def _consume_object(self, object_: events.Object):
        if not is_schema(object_.name):
            return

        if object_.format != "application/schema+json":
            return

        self.schemas.add(object_.identifier())

    def _consume_object_create(self, event: events.ObjectCreateEvent):
        self._consume_object(event.object)

    def _consume_object_update(self, event: events.ObjectUpdateEvent):
        self._consume_object(event.object)
