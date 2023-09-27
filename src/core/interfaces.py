# Copyright 2023 - Compute Heavy Industries Incorporated
# This work is released, distributed, and licensed under AGPLv3.

import abc
import json
import uuid
import typing
import jsonschema
import multiprocessing

from gonk.core import exceptions
from gonk.core import events

### Machine ###
class Machine:
    """Machine coordinates event validation and consumption."""
    def __init__(self):
        self.validators: list[Validator] = []
        """:class:`Validator`\s that have been registered."""
        self.consumers: list[Consumer] = []
        """:class:`Consumer`\s that have been registered."""
        self.lock = multiprocessing.Lock()
        """**DO NOT CONSIDER THIS THREAD SAFE WITHOUT FURTHER TESTING**"""

    def process_event(self, event):
        """Runs registered validators and consumers."""
        with self.lock:
            for validator in self.validators:
                validator.validate(event)

            for consumer in self.consumers:
                consumer.consume(event)

    def register(self, worker):
        """Registers a class instance as a validator, consumer, or both."""
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
    """Abstract class for validators."""
    @abc.abstractmethod
    def validate(self, event):
        """Validate the event.

        Raises:
            ValidationError: Invalid event."""
        raise NotImplementedError("unimplemented method")

class Consumer(abc.ABC):
    """Abstract class for consumers."""
    @abc.abstractmethod
    def consume(self, event):
        """Consume the event."""
        raise NotImplementedError("unimplemented method")

### Record Keeper ###
class RecordKeeper(Consumer, Validator, abc.ABC):
    """A RecordKeeper acts as the :class:`gonk.core.events.Event` 
        storage for the dataset."""
    def validate(self, event: events.EventT):
        if self.exists(event.uuid):
            raise exceptions.ValidationError("event UUID already exists")

    def consume(self, event: events.EventT):
        self.add(event)

    @abc.abstractmethod
    def add(self, event: events.EventT):
        """Add a :class:`gonk.core.events.Event` to storage."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def exists(self, uuid_: uuid.UUID) -> bool:
        """Check :class:`gonk.core.events.Event` existence."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def read(self, uuid_: uuid.UUID) -> events.Event:
        """Read a :class:`gonk.core.events.Event` from storage.

        Raises:
            ValueError: Event does not exist."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def next(self, uuid_: uuid.UUID | None) -> uuid.UUID | None:
        """Get the next :class:`gonk.core.events.Event`'s 
            UUID in the event history.

        Returns:
            The next event UUID in the event history 
                or None if the provided UUID is the tail.

        Raises:
            ValueError: Event does not exist."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def tail(self) -> uuid.UUID | None:
        """Get the last :class:`gonk.core.events.Event`'s 
            UUID in the event history.

        Returns:
            The last event UUID in the event history 
                or None when there are no events."""
        raise NotImplementedError("unimplemented method")

### Depot ###
class Depot(abc.ABC):
    """A Depot acts as the object (and annotation) storage for a dataset.

        The Depot interface is a little clunky. It was designed to be 
        compatible with S3's multipart upload. This was done to enable 
        working with very large objects as well as bittorrent-style 
        block-based file sharing."""
    @abc.abstractmethod
    def exists(self, identifier: events.Identifier):
        """Check whether an object exists."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def reserve(self, identifier: events.Identifier, size: int):
        """Reserve an object identifier for writing.

        Raises:
            StorageError: Identifier already exists."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def write(self, identifier: events.Identifier, offset: int, buf: bytes):
        """Write bytes to an object at an `offset`.

        Raises:
            StorageError: Identifier not found.
            StorageError: Object finalized."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def finalize(self, identifier: events.Identifier):
        """Finalize an object and make it readable.

        Raises:
            StorageError: Identifier not found.
            StorageError: Object already finalized."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def read(self, identifier: events.Identifier, offset: int, size: int):
        """Read `size` bytes of an object from `offset`.

        Raises:
            StorageError: Identifier not found.
            StorageError: Object not yet finalized."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def purge(self, identifier: events.Identifier):
        """Delete the object.

        Raises:
            StorageError: Identifier not found."""
        raise NotImplementedError("unimplemented method")

### State ###
class SchemaInfo:
    """Informational class for schemas."""
    def __init__(self, name: str, uuid_: uuid.UUID, versions: int):
        self.name: str = name
        """Schema name."""
        self.uuid: uuid.UUID = uuid_
        """Schema UUID."""
        self.versions: int = versions
        """Number of versions."""

    def serialize(self) -> dict:
        """Serialize instance to dictionary."""
        return {
            "name": self.name,
            "uuid": str(self.uuid),
            "versions": self.versions,
        }

class ObjectInfo:
    """Informational class for :class:`gonk.core.events.Object`\s."""
    def __init__(self, uuid_: uuid.UUID, versions: int):
        self.uuid: uuid.UUID = uuid_
        """Object UUID."""
        self.versions: int = versions
        """Number of versions."""

    def serialize(self) -> dict:
        """Serialize instance to dictionary."""
        return {
            "uuid": str(self.uuid),
            "versions": self.versions,
        }

class AnnotationInfo:
    """Informational class for :class:`gonk.core.events.Annotation`\s."""
    def __init__(self, uuid_: uuid.UUID, versions: int):
        self.uuid: uuid.UUID = uuid_
        """Annotation UUID."""
        self.versions: int = versions
        """Number of versions."""

    def serialize(self) -> dict:
        """Serialize instance to dictionary."""
        return {
            "uuid": str(self.uuid),
            "versions": self.versions,
        }

class EventInfo:
    """Informational class for :class:`gonk.core.events.Event`\s."""
    def __init__(self, uuid_: uuid.UUID, type_: str, review: None|str):
        self.uuid: uuid.UUID = uuid_
        """Event UUID."""
        self.type: str = type_
        """Event type (class name)."""
        self.review: None|str = review
        """The review status of the event.

        For unreviewable events, :class:`gonk.core.events.OwnerEvent` 
        and :class:`gonk.core.events.ReviewEvent`, this field is None.
        For reviewable events, :class:`gonk.core.events.ObjectEvent` and 
        :class:`gonk.core.events.AnnotationEvent`, this field can be
        ``PENDING``, ``ACCEPTED``, or ``REJECTED``."""

    def serialize(self) -> dict:
        """Serialize instance to dictionary."""
        return {
            "uuid": str(self.uuid),
            "type": self.type,
            "review": self.review,
        }

    def __repr__(self):
        return f"EventInfo({self.uuid}, {self.type}, {self.review})"

class NamedIdentifier(events.Identifier):
    """Return type for schemas and objects."""
    def __init__(self, uuid_: uuid.UUID, version: int, name: str):
        super().__init__(uuid_, version)
        self.name = name

    def serialize(self) -> dict:
        """Serialize instance to dictionary."""
        return {
            "uuid": str(self.uuid),
            "version": self.version,
            "name": self.name,
        }

class State(Validator, Consumer, abc.ABC):
    """The state interface tracks the current state of the dataset.

    The information it stores in implementation dependent but must enable 
    event validation as well as provide the API that the web service 
    requires to run."""
    @abc.abstractmethod
    def events_by_object(self, 
        identifier: events.Identifier) -> list[EventInfo]:
        """Get the events for a given object."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def events_by_annotation(self, 
        identifier: events.Identifier) -> list[EventInfo]:
        """Get the events for a given annotation."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def events_all(self, after: None|uuid.UUID = None) -> list[EventInfo]:
        """Get a page of 25 events.

        Args:
            after: Event UUID to page after or None for the first page."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def annotations_all(self, uuid_: None|uuid.UUID = None, 
        after: None|uuid.UUID = None) -> list[AnnotationInfo]:
        """Get a page of 25 annotations.

        Only one of ``uuid_`` and ``after`` may be provided.

        Args:
            uuid_: The annotation UUID to get information about.
            after: Annotation UUID to page after or None for the first page."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def annotations_by_object(self, 
        object_identifier: events.Identifier) -> list[AnnotationInfo]:
        """Gets the annotations for a given object."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def annotations_by_status(self, status: str, 
        after: None|uuid.UUID = None) -> list[events.Identifier]:
        """Gets the annotations for a given status.

        Args:
            status: ``pending``, ``accepted``, ``rejected``, or ``deleted``.
            after: Annotation UUID to page after or None for the first page."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def annotation(self, 
        identifier: events.Identifier) -> events.Annotation | None:
        """Get an Annotation for a given identifier."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def objects_all(self, uuid_: None|uuid.UUID = None, 
        after: None|uuid.UUID = None) -> list[ObjectInfo]:
        """Get a page of 25 objects.

        Only one of ``uuid_`` and ``after`` may be provided.

        Args:
            uuid_: The object UUID to get information about.
            after: Object UUID to page after or None for the first page."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def objects_by_annotation(self, 
        annotation_uuid: uuid.UUID) -> list[events.Identifier]:
        """Get the objects for a given annotation."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def objects_by_status(self, status: str, 
        after: None|uuid.UUID = None) -> list[events.Identifier]:
        """Gets the objects for a given status.

        Args:
            status: ``pending``, ``accepted``, ``rejected``, or ``deleted``.
            after: Object UUID to page after or None for the first page."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def object(self, identifier: events.Identifier) -> events.Object | None:
        """Get an Object for a given identifier."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def schemas_all(self, name: None|str = None) -> list[SchemaInfo]:
        """Get all schemas.

        Args:
            name: If provided only information 
                about that schema will be returned."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def schemas_by_status(self, 
        status: str, after: None|uuid.UUID = None) -> list[NamedIdentifier]:
        """Gets the schemas for a given status.

        Args:
            status: ``pending``, ``accepted``, ``rejected``, or ``deprecated``.
            after: Schema UUID to page after or None for the first page."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def schema(self, name: str, version: int) -> events.Object | None:
        """Get a schema for a given name and version."""
        raise NotImplementedError("unimplemented method")
    
    @abc.abstractmethod
    def owners(self) -> list[str]:
        """Get all owners."""
        raise NotImplementedError("unimplemented method")

    def validate(self, event: events.EventT):
        """Dispatch method for event validation methods."""
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
    def _validate_object_create(self, event: events.ObjectCreateEvent):
        """Validate :class:`gonk.core.events.ObjectCreateEvent`.

        Raises:
            ValidationError: Object UUID already exists.
            ValidationError: Schema name already in use.
            ValidationError: Duplicate hash detected.
            ValidationError: Object version not zero.
        """
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_object_update(self, event: events.ObjectUpdateEvent):
        """Validate :class:`gonk.core.events.ObjectUpdateEvent`.

        Raises:
            ValidationError: Object UUID does not exists.
            ValidationError: Duplicate hash detected.
            ValidationError: Schema name changed between versions.
            ValidationError: Incorrect object version.
        """
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_object_delete(self, event: events.ObjectDeleteEvent):
        """Validate :class:`gonk.core.events.ObjectDeleteEvent`.

        Raises:
            ValidationError: Object is a schema.
            ValidationError: Object identifier not found.
            ValidationError: Object is rejected.
            ValidationError: Object pending deletion.
            ValidationError: Object is deleted.
        """
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_annotation_create(self, event: events.AnnotationCreateEvent):
        """Validate :class:`gonk.core.events.AnnotationCreateEvent`.

        Raises:
            ValidationError: Annotation UUID already exists.
            ValidationError: Annotation version not zero.
            ValidationError: Object identifier not found.
            ValidationError: Annotating a rejected object.
            ValidationError: Annotating a deleted object.
            ValidationError: Annotating a schema.
        """
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_annotation_update(self, event: events.AnnotationUpdateEvent):
        """Validate :class:`gonk.core.events.AnnotationUpdateEvent`.

        Raises:
            ValidationError: Annotation UUID does not exists.
            ValidationError: Incorrect annotation version.
        """
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_annotation_delete(self, event: events.AnnotationDeleteEvent):
        """Validate :class:`gonk.core.events.AnnotationDeleteEvent`.

        Raises:
            ValidationError: Annotation identifier not found.
            ValidationError: Annotation is rejected.
            ValidationError: Annotation pending deletion.
            ValidationError: Annotation is deleted.
        """
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_review_accept(self, event: events.ReviewAcceptEvent):
        """Validate :class:`gonk.core.events.ReviewAcceptEvent`.

        Raises:
            ValidationError: Event not found.
            ValidationError: Event already reviewed.
            ValidationError: Review of non object or annotation event.
            ValidationError: Author is not owner.
        """
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_review_reject(self, event: events.ReviewRejectEvent):
        """Validate :class:`gonk.core.events.ReviewRejectEvent`.

        Raises:
            ValidationError: Event not found.
            ValidationError: Event already reviewed.
            ValidationError: Review of non object or annotation event.
            ValidationError: Author is not owner.
        """
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_owner_add(self, event: events.OwnerAddEvent):
        """Validate :class:`gonk.core.events.OwnerAddEvent`.

        Raises:
            ValidationError: Owner already exists.
            ValidationError: Author is not owner.
            ValidationError: First owner add event must be a self add.
        """
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _validate_owner_remove(self, event: events.OwnerRemoveEvent):
        """Validate :class:`gonk.core.events.OwnerRemoveEvent`.

        Raises:
            ValidationError: No owners to remove.
            ValidationError: Author is not owner.
            ValidationError: Target of remove event is not an owner.
            ValidationError: Cannot leave the dataset ownerless.
            ValidationError: Target is a higher rank than author.
        """
        raise NotImplementedError("unimplemented method")

    def consume(self, event: events.EventT):
        """Dispatch method for event consumption methods."""
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
        """Consume :class:`gonk.core.events.ObjectCreateEvent`."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_object_update(self, event: events.ObjectUpdateEvent):
        """Consume :class:`gonk.core.events.ObjectUpdateEvent`."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_object_delete(self, event: events.ObjectDeleteEvent):
        """Consume :class:`gonk.core.events.ObjectDeleteEvent`."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_annotation_create(self, event: events.AnnotationCreateEvent):
        """Consume :class:`gonk.core.events.AnnotationCreateEvent`."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_annotation_update(self, event: events.AnnotationUpdateEvent):
        """Consume :class:`gonk.core.events.AnnotationUpdateEvent`."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_annotation_delete(self, event: events.AnnotationDeleteEvent):
        """Consume :class:`gonk.core.events.AnnotationDeleteEvent`."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_review_accept(self, event: events.ReviewAcceptEvent):
        """Consume :class:`gonk.core.events.ReviewAcceptEvent`."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_review_reject(self, event: events.ReviewRejectEvent):
        """Consume :class:`gonk.core.events.ReviewRejectEvent`."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_owner_add(self, event: events.OwnerAddEvent):
        """Consume :class:`gonk.core.events.OwnerAddEvent`."""
        raise NotImplementedError("unimplemented method")

    @abc.abstractmethod
    def _consume_owner_remove(self, event: events.OwnerRemoveEvent):
        """Consume :class:`gonk.core.events.OwnerRemoveEvent`."""
        raise NotImplementedError("unimplemented method")

