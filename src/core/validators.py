# Copyright 2023 - Compute Heavy Industries Incorporated
# This work is released, distributed, and licensed under AGPLv3.

import json
import uuid
import typing
import jsonschema

from gonk.core import interfaces
from gonk.core import exceptions
from gonk.core import events

def is_schema(name: str) -> bool:
    return name.startswith("schema-")

class FieldValidator(interfaces.Validator):
    """Validator for the fields of :class:`gonk.core.events.Object` and 
        :class:`gonk.core.events.Annotation`."""
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
            raise exceptions.ValidationError("UUID is not of type uuid.UUID")

        if object_.version < 0:
            raise exceptions.ValidationError(
                "version must be a non-negative integer")

        if len(object_.name) == 0:
            raise exceptions.ValidationError("object name cannot be empty")

        if len(object_.format) == 0:
            raise exceptions.ValidationError("object format cannot be empty")

        if object_.size < 0:
            raise exceptions.ValidationError(
                "size must be a non-negative integer")

        if object_.hash_type != events.HashTypeT.SHA256:
            raise exceptions.ValidationError("hash type must be SHA256")

        if len(object_.hash) != 64:
            raise exceptions.ValidationError(
                "hash should be a hex encoded SHA256")

    def _validate_annotation(self,
        event: events.AnnotationCreateEvent|events.AnnotationUpdateEvent):
        annotation = event.annotation
        if not isinstance(annotation.uuid, uuid.UUID):
            raise exceptions.ValidationError("UUID is not of type uuid.UUID")

        if annotation.version < 0:
            raise exceptions.ValidationError(
                "version must be a positive integer")

        if annotation.size < 0:
            raise exceptions.ValidationError(
                "size must be a non-negative integer")

        if not isinstance(annotation.schema_, events.Identifier):
            raise exceptions.ValidationError("schema must be an identifier")

        if annotation.hash_type != events.HashTypeT.SHA256:
            raise exceptions.ValidationError("hash type must be SHA256")

        if len(annotation.hash) != 64:
            raise exceptions.ValidationError(
                "hash should be a hex encoded SHA256")

class SchemaValidator(interfaces.Validator, interfaces.Consumer):
    """JSON Schema validator to ensure schemas are valid and 
    that annotations conform to the schema they reference. Schema objects 
    must be mimetype ``application/schema+json``."""
    def __init__(self, depot: interfaces.Depot):
        super().__init__()
        self.depot: interfaces.Depot = depot
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
            raise exceptions.ValidationError("invalid JSON schema") from error

    def _validate_annotation(self, annotation):
        if annotation.schema_ not in self.schemas:
            return

        schema_bs = bytes()
        off = 0
        chunk = 1024*5
        while True:
            buf = self.depot.read(annotation.schema_, off, chunk)
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
            raise exceptions.ValidationError(
                "annotation does not match schema") from error

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