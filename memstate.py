import core
import uuid

class ObjectAnnotationLink:
    def __init__(self):
        self.forward: dict[Identifier, list[uuid.UUID]] = {}
        self.reverse: dict[uuid.UUID, list[Identifier]] = {}

class State(core.State):
    def __init__(self):
        super().__init__()
        self.deleted_objects: set[core.Identifier] = set()
        self.deleted_annotations: set[core.Identifier] = set()
        self.objects: dict[uuid.UUID, list[core.Object]] = dict()
        self.schemas: dict[str, uuid.UUID] = dict()
        self.annotations: dict[uuid.UUID, list[core.Annotation]] = dict()
        self.link = ObjectAnnotationLink()

    def _validate_object_create(
    	self, event: core.ObjectCreateEvent) -> str | None:
        if core.is_schema(event.object.name):
            if event.object.name in self.schemas:
                return 'Schema name already in use.'

        if event.object.uuid in self.objects:
            return 'UUID already exists in object store.'

        if event.object.version != 0:
            return 'Object version must be zero in create event.'

        return None

    def _validate_object_update(
    	self, event: core.ObjectCreateEvent) -> str | None:
        if core.is_schema(event.object.name):
            if event.object.name not in self.schemas:
                return 'Schema name does not exist.'

            if self.schemas[event.object.name] != event.object.uuid:
                return 'Unexpected UUID for schema name.'

        if event.object.uuid not in self.objects:
            return 'UUID not found in object store.'

        expected_version = len(self.objects[event.object.uuid])
        if event.object.version != expected_version:
            return f'Object version should be {expected_version}.'

        return None

    def _validate_object_delete(
    	self, event: core.ObjectDeleteEvent) -> str | None:
        if event.object_identifier.uuid in self.schemas.values():
            return 'Schemas can not be deleted.'

        identifier = event.object_identifier
        if identifier.uuid not in self.objects:
            return 'Object identifier not found.'

        if len(self.objects[identifier.uuid]) <= identifier.version:
            return 'Version does not exist.'

        if identifier in self.deleted_objects:
            return 'Object version already deleted.'

        return None

    def _validate_annotation_create(
        self, event: core.AnnotationCreateEvent) -> str | None:

        if event.annotation.uuid in self.annotations:
            return 'UUID already exists in annotation store.'

        if event.annotation.version != 0:
            return 'Annotation version must be zero in create event.'

        if event.annotation.uuid in self.link.reverse:
            return 'Annotation identifier already linked to objects.'

        for identifier in event.object_identifiers:
            if identifier.uuid not in self.objects:
                return 'Object identifier not found in object store.'

            if len(self.objects[identifier.uuid]) <= identifier.version:
                return 'Version does not exist.'

            if identifier in self.deleted_objects:
                return 'Deleted objects cannot be annotated.'

            if identifier.uuid in self.schemas.values():
                return 'Schemas can not be annotated.'

        return None

    def _validate_annotation_update(
        self, event: core.AnnotationUpdateEvent) -> str | None:
        if event.annotation.uuid not in self.annotations:
            return 'UUID not found in annotation store.'

        expected_version = len(self.annotations[event.annotation.uuid])
        if event.annotation.version != expected_version:
            return f'Annotation version should be {expected_version}.'

        if event.annotation.uuid not in self.link.reverse:
            return 'Annotation identifier not linked to any objects.'

        for identifier in self.link.reverse[event.annotation.uuid]:
            if identifier in self.deleted_objects:
                return 'Annotating a deleted object.'

        return None

    def _validate_annotation_delete(
        self, event: core.AnnotationDeleteEvent) -> str | None:
        identifier = event.annotation_identifier
        if identifier.uuid not in self.annotations:
            return 'Annotation identifier not found.'

        if len(self.annotations[identifier.uuid]) <= identifier.version:
            return 'Version does not exist.'

        if identifier in self.deleted_annotations:
            return 'Annotation version already deleted.'

        return None

    def _consume_object_create(
    	self, event: core.ObjectCreateEvent) -> str | None:
        self.objects[event.object.uuid] = []
        self.objects[event.object.uuid].append(event.object)

        if core.is_schema(event.object.name):
            self.schemas[event.object.name] = event.object.uuid

    def _consume_object_update(
    	self, event: core.ObjectUpdateEvent) -> str | None:
        self.objects[event.object.uuid].append(event.object)

    def _consume_object_delete(
    	self, event: core.ObjectDeleteEvent) -> str | None:
        self.deleted_objects.add(event.object_identifier)

    def _consume_annotation_create(
        self, event: core.AnnotationCreateEvent) -> str | None:
        self.annotations[event.annotation.uuid] = []
        self.annotations[event.annotation.uuid].append(event.annotation)
    
        self.link.reverse[event.annotation.uuid] = []
        for identifier in event.object_identifiers:
            if identifier not in self.link.forward:
                self.link.forward[identifier] = []
            
            self.link.forward[identifier].append(
                event.annotation.uuid)

            self.link.reverse[event.annotation.uuid].append(
                identifier) 

    def _consume_annotation_update(
        self, event: core.AnnotationUpdateEvent) -> str | None:
        self.annotations[event.annotation.uuid].append(event.annotation)

    def _consume_annotation_delete(
        self, event: core.AnnotationDeleteEvent) -> str | None:
        self.deleted_annotations.add(event.annotation_identifier)