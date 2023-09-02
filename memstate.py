import core
import uuid
import nacl
import enum

class IdentifierUUIDLink:
    def __init__(self):
        self.forward: dict[Identifier, list[uuid.UUID]] = {}
        self.reverse: dict[uuid.UUID, list[Identifier]] = {}

    def add(self, identifier: core.Identifier, uuid_: uuid.UUID):
        if identifier not in self.forward:
            self.forward[identifier] = []

        if uuid_ not in self.reverse:
            self.reverse[uuid_] = []

        self.forward[identifier].append(uuid_)
        self.reverse[uuid_].append(identifier)

class State(core.State):
    def __init__(self, record_keeper: core.RecordKeeper):
        super().__init__()
        self.record_keeper = record_keeper

        self.schemas: dict[str, uuid.UUID] = dict()
        self.object_lookup: dict[uuid.UUID, list[core.Object]] = dict()
        self.annotation_lookup: dict[uuid.UUID, list[core.Annotation]] = dict()
        self.object_annotation_link: IdentifierUUIDLink = IdentifierUUIDLink()

        self.pending_events: set[uuid.UUID] = set()
        self.entity_status: dict[core.Identifier, set[core.StatusT]] = dict()
        self.entity_event_link: IdentifierUUIDLink = IdentifierUUIDLink()

        self.owner_list: list[nacl.signing.VerifyKey] = list()

    def object_exists(self, identifier: core.Identifier = None, 
        uuid_: uuid.UUID = None):
        if identifier is not None:
            if identifier.uuid not in self.object_lookup:
                return False

            if identifier.version > -1 and identifier.version < len(
                self.object_lookup[identifier.uuid]):
                return True

            return False

        if uuid_ is not None:
            return uuid_ in self.object_lookup

        raise ValueError("requires an argument")

    def objects(self, identifier: core.Identifier = None, 
        uuid_: uuid.UUID = None, annotation: uuid.UUID = None, 
        status: set(core.StatusT) = None, page: int = None, 
        page_size: int = None):
        if annotation is not None:
            objects = [object_ for id_ in 
                self.object_annotation_link.reverse[annotation] 
                for object_ in self.object_lookup[id_.uuid]]

            if identifier is not None:
                objects = [object_ for object_ in objects 
                    if object_.identifer() == identifier]

            if uuid_ is not None:
                objects = [object_ for object_ in objects 
                    if object_.uuid == uuid_]

            if status is not None:
                filtered = []
                for object_ in objects:
                    object_status = self.entity_status[object_.identifier()]
                    if len(object_status.intersection(status)) == len(status):
                        filtered.append(object_)
                objects = filtered
        elif identifier is not None or uuid_ is not None:
            if uuid_ is None:
                uuid_ = identifier.uuid

            objects = self.object_lookup[uuid_]
            if identifier is not None:
                objects = [object_ for object_ in objects 
                    if object_.version == identifier.version 
                        and object_.uuid == identifier.uuid]

            if status is not None:
                filtered = []
                for object_ in objects:
                    object_status = self.entity_status[object_.identifier()]
                    if len(object_status.intersection(status)) == len(status):
                        filtered.append(object_)
                objects = filtered 
                
        elif status is not None:
            objects = []
            for identifier, object_ in self.object_lookup.items():
                object_status = self.entity_status[identifier]
                if len(object_status.intersection(status)) == len(status):
                    objects.append(object_)
        else:
            raise ValueError(
                "requires one of [identifier, uuid_, annotation, status]")

        if page is not None and page_size is not None:
            start = page*page_size
            end = start + page_size
            objects = objects[start:end]

        return objects

    def object_status(self, identifier: core.Identifier = None):
        if identifier is None:
            raise ValueError("requires an argument")

        if identifier not in self.entity_status:
            raise ValueError("object not found")

        return self.entity_status[identifier]

    def object_versions(self, uuid_: uuid.UUID = None):
        if uuid_ is None:
            raise ValueError("requires an argument")

        if uuid_ not in self.object_lookup:
            raise ValueError("object not found")

        return self.object_lookup[uuid_]

    def schema_exists(self, identifier: core.Identifier = None, 
        uuid_: uuid.UUID = None, name: str = None):
        if name is not None:
            return name in self.schemas

        if uuid_ is not None:
            return uuid_ in self.schemas.values()

        if identifier is not None:
            if identifier.uuid not in self.schemas.values():
                return False

            if identifier.version > -1 and identifier.version < len(
                self.object_lookup[identifier.uuid]):
                return True

            return False

        raise ValueError("requires an argument")

    def annotation_exists(self, identifier: core.Identifier = None, 
        uuid_: uuid.UUID = None):
        if identifier is not None:
            if identifier.uuid not in self.annotation_lookup:
                return False

            if identifier.version > -1 and identifier.version < len(
                self.annotation_lookup[identifier.uuid]):
                return True

            return False

        if uuid_ is not None:
            return uuid_ in self.annotation_lookup

        raise ValueError("requires an argument")

    def annotations_by_object(self, object: core.Object):
        raise NotImplementedError("unimplemented method")

    def annotation_status(self, identifier: core.Identifier):
        if identifier is None:
            raise ValueError("requires an argument")

        if identifier not in self.entity_status:
            raise ValueError("annotation not found")

        return self.entity_status[identifier]

    def annotation_versions(self, uuid_: uuid.UUID = None):
        if uuid_ is None:
            raise ValueError("requires an argument")

        if uuid_ not in self.annotation_lookup:
            raise ValueError("object not found")

        return self.annotation_lookup[uuid_]

    def owner_exists(self, public_key: bytes = None):
        if public_key is None:
            raise ValueError("requires an argument")

        return public_key in self.owner_list

    def owners(self):
        return self.owner_list

    def event_pending(self, uuid_: uuid.UUID):
        return uuid_ in self.pending_events

class StateConsumer(core.StateConsumer):
    def __init__(self, state: State):
        super().__init__(state)
        
    def _consume_object_create(self, event: core.ObjectCreateEvent):
        self.state.object_lookup[event.object.uuid] = []
        self.state.object_lookup[event.object.uuid].append(event.object)

        if core.is_schema(event.object.name):
            self.state.schemas[event.object.name] = event.object.uuid

        self.state.pending_events.add(event.uuid)
        self.state.entity_status[event.object.identifier()] = set(
            [core.StatusT.CREATE_PENDING])
        self.state.entity_event_link.add(event.object.identifier(), event.uuid)

    def _consume_object_update(self, event: core.ObjectUpdateEvent):
        self.state.object_lookup[event.object.uuid].append(event.object)
        self.state.pending_events.add(event.uuid)
        self.state.entity_status[event.object.identifier()] = set(
            [core.StatusT.CREATE_PENDING])
        self.state.entity_event_link.add(event.object.identifier(), event.uuid)

    def _consume_object_delete(self, event: core.ObjectDeleteEvent):
        self.state.pending_events.add(event.uuid)
        self.state.entity_status[event.object_identifier].add(
            core.StatusT.DELETE_PENDING)
        self.state.entity_event_link.add(event.object_identifier, event.uuid)

    def _consume_annotation_create(self, event: core.AnnotationCreateEvent):
        self.state.annotation_lookup[event.annotation.uuid] = []
        self.state.annotation_lookup[
            event.annotation.uuid].append(event.annotation)
    
        for identifier in event.object_identifiers:
            self.state.object_annotation_link.add(
                identifier, event.annotation.uuid)

        self.state.pending_events.add(event.uuid)
        self.state.entity_status[event.annotation.identifier()] = set(
            [core.StatusT.CREATE_PENDING])
        self.state.entity_event_link.add(
            event.annotation.identifier(), event.uuid)

    def _consume_annotation_update(self, event: core.AnnotationUpdateEvent):
        self.state.annotation_lookup[
            event.annotation.uuid].append(event.annotation)
        self.state.pending_events.add(event.uuid)
        self.state.entity_status[event.annotation.identifier()] = set(
            [core.StatusT.CREATE_PENDING])
        self.state.entity_event_link.add(
            event.annotation.identifier(), event.uuid)

    def _consume_annotation_delete(self, event: core.AnnotationDeleteEvent):
        self.state.pending_events.add(event.uuid)
        self.state.entity_status[event.annotation_identifier].add(
            core.StatusT.DELETE_PENDING)
        self.state.entity_event_link.add(
            event.annotation_identifier, event.uuid)

    def _consume_review_accept(self, event: core.ReviewAcceptEvent):
        self.state.pending_events.remove(event.event_uuid)

        target = self.state.record_keeper.read(event.event_uuid)
        if target.action in [core.ActionT.CREATE, core.ActionT.UPDATE]:
            if isinstance(target, core.ObjectEvent):
                entity_identifier = target.object.identifier()
            elif isinstance(target, core.AnnotationEvent):
                entity_identifier = target.annotation.identifier()
            else:
                raise TypeError("target event has unexpected type")

            self.state.entity_status[entity_identifier].remove(
                core.StatusT.CREATE_PENDING)
        elif target.action == core.ActionT.DELETE:
            if isinstance(target, core.ObjectEvent):
                entity_identifier = target.object_identifier
            elif isinstance(target, core.AnnotationEvent):
                entity_identifier = target.annotation_identifier
            else:
                raise TypeError("target event has unexpected type")

            self.state.entity_status[entity_identifier].remove(
                core.StatusT.DELETE_PENDING)
            self.state.entity_status[entity_identifier].add(
                core.StatusT.DELETE_ACCEPTED)
        else:
            raise ValueError("target event has unexpected action")

        self.state.entity_event_link.add(entity_identifier, event.uuid)

    def _consume_review_reject(self, event: core.ReviewRejectEvent):
        self.state.pending_events.remove(event.event_uuid)

        target = self.state.record_keeper.read(event.event_uuid)
        if target.action in [core.ActionT.CREATE, core.ActionT.UPDATE]:
            if isinstance(target, core.ObjectEvent):
                entity = target.object
            elif isinstance(target, core.AnnotationEvent):
                entity = target.annotation
            else:
                raise TypeError("target event has unexpected type")

            self.state.entity_status[entity.identifier()].remove(
                core.StatusT.CREATE_PENDING)
            self.state.entity_status[entity.identifier()].add(
                core.StatusT.CREATE_REJECTED)
        elif target.action == core.ActionT.DELETE:
            if isinstance(target, core.ObjectEvent):
                identifier = target.object_identifier
            elif isinstance(target, core.AnnotationEvent):
                identifier = target.annotation_identifier
            else:
                raise TypeError("target event has unexpected type")

            self.state.entity_status[identifier].remove(
                core.StatusT.DELETE_PENDING)
        else:
            raise ValueError("target event has unexpected action")

    def _consume_owner_add(self, event: core.OwnerAddEvent):
        self.state.owner_list.append(event.public_key)

    def _consume_owner_remove(self, event: core.OwnerRemoveEvent):
        self.state.owner_list.remove(event.public_key)
