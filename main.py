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
	# 	hash_: str, uuid_: uuid.UUID = uuid.uuid4(), version: int = 0):
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
		# 	self.hash, self.uuid, self.version)
		return Object(self.name, self.uuid, self.version)

	def versioned_copy(self):
		copy = self.__copy__()
		copy.version += 1
		return copy

	def identifier(self):
		return Identifier(self.uuid, self.version)

class Annotation:
	# def __init__(self, schema: Identifier, hash_type: HashTypeT, hash_: str, 
	# 	uuid_: uuid.UUID = uuid.uuid4(), version: int = 0):
	def __init__(self, uuid_: uuid.UUID = uuid.uuid4(), version: int = 0):
		self.uuid = uuid_
		self.version = version
		# self.schema = schema
		# self.hash_type = hash_type
		# self.hash = hash_

	def __copy__(self):
		# return Annotation(self.schema, self.hash_type, 
		# 	self.hash, self.uuid, self.version)
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

### Machine ###
class Machine:
	def __init__(self):
		self.validators: list[Validator] = []
		self.event_log: list[Event] = []
		self.consumers: list[Consumer] = []

	def process_event(self, event):
		for v in self.validators:
			res = v.validate(event)
			if res is not None:
				raise Exception(res)

		self.event_log.append(event)

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
	def __init__(self):
		self.deleted_objects: set[Identifier] = set()
		self.deleted_annotations: set[Identifier] = set()
		self.objects: dict[uuid.UUID, list[Object]] = dict()
		self.annotations: dict[uuid.UUID, list[Annotation]] = dict()
		self.link = ObjectAnnotationLink()

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

	def _validate_object_create(self, event: ObjectCreateEvent) -> str | None:
		if event.object.uuid in self.objects:
			return 'UUID already exists in object store.'

		if event.object.version != 0:
			return 'Object version must be zero in create event.'

		return None

	def _validate_object_update(self, event: ObjectCreateEvent) -> str | None:
		if event.object.uuid not in self.objects:
			return 'UUID not found in object store.'

		expected_version = len(self.objects[event.object.uuid])
		if event.object.version != expected_version:
			return f'Object version should be {expected_version}.'

		return None

	def _validate_object_delete(self, event: ObjectDeleteEvent) -> str | None:
		identifier = event.object_identifier
		if identifier.uuid not in self.objects:
			return 'Object identifier not found.'

		if len(self.objects[identifier.uuid]) <= identifier.version:
			return 'Version does not exist.'

		if identifier in self.deleted_objects:
			return 'Object version already deleted.'

		return None

	def _validate_annotation_create(
		self, event: AnnotationCreateEvent) -> str | None:

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
				return 'Annotating a deleted object.'

		return None

	def _validate_annotation_update(
		self, event: AnnotationUpdateEvent) -> str | None:
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
		self, event: AnnotationDeleteEvent) -> str | None:
		identifier = event.annotation_identifier
		if identifier.uuid not in self.annotations:
			return 'Annotation identifier not found.'

		if len(self.annotations[identifier.uuid]) <= identifier.version:
			return 'Version does not exist.'

		if identifier in self.deleted_annotations:
			return 'Annotation version already deleted.'

		return None

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

	def _consume_object_create(self, event: ObjectCreateEvent) -> str | None:
		self.objects[event.object.uuid] = []
		self.objects[event.object.uuid].append(event.object)

	def _consume_object_update(self, event: ObjectUpdateEvent) -> str | None:
		self.objects[event.object.uuid].append(event.object)

	def _consume_object_delete(self, event: ObjectDeleteEvent) -> str | None:
		self.deleted_objects.add(event.object_identifier)

	def _consume_annotation_create(
		self, event: AnnotationCreateEvent) -> str | None:
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
		self, event: AnnotationUpdateEvent) -> str | None:
		self.annotations[event.annotation.uuid].append(event.annotation)

	def _consume_annotation_delete(
		self, event: AnnotationDeleteEvent) -> str | None:
		self.deleted_annotations.add(event.annotation_identifier)

class ObjectAnnotationLink:
	def __init__(self):
		self.forward: dict[Identifier, list[uuid.UUID]] = {}
		self.reverse: dict[uuid.UUID, list[Identifier]] = {}

### Main ###
def main():
	state = State()
	machine = Machine()
	machine.register(state)

	o1v0 = Object("obj1")
	machine.process_event(ObjectCreateEvent(o1v0))

	o1v1 = o1v0.versioned_copy()
	machine.process_event(ObjectUpdateEvent(o1v1))

	o1v2 = o1v1.versioned_copy()
	machine.process_event(ObjectUpdateEvent(o1v2))

	a1v0 = Annotation()
	machine.process_event(AnnotationCreateEvent([o1v2.identifier()], a1v0))

	a1v1 = a1v0.versioned_copy()
	machine.process_event(AnnotationUpdateEvent(a1v1))

	machine.process_event(AnnotationDeleteEvent(a1v1.identifier()))

	print(state.__dict__)

if __name__ == '__main__':
	main()