'''
Events
	Object created
	Object deleted
	Object versioned
	Annotation created
	Annotation deleted
	Annotation versioned
	Change approved
	Change rejected
'''

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

class DecisionT(enum.Enum):
	ACCEPT = 1<<0
	REJECT = 1<<1

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

class Annotation:
	# def __init__(self, object_uuid: uuid.UUID, schema: str, 
	# 	hash_type: HashTypeT, hash_: str, uuid_: uuid.UUID = uuid.uuid4(), 
	# 	version: int = 0):
	def __init__(self, object_uuid: uuid.UUID, uuid_: uuid.UUID = uuid.uuid4(), 
		version: int = 0):
		self.uuid = uuid_
		self.version = version
		# self.schema = schema
		# self.hash_type = hash_type
		# self.hash = hash_

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
	def __init__(self, object_uuid: uuid.UUID):
		super().__init__(ActionT.DELETE)
		self.object_uuid = object_uuid

### Annotation Events ###
class AnnotationEvent(Event):
	def __init__(self, action: ActionT):
		super().__init__()
		self.action = action

class AnnotationCreateEvent(AnnotationEvent):
	def __init__(self, object_uuids: list[uuid.UUID], annotation: Annotation):
		super().__init__(ActionT.CREATE)
		self.object_uuids = object_uuids
		self.annotation = annotation

class AnnotationUpdateEvent(AnnotationEvent):
	def __init__(self, annotation: Annotation):
		super().__init__(ActionT.UPDATE)
		self.annotation = annotation

class AnnotationDeleteEvent(AnnotationEvent):
	def __init__(self, annotation_uuid: uuid.UUID):
		super().__init__(ActionT.DELETE)
		self.annotation_uuid = annotation_uuid

### Review Events ###
class ReviewEvent(Event):
	def __init__(self, event_uuid: uuid.UUID, decision: DecisionT):
		super().__init__()
		self.event_uuid = event_uuid
		self.decision = decision

class ReviewAcceptEvent(ReviewEvent):
	def __init__(self, event_uuid: uuid.UUID):
		super().__init__(event_uuid, DecisionT.ACCEPT)

class ReviewRejectEvent(ReviewEvent):
	def __init__(self, event_uuid: uuid.UUID):
		super().__init__(event_uuid, DecisionT.REJECT)

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
		self.objects: dict[uuid.UUID, list[StateObject]] = dict()
		self.annotations = dict()
		self.deltas: dict[uuid.UUID, Delta] = dict()
		self.object_annotation_link = Link()

	def validate(self, event: Event) -> str:
		handler: dict[type, callable[[Event], str]] = {
			ObjectCreateEvent: self._validate_object_create,
			ObjectUpdateEvent: self._validate_object_update,
			# ObjectDeleteEvent: None,
			# AnnotationCreateEvent: None,
			# AnnotationUpdateEvent: None,
			# AnnotationDeleteEvent: None,
			ReviewAcceptEvent: self._validate_review,
			ReviewRejectEvent: self._validate_review,
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

	def _validate_review(self, event: ReviewAcceptEvent) -> str | None:
		if event.event_uuid not in self.deltas:
			return 'Delta not found.'

		delta = self.deltas[event.event_uuid]
		store_lookup = {
			Object: self.objects,
			Annotation: self.annotations,
		}
		if delta.type not in store_lookup:
			return 'Delta type must be Object or Annotation.'

		store = store_lookup[delta.type]

		if delta.target_uuid not in store:
			return 'Target UUID not found.'

		if delta.review is not None:
			return 'Delta already reviewed.'

		return None

	def consume(self, event: Event) -> str | None:
		handler: dict[type, callable[[Event], str]] = {
			ObjectCreateEvent: self._consume_object_create,
			ObjectUpdateEvent: self._consume_object_update,
			# ObjectDeleteEvent: None,
			# AnnotationCreateEvent: None,
			# AnnotationUpdateEvent: None,
			# AnnotationDeleteEvent: None,
			ReviewAcceptEvent: self._consume_review,
			ReviewRejectEvent: self._consume_review,
		}

		if type(event) not in handler:
			return 'Unhandled event type in consume.'

		return handler[type(event)](event)

	def _consume_object_create(self, event: ObjectCreateEvent) -> str | None:
		state_object = StateObject(event.object)
		delta = Delta(event.uuid, event.action, Object, event.object.uuid)
		state_object.deltas.append(delta)

		if event.object.uuid not in self.objects:
			self.objects[event.object.uuid] = []

		self.objects[event.object.uuid].append(state_object)
		self.deltas[event.uuid] = delta

		return None

	def _consume_object_update(self, event: ObjectCreateEvent) -> str | None:
		state_object = StateObject(event.object)
		delta = Delta(event.uuid, event.action, Object, event.object.uuid)
		state_object.deltas.append(delta)

		if event.object.uuid not in self.objects:
			self.objects[event.object.uuid] = []

		self.objects[event.object.uuid].append(state_object)
		self.deltas[event.uuid] = delta

		return None

	def _consume_review(self, event: ReviewAcceptEvent) -> str | None:
		delta = self.deltas[event.event_uuid]
		delta.review = Review(event.decision)

		return None

class Link:
	def __init__(self):
		self.forward: dict[uuid.UUID, list[uuid.UUID]] = {}
		self.reverse: dict[uuid.UUID, list[uuid.UUID]] = {}

class StateObject(Object):
	def __init__(self, object_: Object):
		# super().__init__(object_.name, object_.format_, object_.size, 
		# 	object_.hash_type, object_.hash_, object_.uuid_, object_.version)
		super().__init__(object_.name, object_.uuid, object_.version)
		self.deltas: list[Delta] = []

class Delta:
	def __init__(self, event_uuid: uuid.UUID, action: ActionT, type_: type, 
		target_uuid: uuid.UUID):
		self.event_uuid = event_uuid
		self.action: ActionT = action
		self.type = type_
		self.target_uuid = target_uuid
		self.review: Review | None = None

class Review:
	def __init__(self, decision: DecisionT):
		self.decision = decision

### Main ###
def main():
	state = State()
	machine = Machine()
	machine.register(state)

	o1c = ObjectCreateEvent(Object("obj1"))
	machine.process_event(o1c)

	machine.process_event(ReviewAcceptEvent(o1c.uuid))

	print(state.__dict__)

if __name__ == '__main__':
	main()