import core
import uuid

class RecordKeeper(core.RecordKeeper):
    def __init__(self):
        super().__init__()
        self.events: list[core.Event] = []
        self.index: dict[uuid.UUID, int] = {}

    def add(self, event: core.Event):
        self.index[event.uuid] = len(self.events)
        self.events.append(event)

    def read(self, uuid_: uuid.UUID) -> core.Event:
        if not self.exists(uuid_):
            raise Exception('UUID not in index')

        return self.events[self.index[uuid_]]

    def exists(self, uuid_: uuid.UUID) -> bool:
        return uuid_ in self.index