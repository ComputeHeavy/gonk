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
            raise ValueError('UUID not in index')

        return self.events[self.index[uuid_]]

    def exists(self, uuid_: uuid.UUID) -> bool:
        return uuid_ in self.index

    def next(self, uuid_: uuid.UUID | None) -> uuid.UUID | None:
        if uuid_ is None:
            if len(events) == 0:
                return None

            return self.events[0]

        if uuid_ not in self.index:
            raise ValueError('UUID not in index')

        curr = self.index[uuid_]
        next_ = curr + 1
        if next_ == len(self.events):
            return None

        return self.events[next_]