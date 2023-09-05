import core
import uuid
import json
import pathlib
import pickle

class Record:
    def __init__(self, event: int):
        self.event = event
        self.next = None

class RecordKeeper(core.RecordKeeper):
    def __init__(self, parent_directory: pathlib.Path):
        super().__init__()
        if not parent_directory.exists():
            raise ValueError("parent directory does not exist")

        self.root_directory = parent_directory.joinpath("fsrk")
        self.event_directory = self.root_directory.joinpath("events")
        self.head_path = self.root_directory.joinpath("head")
        self.tail_path = self.root_directory.joinpath("tail")

        if not self.root_directory.exists():
            self.root_directory.mkdir()

        if not self.event_directory.exists():
            self.event_directory.mkdir()

    def _link_tail(self, key: str):
        if not self.tail_path.exists():
            return

        tail = self.tail_path.read_text()

        if len(tail) != 36:
            raise ValueError("invalid data for tail")

        record_path = self.event_directory.joinpath(
            f"{tail[0]}/{tail[1]}/{tail[2]}/{tail}")

        event_json = record_path.read_text()
        event_data = json.loads(event_json)
        event_data["next"] = key
        event_json = json.dumps(event_data)

        record_path.write_text(event_json)

    def add(self, event: core.EventT):
        event_data = event.serialize()
        event_data["type"] = event.__class__.__name__
        event_data["next"] = None

        event_json = json.dumps(event_data)

        key = event_data["uuid"]
        
        record_path = self.event_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}")
        record_path.mkdir(parents=True, exist_ok=True)
        record_path = record_path.joinpath(key)
        record_path.write_text(event_json)

        if not self.head_path.exists():
            self.head_path.write_text(key)

        self._link_tail(key)
        self.tail_path.write_text(key)

    def read(self, uuid_: uuid.UUID) -> core.Event:
        if not self.exists(uuid_):
            raise ValueError("event does not exist")

        key = str(uuid_)
        record_path = self.event_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")
        event_json = record_path.read_text()
        event_data = json.loads(event_json)
        event = getattr(core, event_data["type"]).deserialize(event_data)

        return event

    def exists(self, uuid_: uuid.UUID) -> bool:
        key = str(uuid_)
        record_path = self.event_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")
        return record_path.exists()

    def next(self, uuid_: uuid.UUID | None) -> uuid.UUID | None:
        if uuid_ is None:
            if not self.head_path.exists():
                return None

            head = self.head_path.read_text()

            if len(head) != 36:
                raise ValueError("invalid data for head")

            return uuid.UUID(head)

        if not self.exists(uuid_):
            raise ValueError("event does not exist")

        key = str(uuid_)
        record_path = self.event_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        event_json = record_path.read_text()
        event_data = json.loads(event_json)
        return uuid.UUID(event_data["next"])
