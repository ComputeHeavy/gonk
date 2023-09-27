# Copyright 2023 - Compute Heavy Industries Incorporated
# This work is released, distributed, and licensed under AGPLv3.

import enum
import uuid
import json
import pathlib

from gonk.core import interfaces
from gonk.core import exceptions
from gonk.core import events

class RecordKeeper(interfaces.RecordKeeper):
    """File system backed RecordKeeper.

    Treats the event log as a linked list. Utilizes ``head`` and ``tail`` 
    files. Events are stored in depth=3 prefix-tree folder structure. For
    example, UUID ``96f76903-7b92-44d1-8e53-fc47a520684c`` would be stored
    in ``rk/events/9/6/f/``."""
    def __init__(self, parent_directory: pathlib.Path):
        super().__init__()
        if not parent_directory.exists():
            raise ValueError("parent directory does not exist")

        self.root_directory = parent_directory.joinpath("rk")
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

        if event_data["next"] is not None:
            raise ValueError("tail next is not none")

        event_data["next"] = key
        event_json = json.dumps(event_data)

        record_path.write_text(event_json)

    def add(self, event: events.EventT):
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

    def read(self, uuid_: uuid.UUID) -> events.Event:
        if not self.exists(uuid_):
            raise ValueError("event does not exist")

        key = str(uuid_)
        record_path = self.event_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")
        event_json = record_path.read_text()
        event_data = json.loads(event_json)
        event = getattr(events, event_data["type"]).deserialize(event_data)

        return event

    def exists(self, uuid_: uuid.UUID) -> bool:
        key = str(uuid_)
        record_path = self.event_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")
        return record_path.exists()

    def next(self, uuid_: uuid.UUID | None=None) -> uuid.UUID | None:
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
        next_ = event_data["next"]

        if next_ is None:
            return None

        return uuid.UUID(next_)

    def tail(self) -> uuid.UUID|None:
        if not self.tail_path.exists():
            return None

        tail = self.tail_path.read_text()

        return uuid.UUID(tail)

class ObjectStateT(enum.Enum):
    """Enum for Depot objects. """
    NONEXISTENT = 1<<0
    """Object does not exist."""
    READABLE = 1<<1
    """Object is readable."""
    WRITABLE = 1<<2
    """Object is writable."""

class Depot(interfaces.Depot):
    """File system backed Depot.

    Objects are stored in depth=3 prefix-tree folder structure. For
    example, UUID ``62af3937-4c5b-405b-a170-6ef12a5e246a`` would be stored
    in ``depot/6/2/a/``. When an object is still writable it will have the
    ``.wr`` extension. Upon finalization this extension is removed."""
    def __init__(self, parent_directory: pathlib.Path):
        super().__init__()
        if not parent_directory.exists():
            raise ValueError("parent directory does not exist")

        self.root_directory = parent_directory.joinpath("depot")

        if not self.root_directory.exists():
            self.root_directory.mkdir()

    def _state(self, identifier: events.Identifier) -> ObjectStateT:
        key = f"{identifier.uuid}.{identifier.version}"

        readable_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        if readable_path.exists():
            return ObjectStateT.READABLE

        writable_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        if writable_path.exists():
            return ObjectStateT.WRITABLE

        return ObjectStateT.NONEXISTENT

    def exists(self, identifier: events.Identifier):
        return self._state(identifier) != ObjectStateT.NONEXISTENT

    def reserve(self, identifier: events.Identifier, size: int):
        if self._state(identifier) != ObjectStateT.NONEXISTENT:
            raise exceptions.StorageError(
                'identifier already exists in storage')

        key = f"{identifier.uuid}.{identifier.version}"

        object_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}")
        object_path.mkdir(parents=True, exist_ok=True)

        object_path = object_path.joinpath(f"{key}.wr")
        object_path.write_bytes(b'\x00'*size)

    def write(self, identifier: events.Identifier, offset: int, buf: bytes):
        state = self._state(identifier)
        if state == ObjectStateT.NONEXISTENT:
            raise exceptions.StorageError('identifier not found in storage')

        if state == ObjectStateT.READABLE:
            raise exceptions.StorageError('identifier already finalized')

        key = f"{identifier.uuid}.{identifier.version}"
        object_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        info = object_path.stat()

        if offset + len(buf) > info.st_size:
            raise exceptions.StorageError('write outside of reserved boundary')

        with object_path.open(mode="rb+") as f:
            f.seek(offset, 0)
            f.write(buf)

    def finalize(self, identifier: events.Identifier):
        state = self._state(identifier)
        if state == ObjectStateT.NONEXISTENT:
            raise exceptions.StorageError('identifier not found in storage')

        if state == ObjectStateT.READABLE:
            raise exceptions.StorageError('identifier already finalized')

        key = f"{identifier.uuid}.{identifier.version}"
        writable_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        readable_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        writable_path.rename(readable_path)

    def read(self, identifier: events.Identifier, offset: int, size: int):
        state = self._state(identifier)
        if state == ObjectStateT.NONEXISTENT:
            raise exceptions.StorageError('identifier not found in storage')

        if state == ObjectStateT.WRITABLE:
            raise exceptions.StorageError('identifier not finalized')

        key = f"{identifier.uuid}.{identifier.version}"
        object_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        with object_path.open(mode="rb") as f:
            f.seek(offset, 0)
            buf = f.read(size)

        return buf

    def purge(self, identifier: events.Identifier):
        state = self._state(identifier)
        if state == ObjectStateT.NONEXISTENT:
            raise exceptions.StorageError('identifier not found in storage')

        key = f"{identifier.uuid}.{identifier.version}"

        if state == ObjectStateT.READABLE:
            readable_path = self.root_directory.joinpath(
                f"{key[0]}/{key[1]}/{key[2]}/{key}")
            readable_path.unlink()

        if state == ObjectStateT.WRITABLE:
            writable_path = self.root_directory.joinpath(
                f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")
            writable_path.unlink()
