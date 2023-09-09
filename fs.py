import core
import uuid
import json
import enum
import pathlib
import pickle

class ObjectStateT(enum.Enum):
    NONEXISTENT = 1<<0
    READABLE = 1<<1
    WRITABLE = 1<<2

class Depot(core.Depot):
    def __init__(self, parent_directory: pathlib.Path):
        super().__init__()
        if not parent_directory.exists():
            raise ValueError("parent directory does not exist")

        self.root_directory = parent_directory.joinpath("depot")

        if not self.root_directory.exists():
            self.root_directory.mkdir()

    def _state(self, identifier: core.Identifier) -> ObjectStateT:
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

    def reserve(self, identifier: core.Identifier, size: int):
        if self._state(identifier) != ObjectStateT.NONEXISTENT:
            raise core.StorageError('identifier already exists in storage')

        key = f"{identifier.uuid}.{identifier.version}"

        object_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}")
        object_path.mkdir(parents=True, exist_ok=True)

        object_path = object_path.joinpath(f"{key}.wr")
        object_path.write_bytes(b'\x00'*size)

    def write(self, identifier: core.Identifier, offset: int, buf: bytes):
        state = self._state(identifier)
        if state == ObjectStateT.NONEXISTENT:
            raise core.StorageError('identifier not found in storage')

        if state == ObjectStateT.READABLE:
            raise core.StorageError('identifier already finalized')

        key = f"{identifier.uuid}.{identifier.version}"
        object_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        info = object_path.stat()

        if offset + len(buf) > info.st_size:
            raise core.StorageError('write outside of reserved boundary')

        with object_path.open(mode="rb+") as f:
            f.seek(offset, 0)
            f.write(buf)

    def finalize(self, identifier: core.Identifier):
        state = self._state(identifier)
        if state == ObjectStateT.NONEXISTENT:
            raise core.StorageError('identifier not found in storage')

        if state == ObjectStateT.READABLE:
            raise core.StorageError('identifier already finalized')

        key = f"{identifier.uuid}.{identifier.version}"
        writable_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        readable_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        writable_path.rename(readable_path)

    def read(self, identifier: core.Identifier, offset: int, size: int):
        state = self._state(identifier)
        if state == ObjectStateT.NONEXISTENT:
            raise core.StorageError('identifier not found in storage')

        if state == ObjectStateT.WRITABLE:
            raise core.StorageError('identifier not finalized')

        key = f"{identifier.uuid}.{identifier.version}"
        object_path = self.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        with object_path.open(mode="rb") as f:
            f.seek(offset, 0)
            buf = f.read(size)

        return buf

    def purge(self, identifier: core.Identifier):
        state = self._state(identifier)
        if state == ObjectStateT.NONEXISTENT:
            raise core.StorageError('identifier not found in storage')

        key = f"{identifier.uuid}.{identifier.version}"
        
        if state == ObjectStateT.READABLE:
            readable_path = self.root_directory.joinpath(
                f"{key[0]}/{key[1]}/{key[2]}/{key}")
            readable_path.unlink()

        if state == ObjectStateT.WRITABLE:
            writable_path = self.root_directory.joinpath(
                f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")
            writable_path.unlink()
