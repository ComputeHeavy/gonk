import core

class Depot(core.Depot):
    def __init__(self):
        super().__init__()
        self.storage: dict[core.Identifier, bytes] = {}
        self.writable: set[core.Identifer] = set()

    def reserve(self, identifier: core.Identifier, size: int):
        if identifier in self.storage:
            raise core.StorageError('Identifier already exists in storage.')

        self.storage[identifier] = b'\x00'*size
        self.writable.add(identifier)

    def write(self, identifier: core.Identifier, offset: int, buf: bytes):
        if identifier not in self.storage:
            raise core.StorageError('Identifier not found in storage.')

        if identifier not in self.writable:
            raise core.StorageError('Identifier already finalized.')

        bs = self.storage[identifier]
        
        if offset + len(buf) > len(bs):
            raise core.StorageError('Write outside of reserved boundary.')

        self.storage[identifier] = bs[:offset] + buf + bs[offset+len(buf):]

    def finalize(self, identifier: core.Identifier):
        if identifier not in self.writable:
            raise core.StorageError('Identifier already finalized.')

        self.writable.remove(identifier)

    def read(self, identifier: core.Identifier, offset: int, size: int):
        if identifier not in self.storage:
            raise core.StorageError('Identifier not found in storage.')

        if identifier in self.writable:
            raise core.StorageError('Identifier still being written.')

        return self.storage[identifier][offset:offset+size]

    def purge(self, identifier: core.Identifier):
        if identifier in self.writable:
            self.writable.remove(identifier)

        if identifier in self.storage:
            del self.storage[identifier]
        