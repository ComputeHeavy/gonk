import fs
import core
import uuid
import pathlib
import secrets
import unittest

def rmtree(p):
    for ea in p.iterdir():
        if ea.is_dir():
            rmtree(ea)
        else:
            ea.unlink()
    p.rmdir()

class TestDepot(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        cls.test_directory.mkdir()

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.test_directory)

    def tearDown(self):
        rmtree(TestDepot.test_directory.joinpath("depot"))
    
    def test_depot_init(self):
        depot = fs.Depot(TestDepot.test_directory)
        self.assertTrue(TestDepot.test_directory.joinpath("depot").exists())

    def test_reserve(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 128)

        key = f"{id_.uuid}.{id_.version}"
        object_path = depot.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        self.assertTrue(object_path.exists())
        self.assertEqual(object_path.stat().st_size, 128)

    def test_write(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 128)
        depot.write(id_, 1, b"A"*64)

        key = f"{id_.uuid}.{id_.version}"
        object_path = depot.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        bs = object_path.read_bytes()
        self.assertEqual(len(bs), 128)
        self.assertEqual(bs, b"\x00"+b"A"*64+b"\x00"*63)

    def test_write_boundary(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 16)
        with self.assertRaises(core.StorageError):
            depot.write(id_, 1, b"A"*16)

    def test_finalize(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 128)
        depot.finalize(id_)

        key = f"{id_.uuid}.{id_.version}"
        writable_path = depot.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        readable_path = depot.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        self.assertTrue(not writable_path.exists())
        self.assertTrue(readable_path.exists())
        self.assertEqual(readable_path.stat().st_size, 128)

    def test_read(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 16)
        depot.write(id_, 3, b"A"*8)
        depot.finalize(id_)

        size = 14
        bs = depot.read(id_, 1, size)

        self.assertEqual(len(bs), size)
        self.assertEqual(bs, b"\x00"*2 + b"A"*8 + b"\x00"*4)

    def test_read_unfinished(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 16)
        with self.assertRaises(core.StorageError):
            depot.read(id_, 1, 10)

    def test_read_not_exist(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        with self.assertRaises(core.StorageError):
            depot.read(id_, 1, 10)

    def test_read_chunk(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        object_size = 16
        depot.reserve(id_, object_size)
        depot.write(id_, 3, b"A"*8)
        depot.finalize(id_)

        read_size = 9
        bs1 = depot.read(id_, 0, read_size)
        bs2 = depot.read(id_, read_size, read_size)

        self.assertEqual(len(bs1), read_size)
        self.assertEqual(len(bs2), object_size-read_size)

    def test_purge_unfinished(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 128)
        depot.purge(id_)

        key = f"{id_.uuid}.{id_.version}"
        writable_path = depot.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        readable_path = depot.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        self.assertTrue(not writable_path.exists())
        self.assertTrue(not readable_path.exists())

    def test_purge_finished(self):
        depot = fs.Depot(TestDepot.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 128)
        depot.finalize(id_)
        depot.purge(id_)

        key = f"{id_.uuid}.{id_.version}"
        writable_path = depot.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        readable_path = depot.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        self.assertTrue(not writable_path.exists())
        self.assertTrue(not readable_path.exists())

if __name__ == '__main__':
    unittest.main()