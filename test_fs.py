import fs
import core
import uuid
import sigs
import pathlib
import secrets
import unittest
import test_utils

import nacl
from nacl import signing


class TestRecordKeeper(unittest.TestCase):
    def setUp(self):
        self.test_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        self.test_directory.mkdir()

    def tearDown(self):
        test_utils.rmtree(self.test_directory)
    
    def test_record_keeper_init(self):
        record_keeper = fs.RecordKeeper(self.test_directory)

        root_dir = self.test_directory.joinpath("rk")
        self.assertTrue(root_dir.exists())
        self.assertTrue(root_dir.joinpath("events").exists())

    def test_add(self):
        record_keeper = fs.RecordKeeper(self.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))
        record_keeper.add(oae1)

        root_dir = self.test_directory.joinpath("rk")

        head_path = root_dir.joinpath("head")
        self.assertTrue(head_path.exists())
        self.assertEqual(head_path.read_text(), str(oae1.uuid))

        tail_path = root_dir.joinpath("tail")
        self.assertTrue(tail_path.exists())
        self.assertEqual(tail_path.read_text(), str(oae1.uuid))

        events_path = root_dir.joinpath("events")

        key = f"{oae1.uuid}"
        event_path = events_path.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}")

        self.assertTrue(event_path.exists())

        sk2 = nacl.signing.SigningKey.generate()
        oae2 = signer.sign(core.OwnerAddEvent(bytes(sk2.verify_key)))
        record_keeper.add(oae2)

        self.assertEqual(head_path.read_text(), str(oae1.uuid))
        self.assertEqual(tail_path.read_text(), str(oae2.uuid))

    def test_read(self):
        record_keeper = fs.RecordKeeper(self.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae_in = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))
        record_keeper.add(oae_in)

        oae_out = record_keeper.read(oae_in.uuid)

        self.assertEqual(oae_in, oae_out)

    def test_exists(self):
        record_keeper = fs.RecordKeeper(self.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))

        self.assertTrue(not record_keeper.exists(oae1.uuid))

        record_keeper.add(oae1)

        self.assertTrue(record_keeper.exists(oae1.uuid))

    def test_next(self):
        record_keeper = fs.RecordKeeper(self.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))
        record_keeper.add(oae1)

        sk2 = nacl.signing.SigningKey.generate()
        oae2 = signer.sign(core.OwnerAddEvent(bytes(sk2.verify_key)))
        record_keeper.add(oae2)

        self.assertEqual(record_keeper.next(), oae1.uuid)
        self.assertEqual(record_keeper.next(oae1.uuid), oae2.uuid)
        self.assertEqual(record_keeper.next(oae2.uuid), None)

class TestDepot(unittest.TestCase):
    def setUp(self):
        self.test_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        self.test_directory.mkdir()

    def tearDown(self):
        test_utils.rmtree(self.test_directory)
    
    def test_depot_init(self):
        depot = fs.Depot(self.test_directory)
        self.assertTrue(self.test_directory.joinpath("depot").exists())

    def test_reserve(self):
        depot = fs.Depot(self.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 128)

        key = f"{id_.uuid}.{id_.version}"
        object_path = depot.root_directory.joinpath(
            f"{key[0]}/{key[1]}/{key[2]}/{key}.wr")

        self.assertTrue(object_path.exists())
        self.assertEqual(object_path.stat().st_size, 128)

    def test_write(self):
        depot = fs.Depot(self.test_directory)
        
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
        depot = fs.Depot(self.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 16)
        with self.assertRaises(core.StorageError):
            depot.write(id_, 1, b"A"*16)

    def test_finalize(self):
        depot = fs.Depot(self.test_directory)
        
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
        depot = fs.Depot(self.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 16)
        depot.write(id_, 3, b"A"*8)
        depot.finalize(id_)

        size = 14
        bs = depot.read(id_, 1, size)

        self.assertEqual(len(bs), size)
        self.assertEqual(bs, b"\x00"*2 + b"A"*8 + b"\x00"*4)

    def test_read_unfinished(self):
        depot = fs.Depot(self.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        depot.reserve(id_, 16)
        with self.assertRaises(core.StorageError):
            depot.read(id_, 1, 10)

    def test_read_not_exist(self):
        depot = fs.Depot(self.test_directory)
        
        id_ = core.Identifier(uuid.uuid4(), 0)
        with self.assertRaises(core.StorageError):
            depot.read(id_, 1, 10)

    def test_read_chunk(self):
        depot = fs.Depot(self.test_directory)
        
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
        depot = fs.Depot(self.test_directory)
        
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
        depot = fs.Depot(self.test_directory)
        
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