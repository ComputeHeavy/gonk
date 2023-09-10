import core
import uuid
import sigs
import json
import sqlite
import pathlib
import secrets
import unittest

import nacl
from nacl import signing

def rmtree(p):
    for ea in p.iterdir():
        if ea.is_dir():
            rmtree(ea)
        else:
            ea.unlink()
    p.rmdir()

class TestRecordKeeper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        cls.test_directory.mkdir()

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.test_directory)

    def tearDown(self):
        self.record_keeper.con.close()
        TestRecordKeeper.test_directory.joinpath("rk.db").unlink()
    
    def test_record_keeper_init(self):
        self.record_keeper = sqlite.RecordKeeper(TestRecordKeeper.test_directory)

        db_path = TestRecordKeeper.test_directory.joinpath("rk.db")
        self.assertTrue(db_path.exists())
        self.record_keeper.con.close()

    def test_add(self):
        self.record_keeper = sqlite.RecordKeeper(TestRecordKeeper.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae_in = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))
        self.record_keeper.add(oae_in)

        cur = self.record_keeper.con.cursor()
        cur.execute("""SELECT uuid, event FROM events""")
        res = cur.fetchone()
        self.assertTrue(res is not None)

        uuid_, event_json = res
        self.assertEqual(oae_in.uuid, uuid.UUID(uuid_))

        event_data = oae_in.serialize()
        event_data["type"] = oae_in.__class__.__name__
        self.assertEqual(json.dumps(event_data), event_json)

    def test_read(self):
        self.record_keeper = sqlite.RecordKeeper(TestRecordKeeper.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae_in = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))
        self.record_keeper.add(oae_in)

        oae_out = self.record_keeper.read(oae_in.uuid)

        self.assertEqual(oae_in, oae_out)

    def test_exists(self):
        self.record_keeper = sqlite.RecordKeeper(TestRecordKeeper.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))

        self.assertTrue(not self.record_keeper.exists(oae1.uuid))

        self.record_keeper.add(oae1)

        self.assertTrue(self.record_keeper.exists(oae1.uuid))

    def test_next(self):
        self.record_keeper = sqlite.RecordKeeper(TestRecordKeeper.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))
        self.record_keeper.add(oae1)

        sk2 = nacl.signing.SigningKey.generate()
        oae2 = signer.sign(core.OwnerAddEvent(bytes(sk2.verify_key)))
        self.record_keeper.add(oae2)

        self.assertEqual(self.record_keeper.next(), oae1.uuid)
        self.assertEqual(self.record_keeper.next(oae1.uuid), oae2.uuid)
        self.assertEqual(self.record_keeper.next(oae2.uuid), None)

if __name__ == '__main__':
    unittest.main()