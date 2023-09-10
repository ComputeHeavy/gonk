import core
import sigs
import fs
import unittest
import hashlib
import nacl
from nacl import signing

class TestSigs(unittest.TestCase):
    def setUp(self):
        self.test_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        self.test_directory.mkdir()

    def tearDown(self):
        test_utils.rmtree(self.test_directory)
        
    def standard_object(self):
        return core.Object(
            "object.txt", 
            "text/plain", 
            len("object contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"object contents").hexdigest())

    def test_signature_validation(self):
        depot = mem.Depot()
        machine = core.Machine()

        machine.register(sigs.SignatureValidator())

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(bytes(vk1))
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

    def test_signature_validation_fails(self):
        depot = mem.Depot()
        machine = core.Machine()

        machine.register(sigs.SignatureValidator())

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        state = mem.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(bytes(vk1))
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        o1v0.name = "FORGERY"

        with self.assertRaises(core.ValidationError):
            machine.process_event(oce)

    def test_replay(self):
        depot = mem.Depot()
        machine = core.Machine()

        machine.register(sigs.SignatureValidator())

        record_keeper = mem.RecordKeeper()
        machine.register(record_keeper)

        machine.register(sigs.ReplayValidator(record_keeper))

        state = mem.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(bytes(vk1))
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        with self.assertRaises(core.ValidationError):
            machine.process_event(wae1)

if __name__ == '__main__':
    unittest.main()