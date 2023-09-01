import core
import sigs
import memstate
import memrk
import memd
import unittest
import hashlib
import nacl
from nacl import signing

class TestSigs(unittest.TestCase):
    def standard_object(self):
        fields = {
            "name": "object.txt",
            "format": "text/plain",
            "size": len("object contents"),
            "hash_type": core.HashTypeT.SHA256,
            "hash": hashlib.sha256(b"object contents").hexdigest(),
        }

        return core.Object(fields["name"], fields["format"], fields["size"], 
            fields["hash_type"], fields["hash"])

    def test_signature_validation(self):
        depot = memd.Depot()
        machine = core.Machine()

        machine.register(sigs.SignatureValidator())

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
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
        depot = memd.Depot()
        machine = core.Machine()

        machine.register(sigs.SignatureValidator())

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
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

if __name__ == '__main__':
    unittest.main()