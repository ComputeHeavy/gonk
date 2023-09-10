import fs
import core
import sigs
import sqlite
import hashlib
import unittest
import test_utils

import nacl
from nacl import signing

class TestSigs(test_utils.GonkTest):
    def standard_object(self):
        return core.Object(
            "object.txt", 
            "text/plain", 
            len("object contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"object contents").hexdigest())

    def test_signature_validation(self):
        machine = core.Machine()

        machine.register(sigs.SignatureValidator())

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        self.closers.append(state.con)
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
        machine = core.Machine()

        machine.register(sigs.SignatureValidator())

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        self.closers.append(state.con)
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
        machine = core.Machine()

        machine.register(sigs.SignatureValidator())

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        self.closers.append(state.con)
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