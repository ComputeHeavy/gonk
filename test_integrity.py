import fs
import core
import integrity
import sqlite
import events
import hashlib
import unittest
import test_utils

import nacl
from nacl import signing

class TestSigs(test_utils.GonkTest):
    def standard_object(self):
        return events.Object(
            "object.txt", 
            "text/plain", 
            len("object contents"), 
            events.HashTypeT.SHA256, 
            hashlib.sha256(b"object contents").hexdigest())

    def test_signature_validation(self):
        machine = core.Machine()

        machine.register(integrity.SignatureValidator())

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = events.OwnerAddEvent(bytes(vk1).hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

    def test_signature_validation_fails(self):
        machine = core.Machine()

        machine.register(integrity.SignatureValidator())

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = events.OwnerAddEvent(bytes(vk1).hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        o1v0.name = "FORGERY"

        with self.assertRaises(core.ValidationError):
            machine.process_event(oce)

    def test_replay(self):
        machine = core.Machine()

        machine.register(integrity.SignatureValidator())

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = events.OwnerAddEvent(bytes(vk1).hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        with self.assertRaises(core.ValidationError):
            machine.process_event(wae1)

class TestHashChain(test_utils.GonkTest):
    def standard_object(self):
        return events.Object(
            "object.txt", 
            "text/plain", 
            len("object contents"), 
            events.HashTypeT.SHA256, 
            hashlib.sha256(b"object contents").hexdigest())

    def test_hash_chain_validation(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        machine.register(integrity.HashChainValidator(record_keeper))

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        author = "TACIXAT"
        linker = integrity.HashChainLinker(record_keeper)

        wae = events.OwnerAddEvent(author)
        wae = linker.link(wae, author)
        machine.process_event(wae)

        o1v0 = self.standard_object()
        oce = events.ObjectCreateEvent(o1v0)
        oce = linker.link(oce, author)
        machine.process_event(oce)

    def test_hash_chain_validation_fails(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        machine.register(integrity.HashChainValidator(record_keeper))

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        author = "TACIXAT"
        linker = integrity.HashChainLinker(record_keeper)

        wae = events.OwnerAddEvent(author)
        wae = linker.link(wae, author)
        machine.process_event(wae)

        o1v0 = self.standard_object()
        oce = linker.link(events.ObjectCreateEvent(o1v0), author)
        o1v0.name = "FORGERY"

        with self.assertRaises(core.ValidationError):
            machine.process_event(oce)

    def test_replay(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        machine.register(integrity.HashChainValidator(record_keeper))

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        author = "TACIXAT"
        linker = integrity.HashChainLinker(record_keeper)

        wae = events.OwnerAddEvent(author)
        wae = linker.link(wae, author)
        machine.process_event(wae)

        with self.assertRaises(core.ValidationError):
            machine.process_event(wae)

if __name__ == '__main__':
    unittest.main()