# Copyright 2023 - Compute Heavy Industries Incorporated
# This work is released, distributed, and licensed under AGPLv3.

import nacl
import typing
import hashlib

from nacl import signing

from gonk.core import interfaces
from gonk.core import exceptions
from gonk.core import events

class HashChainLinker:
    """Hash linker that links one event to the tail event in RecordKeeper."""
    def __init__(self, record_keeper: interfaces.RecordKeeper):
        self.record_keeper: interfaces.RecordKeeper = record_keeper
        """Dataset's RecordKeeper."""

    def link(self, event: events.EventT, author: str) -> events.EventT:
        """Calculate chained hash and populate 
            :class:`gonk.core.events.Event`'s 
            ``integrity`` and ``author`` fields."""
        tail = self.record_keeper.tail()
        prefix = b""
        if tail is not None:
            prev = self.record_keeper.read(tail)
            if prev.integrity is None:
                raise exceptions.ValidationError("tail event missing integrity")

            prefix = prev.integrity

        event.author = author
        event.integrity = hashlib.sha256(
            prefix + event.signature_bytes()).digest()
        return event

class HashChainValidator(interfaces.Validator):
    """Validator for hash linked events."""
    def __init__(self, record_keeper: interfaces.RecordKeeper):
        self.record_keeper = record_keeper
        """Dataset's RecordKeeper."""

    def validate(self, event: events.EventT):
        """Validate :class:`gonk.core.events.Event`'s 
            ``author`` and ``integrity`` fields."""
        if event.author is None:
            raise exceptions.ValidationError("event missing author")

        if event.integrity is None:
            raise exceptions.ValidationError("event missing integrity")
        
        tail = self.record_keeper.tail()
        prefix = b""
        if tail is not None:
            prev = self.record_keeper.read(tail)
            if prev.integrity is None:
                raise exceptions.ValidationError(
                    "previous event missing integrity")

            prefix = prev.integrity

        hash_ = hashlib.sha256(prefix + event.signature_bytes()).digest()
        if hash_ != event.integrity:
            raise exceptions.ValidationError(
                "event integrity failed to validate")

class Signer:
    """Event signer using nacl (ED25519)."""
    def __init__(self, signing_key: nacl.signing.SigningKey):
        self.signing_key: nacl.signing.SigningKey = signing_key
        """Key for signing."""
        self.verify_bytes: bytes = bytes(self.signing_key.verify_key)
        """Public key bytes for identity purposes."""

    def sign(self, event: events.EventT) -> events.EventT:
        """Sign event with ``signing_key``."""
        signed = self.signing_key.sign(event.signature_bytes())
        event.integrity = signed.signature
        event.author = self.verify_bytes.hex()
        return event

class SignatureValidator(interfaces.Validator):
    """Validator for signed events."""
    def __init__(self):
        super().__init__()

    def validate(self, event: events.EventT):
        """Validate that event is signed with the public key in ``author``."""
        if event.author is None:
            raise exceptions.ValidationError("event missing author")
        verify_key = nacl.signing.VerifyKey(bytes.fromhex(event.author))
        try:
            verify_key.verify(event.signature_bytes(), event.integrity)
        except nacl.exceptions.BadSignatureError as error:
            raise exceptions.ValidationError(
                "event integrity failed to validate") from error

class KeyPair:
    """Convenience class for serializing nacl key pairs."""
    def __init__(self, signing_key: bytes|None=None):
        if signing_key is not None:
            self.signing_key = nacl.signing.SigningKey(signing_key)
        else:
            self.signing_key = nacl.signing.SigningKey.generate()
        self.verify_key = self.signing_key.verify_key

    def serialize(self):
        return {
            "signing_key": bytes(self.signing_key).hex(),
            "verify_key": bytes(self.verify_key).hex(),
        }

    @classmethod
    def deserialize(cls, data: dict):
        return KeyPair(bytes.fromhex(data["signing_key"]))