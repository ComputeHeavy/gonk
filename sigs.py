import typing
import nacl

from nacl import signing
from nacl import exceptions

import core
import events

class KeyPair:
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

class Signer:
    def __init__(self, signing_key: nacl.signing.SigningKey):
        self.signing_key: nacl.signing.SigningKey = signing_key
        self.verify_bytes: bytes = bytes(self.signing_key.verify_key)

    def sign(self, event: events.EventT) -> events.EventT:
        signed = self.signing_key.sign(event.signature_bytes())
        event.signature = signed.signature
        event.signer = bytes(self.verify_bytes)
        return event

class SignatureValidator(core.Validator):
    def __init__(self):
        super().__init__()

    def validate(self, event: events.EventT):
        if event.signer is None:
            raise core.ValidationError("event missing signer")
        verify_key = nacl.signing.VerifyKey(event.signer)
        try:
            verify_key.verify(event.signature_bytes(), event.signature)
        except nacl.exceptions.BadSignatureError as error:
            raise core.ValidationError(
                "event signature failed to validate") from error
