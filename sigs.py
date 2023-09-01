import core
import nacl
from nacl import signing
from nacl import exceptions

class Signer:
    def __init__(self, signing_key: nacl.signing.SigningKey):
        self.signing_key: nacl.signing.SigningKey = signing_key
        self.verify_bytes: bytes = bytes(self.signing_key.verify_key)

    def sign(self, event: core.Event) -> core.Event:
        signed = self.signing_key.sign(event.signature_bytes())
        event.signature = signed.signature
        event.signer = bytes(self.verify_bytes)
        return event

class SignatureValidator(core.Validator):
    def __init__(self):
        super().__init__()

    def validate(self, event):
        verify_key = nacl.signing.VerifyKey(event.signer)
        try:
            verify_key.verify(event.signature_bytes(), event.signature)
        except nacl.exceptions.BadSignatureError as error:
            raise core.ValidationError(
                "event signature failed to validate") from error