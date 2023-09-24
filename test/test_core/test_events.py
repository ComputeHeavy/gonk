import hashlib
import unittest
import nacl
import uuid

from gonk.core import integrity
from gonk.core import events

class TestEventSerde(unittest.TestCase):
    def standard_object(self):
        return events.Object(
            "object.txt", 
            "text/plain", 
            len("object contents"), 
            events.HashTypeT.SHA256, 
            hashlib.sha256(b"object contents").hexdigest())

    def standard_schema(self):
        return events.Object(
            "schema-sample", 
            "application/schema+json", 
            len("schema contents"), 
            events.HashTypeT.SHA256, 
            hashlib.sha256(b"schema contents").hexdigest())

    def standard_annotation(self, schema: events.Identifier):
        return events.Annotation(
            schema, 
            len("annotation contents"), 
            events.HashTypeT.SHA256, 
            hashlib.sha256(b"annotation contents").hexdigest())

    def test_object_create_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        o1v0 = self.standard_object()
        oce_in = signer.sign(events.ObjectCreateEvent(o1v0))

        oce_out = events.ObjectCreateEvent.deserialize(oce_in.serialize())
        self.assertEqual(oce_in, oce_out)

    def test_object_update_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        o1v1 = self.standard_object()
        o1v1.version = 1
        oue_in = signer.sign(events.ObjectUpdateEvent(o1v1))

        oue_out = events.ObjectUpdateEvent.deserialize(oue_in.serialize())
        self.assertEqual(oue_in, oue_out)

    def test_object_delete_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        o1v0 = self.standard_object()
        ode_in = signer.sign(events.ObjectDeleteEvent(o1v0.identifier()))

        ode_out = events.ObjectDeleteEvent.deserialize(ode_in.serialize())
        self.assertEqual(ode_in, ode_out)

    def test_annotation_create_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        o1v0 = self.standard_object()
        s1v0 = self.standard_schema()
        a1v0 = self.standard_annotation(s1v0.identifier())
        ace_in = signer.sign(
            events.AnnotationCreateEvent([o1v0.identifier()], a1v0))

        ace_out = events.AnnotationCreateEvent.deserialize(ace_in.serialize())
        self.assertEqual(ace_in, ace_out)

    def test_annotation_update_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        s1v0 = self.standard_schema()
        a1v1 = self.standard_annotation(s1v0.identifier())
        a1v1.version = 1
        aue_in = signer.sign(events.AnnotationUpdateEvent(a1v1))

        aue_out = events.AnnotationUpdateEvent.deserialize(aue_in.serialize())
        self.assertEqual(aue_in, aue_out)

    def test_annotation_delete_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        s1v0 = self.standard_schema()
        a1v0 = self.standard_annotation(s1v0.identifier())
        ade_in = signer.sign(events.AnnotationDeleteEvent(a1v0.identifier()))

        ade_out = events.AnnotationDeleteEvent.deserialize(ade_in.serialize())
        self.assertEqual(ade_in, ade_out)

    def test_review_accept_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        event_uuid = uuid.uuid4()
        rae_in = signer.sign(events.ReviewAcceptEvent(event_uuid))

        rae_out = events.ReviewAcceptEvent.deserialize(rae_in.serialize())
        self.assertEqual(rae_in, rae_out)

    def test_review_reject_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        event_uuid = uuid.uuid4()
        rae_in = signer.sign(events.ReviewRejectEvent(event_uuid))

        rae_out = events.ReviewRejectEvent.deserialize(rae_in.serialize())
        self.assertEqual(rae_in, rae_out)

    def test_owner_add_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        oae_in = signer.sign(events.OwnerAddEvent(bytes(sk1.verify_key).hex()))

        oae_out = events.OwnerAddEvent.deserialize(oae_in.serialize())
        self.assertEqual(oae_in, oae_out)

    def test_owner_remove_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

        ore_in = signer.sign(
            events.OwnerRemoveEvent(bytes(sk1.verify_key).hex()))

        ore_out = events.OwnerRemoveEvent.deserialize(ore_in.serialize())
        self.assertEqual(ore_in, ore_out)