import fs
import core
import nacl
import uuid
import sigs
import events
import sqlite
import hashlib
import test_utils
import unittest
import jsonschema

from nacl import signing

schema_buf = b'''{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "$id": "https://computeheavy.com/example-dataset/bounding-box.schema.json",
  "title": "bounding-box",
  "description": "Captures a bounding box and label in an image.",
  "definitions": {
    "point": {
      "type": "object",
      "properties": {
        "x": {
          "type": "number"
        },
        "y": {
          "type": "number"
        }
      },
      "required": [
        "x",
        "y"
      ]
    }
  },
  "type": "object",
  "properties": {
    "label": {
      "type": "string"
    },
    "points": {
      "type": "array",
      "items": { 
        "$ref": "#/definitions/point"
      },
      "minItems": 2,
      "maxItems": 2
    }
  },
  "required": [
    "points",
    "label"
  ]
}'''

class TestSchemaValidation(test_utils.GonkTest):
    def test_validator_register(self):
        depot = fs.Depot(self.test_directory)
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        schema_validator = core.SchemaValidator(depot)
        machine.register(schema_validator)

        self.assertEqual(len(machine.validators), 2)
        self.assertEqual(len(machine.consumers), 2)

    def test_schema_object_validate(self):
        depot = fs.Depot(self.test_directory)
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        schema_validator = core.SchemaValidator(depot)
        machine.register(schema_validator)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        s1v0 = events.Object("schema-bounding-box", "application/schema+json", 
            len(schema_buf), events.HashTypeT.SHA256, 
            hashlib.sha256(schema_buf).hexdigest())
        
        depot.reserve(s1v0.identifier(), len(schema_buf))
        depot.write(s1v0.identifier(), 0, schema_buf)
        depot.finalize(s1v0.identifier())

        oce = events.ObjectCreateEvent(s1v0)
        oce = signer.sign(oce)

        machine.process_event(oce)

    def test_schema_annotation_validate(self):
        depot = fs.Depot(self.test_directory)
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        schema_validator = core.SchemaValidator(depot)
        machine.register(schema_validator)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        s1v0 = events.Object("schema-bounding-box", "application/schema+json", 
            len(schema_buf), events.HashTypeT.SHA256, 
            hashlib.sha256(schema_buf).hexdigest())
        
        depot.reserve(s1v0.identifier(), len(schema_buf))
        depot.write(s1v0.identifier(), 0, schema_buf)
        depot.finalize(s1v0.identifier())

        sce = events.ObjectCreateEvent(s1v0)
        sce = signer.sign(sce)
        machine.process_event(sce)

        o1v0 = events.Object("image.jpeg", "application/jpeg", 10, 
            events.HashTypeT.SHA256, hashlib.sha256(b"0123456789").hexdigest())

        oce = events.ObjectCreateEvent(o1v0)
        oce = signer.sign(oce)
        machine.process_event(oce)

        annotation_buf = b'''
            {
              "points": [
                {"x": 10, "y": 20},
                {"x": 50, "y": 43}
              ],
              "label": "DOG"
            }
        '''

        a1v0 = events.Annotation(s1v0.identifier(), len(annotation_buf), 
            events.HashTypeT.SHA256, hashlib.sha256(annotation_buf).hexdigest())
        depot.reserve(a1v0.identifier(), len(annotation_buf))
        depot.write(a1v0.identifier(), 0, annotation_buf)
        depot.finalize(a1v0.identifier())

        ace = events.AnnotationCreateEvent([o1v0.identifier()], a1v0)
        ace = signer.sign(ace)
        machine.process_event(ace)

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
        signer = sigs.Signer(sk1)

        o1v0 = self.standard_object()
        oce_in = signer.sign(events.ObjectCreateEvent(o1v0))

        oce_out = events.ObjectCreateEvent.deserialize(oce_in.serialize())
        self.assertEqual(oce_in, oce_out)

    def test_object_update_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        o1v1 = self.standard_object()
        o1v1.version = 1
        oue_in = signer.sign(events.ObjectUpdateEvent(o1v1))

        oue_out = events.ObjectUpdateEvent.deserialize(oue_in.serialize())
        self.assertEqual(oue_in, oue_out)

    def test_object_delete_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        o1v0 = self.standard_object()
        ode_in = signer.sign(events.ObjectDeleteEvent(o1v0.identifier()))

        ode_out = events.ObjectDeleteEvent.deserialize(ode_in.serialize())
        self.assertEqual(ode_in, ode_out)

    def test_annotation_create_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        o1v0 = self.standard_object()
        s1v0 = self.standard_schema()
        a1v0 = self.standard_annotation(s1v0.identifier())
        ace_in = signer.sign(
            events.AnnotationCreateEvent([o1v0.identifier()], a1v0))

        ace_out = events.AnnotationCreateEvent.deserialize(ace_in.serialize())
        self.assertEqual(ace_in, ace_out)

    def test_annotation_update_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        s1v0 = self.standard_schema()
        a1v1 = self.standard_annotation(s1v0.identifier())
        a1v1.version = 1
        aue_in = signer.sign(events.AnnotationUpdateEvent(a1v1))

        aue_out = events.AnnotationUpdateEvent.deserialize(aue_in.serialize())
        self.assertEqual(aue_in, aue_out)

    def test_annotation_delete_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        s1v0 = self.standard_schema()
        a1v0 = self.standard_annotation(s1v0.identifier())
        ade_in = signer.sign(events.AnnotationDeleteEvent(a1v0.identifier()))

        ade_out = events.AnnotationDeleteEvent.deserialize(ade_in.serialize())
        self.assertEqual(ade_in, ade_out)

    def test_review_accept_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        event_uuid = uuid.uuid4()
        rae_in = signer.sign(events.ReviewAcceptEvent(event_uuid))

        rae_out = events.ReviewAcceptEvent.deserialize(rae_in.serialize())
        self.assertEqual(rae_in, rae_out)

    def test_review_reject_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        event_uuid = uuid.uuid4()
        rae_in = signer.sign(events.ReviewRejectEvent(event_uuid))

        rae_out = events.ReviewRejectEvent.deserialize(rae_in.serialize())
        self.assertEqual(rae_in, rae_out)

    def test_owner_add_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae_in = signer.sign(events.OwnerAddEvent(bytes(sk1.verify_key).hex()))

        oae_out = events.OwnerAddEvent.deserialize(oae_in.serialize())
        self.assertEqual(oae_in, oae_out)

    def test_owner_remove_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        ore_in = signer.sign(
            events.OwnerRemoveEvent(bytes(sk1.verify_key).hex()))

        ore_out = events.OwnerRemoveEvent.deserialize(ore_in.serialize())
        self.assertEqual(ore_in, ore_out)


if __name__ == '__main__':
    unittest.main()