import core
import memd
import nacl
import sigs
import memrk
import hashlib
import memstate
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

class TestSchemaValidation(unittest.TestCase):
    def test_validator_register(self):
        depot = memd.Depot()
        machine = core.Machine()
        
        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        schema_validator = core.SchemaValidator(depot)
        machine.register(schema_validator)

        self.assertEqual(len(machine.validators), 1)
        self.assertEqual(len(machine.consumers), 2)

    def test_schema_object_validate(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = memstate.StateConsumer(state)
        machine.register(state_consumer)

        schema_validator = core.SchemaValidator(depot)
        machine.register(schema_validator)

        s1v0 = core.Object("schema-bounding-box", "application/schema+json", 
            len(schema_buf), core.HashTypeT.SHA256, 
            hashlib.sha256(schema_buf).hexdigest())
        
        depot.reserve(s1v0.identifier(), len(schema_buf))
        depot.write(s1v0.identifier(), 0, schema_buf)
        depot.finalize(s1v0.identifier())

        machine.process_event(core.ObjectCreateEvent(s1v0))

    def test_schema_annotation_validate(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        state_validator = core.StateValidator(state)
        machine.register(state_validator)
        state_consumer = memstate.StateConsumer(state)
        machine.register(state_consumer)

        schema_validator = core.SchemaValidator(depot)
        machine.register(schema_validator)

        s1v0 = core.Object("schema-bounding-box", "application/schema+json", 
            len(schema_buf), core.HashTypeT.SHA256, 
            hashlib.sha256(schema_buf).hexdigest())
        
        depot.reserve(s1v0.identifier(), len(schema_buf))
        depot.write(s1v0.identifier(), 0, schema_buf)
        depot.finalize(s1v0.identifier())

        machine.process_event(core.ObjectCreateEvent(s1v0))

        o1v0 = core.Object("image.jpeg", "application/jpeg", 10, 
            core.HashTypeT.SHA256, hashlib.sha256(b"0123456789").hexdigest())
        machine.process_event(core.ObjectCreateEvent(o1v0))

        annotation_buf = b'''
            {
              "points": [
                {"x": 10, "y": 20},
                {"x": 50, "y": 43}
              ],
              "label": "DOG"
            }
        '''

        a1v0 = core.Annotation(s1v0.identifier(), len(annotation_buf), 
            core.HashTypeT.SHA256, hashlib.sha256(annotation_buf).hexdigest())
        depot.reserve(a1v0.identifier(), len(annotation_buf))
        depot.write(a1v0.identifier(), 0, annotation_buf)
        depot.finalize(a1v0.identifier())

        machine.process_event(
            core.AnnotationCreateEvent([o1v0.identifier()], a1v0))

class TestEvents(unittest.TestCase):
    def standard_object(self):
        return core.Object(
            "object.txt", 
            "text/plain", 
            len("object contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"object contents").hexdigest())

    def standard_schema(self):
        return core.Object(
            "schema-sample", 
            "application/schema+json", 
            len("schema contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"schema contents").hexdigest())

    def standard_annotation(self, schema: core.Identifier):
        return core.Annotation(
            schema, 
            len("annotation contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"annotation contents").hexdigest())

    def test_object_create_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        o1v0 = self.standard_object()
        oce_in = signer.sign(core.ObjectCreateEvent(o1v0))

        oce_out = core.ObjectCreateEvent.load(oce_in.dump())
        self.assertEqual(oce_in, oce_out)

    def test_object_update_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        o1v1 = self.standard_object()
        o1v1.version = 1
        oue_in = signer.sign(core.ObjectUpdateEvent(o1v1))

        oue_out = core.ObjectUpdateEvent.load(oue_in.dump())
        self.assertEqual(oue_in, oue_out)

    def test_object_delete_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        o1v0 = self.standard_object()
        ode_in = signer.sign(core.ObjectDeleteEvent(o1v0.identifier()))

        ode_out = core.ObjectDeleteEvent.load(ode_in.dump())
        self.assertEqual(ode_in, ode_out)

    def test_annotation_create_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        o1v0 = self.standard_object()
        s1v0 = self.standard_schema()
        a1v0 = self.standard_annotation(s1v0.identifier())
        ace_in = signer.sign(
            core.AnnotationCreateEvent([o1v0.identifier()], a1v0))

        ace_out = core.AnnotationCreateEvent.load(ace_in.dump())
        self.assertEqual(ace_in, ace_out)

    def test_annotation_update_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        s1v0 = self.standard_schema()
        a1v1 = self.standard_annotation(s1v0.identifier())
        a1v1.version = 1
        aue_in = signer.sign(core.AnnotationUpdateEvent(a1v1))

        aue_out = core.AnnotationUpdateEvent.load(aue_in.dump())
        self.assertEqual(aue_in, aue_out)

    def test_annotation_delete_serde(self):
        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        s1v0 = self.standard_schema()
        a1v0 = self.standard_annotation(s1v0.identifier())
        ade_in = signer.sign(core.AnnotationDeleteEvent(a1v0.identifier()))

        ade_out = core.AnnotationDeleteEvent.load(ade_in.dump())
        self.assertEqual(ade_in, ade_out)


if __name__ == '__main__':
    unittest.main()