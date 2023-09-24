import nacl
import uuid
import hashlib
import test_utils
import unittest
import jsonschema

from nacl import signing

from gonk.core import integrity
from gonk.core import validators
from gonk.core import interfaces
from gonk.core import events
from gonk.core import sq3
from gonk.core import fs

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
        machine = interfaces.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        schema_validator = validators.SchemaValidator(depot)
        machine.register(schema_validator)

        self.assertEqual(len(machine.validators), 2)
        self.assertEqual(len(machine.consumers), 2)

    def test_schema_object_validate(self):
        depot = fs.Depot(self.test_directory)
        machine = interfaces.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sq3.State(self.test_directory, record_keeper)
        machine.register(state)

        schema_validator = validators.SchemaValidator(depot)
        machine.register(schema_validator)

        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

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
        machine = interfaces.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sq3.State(self.test_directory, record_keeper)
        machine.register(state)

        schema_validator = validators.SchemaValidator(depot)
        machine.register(schema_validator)

        sk1 = nacl.signing.SigningKey.generate()
        signer = integrity.Signer(sk1)

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

if __name__ == '__main__':
    unittest.main()