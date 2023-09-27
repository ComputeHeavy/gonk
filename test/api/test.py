import sys
import uuid
import json
import base64
import pathlib
import secrets
import inspect
import requests
import unittest

def source_order(_, a, b):
    fns = inspect.getmembers(TestAPI, predicate=inspect.isfunction)
    fns = [fn for _, fn in fns if fn.__name__.startswith("test_")]

    rank = {}
    for fn in fns:
        start = next(ln for ln in fn.__code__.co_lines() if ln[2] is not None)
        rank[fn.__name__] = start

    return (rank[a] > rank[b]) - (rank[a] < rank[b])

unittest.TestLoader.sortTestMethodsUsing = source_order
host = "127.0.0.1:5000"
key = "gk_Zyrhdki6a4MJ9p3dlG1ziEM7gO4yXBXa"

dataset_name = f"testing-{secrets.token_hex(4)}"
schema_name = "schema-example"

user_1 = "TESTUSER1"
user_2 = "TESTUSER2"
# gonk-api init --username TESTUSER1
# gonk-api users add TESTUSER2

# for /f %i in ('dir /b root\datasets\') do rmdir /s /q root\datasets\%i
# rm -rf root/datasets/*

def FUNC(back = 0):
    return sys._getframe(back+1).f_code.co_name

def rmtree(p):
    for ea in p.iterdir():
        if ea.is_dir():
            rmtree(ea)
        else:
            ea.unlink()
    p.rmdir()

class TestAPI(unittest.TestCase):
    debug = True

    @classmethod
    def tearDownClass(cls):
        rmtree(pathlib.Path("root/datasets").joinpath(dataset_name))

    def test_create_dataset(self):
        resp = requests.post(
            f"http://{host}/datasets", 
            headers={
                "x-api-key": key,
            },
            json={
                "name": dataset_name,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

    def test_list_datasets(self):
        resp = requests.get(
            f"http://{host}/datasets", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        self.assertIn("datasets", resp_data)
        self.assertIn(dataset_name, resp_data["datasets"])

    def test_schema_create(self):
        schema_buf = b'''{
            "$schema": "http://json-schema.org/draft-04/schema#",
            "$id": "https://computeheavy.com/dataset-name/schema-example.schema.json",
            "title": "schema-example",
            "description": "Captures a label for an object.",
            "type": "object",
            "properties": {
                "label": {
                    "type": "string"
                }
            },
            "required": [
                "label"
            ]
        }'''

        resp = requests.post(
            f"http://{host}/datasets/{dataset_name}/schemas", 
            headers={
                "x-api-key": key,
            },
            json={
                "name": schema_name,
                "schema": base64.b64encode(schema_buf).decode(),
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("versions", resp_data)
        self.assertEqual(1, resp_data["versions"])
        
        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))
        
        self.assertIn("name", resp_data)
        self.assertEqual(schema_name, resp_data["name"])

    def test_schema_list(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/schemas", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp_data), 1)
        schema_info = resp_data[0]
        
        self.assertIn("versions", schema_info)
        self.assertEqual(1, schema_info["versions"])
        
        self.assertIn("uuid", schema_info)
        self.assertEqual(36, len(schema_info["uuid"]))
        
        self.assertIn("name", schema_info)
        self.assertEqual(schema_name, schema_info["name"])

    def test_schema_info(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/schemas/{schema_name}",
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        
        self.assertIn("versions", resp_data)
        self.assertEqual(1, resp_data["versions"])
        
        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))
        
        self.assertIn("name", resp_data)
        self.assertEqual(schema_name, resp_data["name"])

    def test_schema_update(self):
        schema_buf = b'''{
            "$schema": "http://json-schema.org/draft-04/schema#",
            "$id": "https://computeheavy.com/example-dataset/schema-example.schema.json",
            "title": "schema-example",
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

        resp = requests.patch(
            f"http://{host}/datasets/{dataset_name}/schemas/{schema_name}",
            headers={
                "x-api-key": key,
            },
            json={
                "schema": base64.b64encode(schema_buf).decode(),
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        
        self.assertIn("versions", resp_data)
        self.assertEqual(2, resp_data["versions"])
        
        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))
        
        self.assertIn("name", resp_data)
        self.assertEqual(schema_name, resp_data["name"])

    def test_schema_details(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}"
            f"/schemas/{schema_name}/0", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("schema", resp_data)
        self.assertIn("events", resp_data)
        self.assertIn("bytes", resp_data)

    def test_schema_list_status(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/schemas/pending", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertEqual(len(resp_data), 2)

        named_identifier = resp_data[0]
        self.assertIn("uuid", named_identifier)
        self.assertEqual(36, len(named_identifier["uuid"]))
        
        self.assertIn("name", named_identifier)
        self.assertEqual(schema_name, named_identifier["name"])
        
        self.assertIn("version", named_identifier)
        self.assertEqual(int, type(named_identifier["version"]))

    def test_schema_deprecate(self):
        resp = requests.delete(
            f"http://{host}/datasets/{dataset_name}/schemas/{schema_name}/0", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))
        
        self.assertIn("name", resp_data)
        self.assertEqual(schema_name, resp_data["name"])
        
        self.assertIn("version", resp_data)
        self.assertEqual(int, type(resp_data["version"]))

    def test_owner_list(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/owners", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp_data), 1)

    def test_owner_add(self):
        resp = requests.put(
            f"http://{host}/datasets/{dataset_name}/owners/{user_2}", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        self.assertIn("user", resp_data)
        self.assertNotEqual(len(resp_data["user"]), 0)

    def test_owner_remove(self):
        resp = requests.delete(
            f"http://{host}/datasets/{dataset_name}/owners/{user_2}", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        print(resp.status_code, resp_data)

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        self.assertIn("user", resp_data)
        self.assertNotEqual(len(resp_data["user"]), 0)

    def test_object_create(self):
        file_buf = b"""
                  //      //      //      //      //
                (o o)   (o o)   (o o)   (o o)   (o o) 
               (  V  ) (  V  ) (  V  ) (  V  ) (  V  )
              /--m-m-----m-m-----m-m-----m-m-----m-m--/
        """

        resp = requests.post(
            f"http://{host}/datasets/{dataset_name}/objects", 
            headers={
                "x-api-key": key,
            },
            json={
                "name": "birds.txt",
                "mimetype": "text/plain",
                "object": base64.b64encode(file_buf).decode(),
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))

        self.__class__.object_uuid = resp_data["uuid"]
        
        self.assertIn("version", resp_data)
        self.assertEqual(int, type(resp_data["version"]))

    def test_objects_list(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/objects", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertEqual(len(resp_data), 1)
        schema_info = resp_data[0]
        
        self.assertIn("uuid", schema_info)
        self.assertEqual(36, len(schema_info["uuid"]))
        
        self.assertIn("versions", schema_info)
        self.assertEqual(1, schema_info["versions"])

    def test_object_info(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/objects/{self.object_uuid}",
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        
        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))

        self.assertIn("versions", resp_data)
        self.assertEqual(1, resp_data["versions"])

    def test_object_update(self):
        file_buf = b"""
                 ////    ////    ////            ////
                (o o)   (o o)   (o o)           (o o) 
               (  V  ) (  V  ) (  V  )         (  V  )
              /--m-m-----m-m-----m-m-------------m-m--/
        """

        resp = requests.patch(
            f"http://{host}/datasets/{dataset_name}/objects/{self.object_uuid}", 
            headers={
                "x-api-key": key,
            },
            json={
                "name": "birds.txt",
                "mimetype": "text/plain",
                "object": base64.b64encode(file_buf).decode(),
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))

        self.assertIn("version", resp_data)
        self.assertEqual(1, resp_data["version"])

    def test_objects_list_status(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/objects/pending", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        print(resp.status_code, resp_data)

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp_data), 2)
        object_info = resp_data[0]
        
        self.assertIn("version", object_info)
        self.assertEqual(int, type(object_info["version"]))
        
        self.assertIn("uuid", object_info)
        self.assertEqual(36, len(object_info["uuid"]))
        
        self.assertIn("name", object_info)
        self.assertEqual("birds.txt", object_info["name"])

    def test_object_details(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/objects/{self.object_uuid}/1", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("object", resp_data)
        self.assertIn("bytes", resp_data)
        self.assertIn("events", resp_data)
        self.assertIn("annotations", resp_data)

    def test_object_delete(self):
        resp = requests.delete(
            f"http://{host}/datasets/{dataset_name}/objects/{self.object_uuid}/0", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))

        self.assertIn("version", resp_data)
        self.assertEqual(0, resp_data["version"])

    def test_events_list(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/events", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp_data), 9)\
        
        event_types = ['OwnerAddEvent', 
            'ObjectCreateEvent', 'ObjectUpdateEvent', 'ObjectDeleteEvent', 
            'OwnerAddEvent', 'OwnerRemoveEvent', 
            'ObjectCreateEvent', 'ObjectUpdateEvent', 'ObjectDeleteEvent']

        self.assertEqual(event_types, [ea["type"] for ea in resp_data])

        self.__class__.event_accept_uuid = resp_data[1]["uuid"]
        self.__class__.event_reject_uuid = resp_data[3]["uuid"]

    def test_event_accept(self):
        resp = requests.put(
            f"http://{host}/datasets/{dataset_name}/events"
            f"/{self.event_accept_uuid}/accept", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))

    def test_event_reject(self):
        resp = requests.put(
            f"http://{host}/datasets/{dataset_name}/events"
            f"/{self.event_reject_uuid}/reject", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)
        
        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))

    def test_annotation_create(self):
        annotation = {
            "label": "bird",
            "points": [
                {"x": 1, "y": 0},
                {"x": 7, "y": 5},
            ]
        }

        annotation_buf = json.dumps(annotation).encode()

        resp = requests.post(
            f"http://{host}/datasets/{dataset_name}/annotations", 
            headers={
                "x-api-key": key,
            },
            json={
                "schema": {
                    "name": "schema-example", 
                    "version": 1
                },
                "object_identifiers": [
                    {
                        "uuid": self.object_uuid, 
                        "version": 0
                    },
                ],
                "annotation": base64.b64encode(annotation_buf).decode(),
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))

        self.__class__.annotation_uuid = resp_data["uuid"]

        self.assertIn("version", resp_data)
        self.assertEqual(0, resp_data["version"])

    def test_annotations_list(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/annotations", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertEqual(len(resp_data), 1)
        annotation_info = resp_data[0]
        
        self.assertIn("versions", annotation_info)
        self.assertEqual(1, annotation_info["versions"])
        
        self.assertIn("uuid", annotation_info)
        self.assertEqual(36, len(annotation_info["uuid"]))

    def test_annotation_info(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/annotations/{self.annotation_uuid}",
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("versions", resp_data)
        self.assertEqual(1, resp_data["versions"])
        
        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))

    def test_annotation_update(self):
        annotation = {
            "label": "bird",
            "points": [
                {"x": 8, "y": 0},
                {"x": 15, "y": 5},
            ]
        }

        annotation_buf = json.dumps(annotation).encode()

        resp = requests.patch(
            f"http://{host}/datasets/{dataset_name}/annotations/{self.annotation_uuid}", 
            headers={
                "x-api-key": key,
            },
            json={
                "schema": {
                    "name": "schema-example", 
                    "version": 1
                },
                "annotation": base64.b64encode(annotation_buf).decode(),
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))

        self.assertIn("version", resp_data)
        self.assertEqual(1, resp_data["version"])

    def test_objects_list_status(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/annotations/pending", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertEqual(len(resp_data), 2)

        identifier = resp_data[0]
        self.assertIn("uuid", identifier)
        self.assertEqual(36, len(identifier["uuid"]))
        
        self.assertIn("version", identifier)
        self.assertEqual(int, type(identifier["version"]))

    def test_annotation_details(self):
        resp = requests.get(
            f"http://{host}/datasets/{dataset_name}/annotations/{self.annotation_uuid}/0", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("annotation", resp_data)
        self.assertIn("bytes", resp_data)
        self.assertIn("events", resp_data)
        self.assertIn("objects", resp_data)

    def test_annotation_delete(self):
        resp = requests.delete(
            f"http://{host}/datasets/{dataset_name}/annotations/{self.annotation_uuid}/0", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        if self.debug:
            print(FUNC(), resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

        self.assertIn("uuid", resp_data)
        self.assertEqual(36, len(resp_data["uuid"]))
        
        self.assertIn("version", resp_data)
        self.assertEqual(int, type(resp_data["version"]))

if __name__ == '__main__':
    unittest.main()