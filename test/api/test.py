import base64
import secrets
import inspect
import requests
import unittest

def source_order(_, a, b):
    # The following returns a list of tuples (function_name, function)
    fns = inspect.getmembers(TestAPI, predicate=inspect.isfunction)
    # Filter for only function objects that have name that starts with test_
    fns = [fn for _, fn in fns if fn.__name__.startswith("test_")]

    # Map function names to their starting line number.
    rank = {}
    for fn in fns:
        # Get starting line number. co_lines() returns an iterator of tuples. 
        # Each tuple contains:
        #   (bytecode_offset_start, bytecode_offset_end, line_number)
        # Line number might be None, so we do it this way.
        start = next(ln for ln in fn.__code__.co_lines() if ln[2] is not None)
        rank[fn.__name__] = start

    # Compare line numbers. 
    # This could be simpler because line numbers should never be equal.
    return (rank[a] > rank[b]) - (rank[a] < rank[b])

unittest.TestLoader.sortTestMethodsUsing = source_order
host = "127.0.0.1:5000"
key = "gk_Zyrhdki6a4MJ9p3dlG1ziEM7gO4yXBXa"

dataset_name = f"testing-{secrets.token_hex(4)}"
schema_name = "schema-example"

# for /f %i in ('dir /b root\datasets\') do rmdir /s /q root\datasets\%i
# rm -rf root/datasets/*

class TestAPI(unittest.TestCase):
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
        print(resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

    def test_list_datasets(self):
        resp = requests.get(
            f"http://{host}/datasets", 
            headers={
                "x-api-key": key,
            })

        resp_data = resp.json()
        print(resp.status_code, resp_data)

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
        print(resp.status_code, resp_data)

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
        print(resp.status_code, resp_data)

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
        print(resp.status_code, resp_data)

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
        print(resp.status_code, resp_data)

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
        print(resp.status_code, resp_data)

        self.assertEqual(resp.status_code, 200)

if __name__ == '__main__':
    unittest.main()