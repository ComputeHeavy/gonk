import requests
import hashlib
import base64
import random
import secrets
import time
import json

key = "gk_1vCv4AoAqFunLw0boMrZreAL43DBRMS2"

def create_dataset():
    resp = requests.post(
        "http://127.0.0.1:5000/datasets", 
        headers={
            "x-api-key": key,
        },
        json={
            "name": "dogslol",
        })

    resp_data = resp.json()
    print(resp.status_code, resp_data)

def list_datasets():
    resp = requests.get(
        "http://127.0.0.1:5000/datasets", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def schema_create():
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

    resp = requests.post(
        "http://127.0.0.1:5000/datasets/dogslol/schemas", 
        headers={
            "x-api-key": key,
        },
        json={
            "name": "schema-bounding-box",
            "schema": base64.b64encode(schema_buf).decode(),
        })

    resp_data = resp.json()
    print(resp.status_code, resp_data)

def schema_list():
    resp = requests.get(
        "http://127.0.0.1:5000/datasets/dogslol/schemas", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def schema_details():
    resp = requests.get(
        "http://127.0.0.1:5000/datasets/dogslol/schemas/schema-bounding-box/0", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def schema_update():
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
                    },
                    "z": {
                        "type": "number"
                    }
                },
                "required": [
                    "x",
                    "y",
                    "z"
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
                "minItems": 3,
                "maxItems": 3
            }
        },
        "required": [
            "points",
            "label"
        ]
    }'''

    resp = requests.patch(
        "http://127.0.0.1:5000/datasets/dogslol/schemas/schema-bounding-box", 
        headers={
            "x-api-key": key,
        },
        json={
                "schema": base64.b64encode(schema_buf).decode(),
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def owner_list():
    resp = requests.get(
        "http://127.0.0.1:5000/datasets/dogslol/owners", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def owner_add():
    resp = requests.put(
        "http://127.0.0.1:5000/datasets/dogslol/owners", 
        headers={
            "x-api-key": key,
        },
        json={
            "username": "Toff",
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def owner_remove():
    resp = requests.delete(
        "http://127.0.0.1:5000/datasets/dogslol/owners", 
        headers={
            "x-api-key": key,
        },
        json={
            "username": "Toff",
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

file_buf = b"""
         ___     ___     ___     ___     ___  
        (o o)   (o o)   (o o)   (o o)   (o o) 
       (  V  ) (  V  ) (  V  ) (  V  ) (  V  )
      /--m-m-----m-m-----m-m-----m-m-----m-m--/
    """

def object_create():
    resp = requests.post(
        "http://127.0.0.1:5000/datasets/dogslol/objects", 
        headers={
            "x-api-key": key,
        },
        json={
            "name": "birds.txt",
            "mimetype": "text/plain",
            "object": base64.b64encode(file_buf).decode(),
        })

    resp_data = resp.json()
    print(resp.status_code, resp_data)

def benchmark():
    t0 = time.time()
    for ea in range(250):
        resp = requests.post(
            "http://127.0.0.1:5000/datasets/dogslol/objects", 
            headers={
                "x-api-key": key,
            },
            json={
                "name": "birds.txt",
                "mimetype": "text/plain",
                "object": base64.b64encode(file_buf + secrets.token_bytes(16)).decode(),
            })

        resp_data = resp.json()
        if ea % 100 == 0:
            print(resp.status_code, resp_data)  
            tn = time.time()
            elapsed = tn-t0
            print(ea, elapsed, elapsed/(ea+1))

    t1 = time.time()
    print("CLOCK", t1-t0)

def object_list():
    resp = requests.get(
        "http://127.0.0.1:5000/datasets/dogslol/objects", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    if resp.status_code == 200:
        print(resp_data["dataset"], len(resp_data["objects"]))
        for obj in resp_data["objects"]:
            print(obj["uuid"], obj["versions"])
    else:
        print(resp.status_code, resp_data)
        return None

    return [ea["uuid"] for ea in resp_data["objects"]]

def object_update(target):
    resp = requests.patch(
            f"http://127.0.0.1:5000/datasets/dogslol/objects/{target}", 
            headers={
                "x-api-key": key,
            },
            json={
                "name": "birds.txt",
                "mimetype": "text/plain",
                "object": base64.b64encode(file_buf + secrets.token_bytes(12)).decode(),
            })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def object_list_after(after):
    resp = requests.get(
        f"http://127.0.0.1:5000/datasets/dogslol/objects?after={after}", 
        headers={
            "x-api-key": key,
        })

    resp_data = resp.json()
    if resp.status_code == 200:
        print(resp.status_code, resp_data["dataset"], len(resp_data["objects"]))
        for obj in resp_data["objects"]:
            print(obj["uuid"], obj["versions"])
    else:
        print(resp.status_code, resp_data)

def object_delete(target):
    resp = requests.delete(
        f"http://127.0.0.1:5000/datasets/dogslol/objects/{target}/0", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def object_details(target):
    resp = requests.get(
        f"http://127.0.0.1:5000/datasets/dogslol/objects/{target}/0", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)
    return resp_data

def event_list():
    resp = requests.get(
        "http://127.0.0.1:5000/datasets/dogslol/events", 
        headers={
            "x-api-key": key,
        })

    resp_data = resp.json()
    print(resp.status_code)
    print(json.dumps(resp_data, indent=4))

def object_list_status(status):
    resp = requests.get(
        f"http://127.0.0.1:5000/datasets/dogslol/objects/{status}", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    if resp.status_code == 200:
        print(resp_data["dataset"], len(resp_data["objects"]))
        for obj in resp_data["objects"]:
            print(obj)
    else:
        print(resp.status_code, resp_data)

    return [ea["uuid"] for ea in resp_data["objects"]]

def events_list():
    resp = requests.get(
        f"http://127.0.0.1:5000/datasets/dogslol/events", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def review_accept(uuid_):
    resp = requests.put(
        f"http://127.0.0.1:5000/datasets/dogslol/events/{uuid_}/accept", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)


def review_reject(uuid_):
    resp = requests.put(
        f"http://127.0.0.1:5000/datasets/dogslol/events/{uuid_}/reject", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def annotation_create(object_uuid, object_version):
    annotation = {
        "label": "bird",
        "points": [
            {"x": 1, "y": 2},
            {"x": 3, "y": 4},
        ]
    }

    annotation_buf = json.dumps(annotation).encode()

    resp = requests.post(
        "http://127.0.0.1:5000/datasets/dogslol/annotations", 
        headers={
            "x-api-key": key,
        },
        json={
            "schema": {
                "name": "schema-bounding-box", 
                "version": 0
            },
            "object_identifiers": [
                {"uuid": object_uuid, "version": object_version},
            ],
            "annotation": base64.b64encode(annotation_buf).decode(),
        })

    resp_data = resp.json()
    print(resp.status_code, resp_data)
    return resp_data

def annotation_update(annotation_uuid):
    annotation = {
        "label": "bird",
        "points": [
            {"x": 5, "y": 4},
            {"x": 6, "y": 7},
        ]
    }

    annotation_buf = json.dumps(annotation).encode()

    resp = requests.patch(
        f"http://127.0.0.1:5000/datasets/dogslol/annotations/{annotation_uuid}", 
        headers={
            "x-api-key": key,
        },
        json={
            "schema": {
                "name": "schema-bounding-box", 
                "version": 0
            },
            "annotation": base64.b64encode(annotation_buf).decode(),
        })

    resp_data = resp.json()
    print(resp.status_code, resp_data)

def annotation_delete(target):
    resp = requests.delete(
        f"http://127.0.0.1:5000/datasets/dogslol/annotations/{target}/0", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)

def annotation_list():
    resp = requests.get(
        "http://127.0.0.1:5000/datasets/dogslol/annotations", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    if resp.status_code == 200:
        print(resp_data["dataset"], len(resp_data["annotations"]))
        for anno in resp_data["annotations"]:
            print(anno["uuid"], anno["versions"])
    else:
        print(resp.status_code, resp_data)
        return None

    return [ea["uuid"] for ea in resp_data["annotations"]]

def annotation_list_after(after):
    resp = requests.get(
        f"http://127.0.0.1:5000/datasets/dogslol/annotations?after={after}", 
        headers={
            "x-api-key": key,
        })

    resp_data = resp.json()
    if resp.status_code == 200:
        print(resp.status_code, resp_data["dataset"], len(resp_data["annotations"]))
        for anno in resp_data["annotations"]:
            print(anno["uuid"], anno["versions"])
    else:
        print(resp.status_code, resp_data)

def annotation_details(target):
    resp = requests.get(
        f"http://127.0.0.1:5000/datasets/dogslol/annotations/{target}/0", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp.status_code, resp_data)
    return resp_data

def annotation_list_status(status):
    resp = requests.get(
        f"http://127.0.0.1:5000/datasets/dogslol/annotations/{status}", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    if resp.status_code == 200:
        print(resp_data["dataset"], len(resp_data["annotations"]))
        for anno in resp_data["annotations"]:
            print(anno)
    else:
        print(resp.status_code, resp_data)

    return [ea["uuid"] for ea in resp_data["annotations"]]

def not_found():
    resp = requests.get(
        f"http://127.0.0.1:5000/doesnotexist", 
        headers={
            "x-api-key": key,
        })
    resp_data = resp.json()
    print(resp_data)

def main():
    # not_found()
    # create_dataset()
    # list_datasets()
    # schema_create()
    # schema_list()
    # schema_details()
    # schema_update()
    # owner_list()
    # owner_add()
    # owner_remove()
    # object_create()
    # benchmark()
    # objs = object_list()
    # target = random.choice(objs[4:])
    # object_update(target)
    # object_list_after(target)
    # object_delete(target)
    # object_details(target)
    event_list()
    
    # pending = object_list_status("pending")
    
    # obj_details_1 = object_details(pending[0])
    # events = [ea["uuid"] for ea in obj_details_1["events"] 
    #     if ea["review"] == "PENDING"]
    # review_accept(events[0])

    # obj_details_2 = object_details(pending[1])
    # events = [ea["uuid"] for ea in obj_details_2["events"] 
    #     if ea["review"] == "PENDING"]
    # review_reject(events[0])

    # info = obj_details_1["info"]
    # create_resp = annotation_create(info["uuid"], info["version"])
    # if "uuid" in create_resp:
    #     annotation_update(create_resp["uuid"])

    #     annotation_delete(create_resp["uuid"])

    # annos = annotation_list()
    # anno = random.choice(annos)
    # annotation_list_after(anno)
    # annotation_details(anno)
    # annotation_list_status("pending")

    # object_list_status("rejected")

    # object_details(pending[0])

if __name__ == "__main__":
    main()