Gonk API
========

Gonk API is a Flask application that exposes a REST interface for dataset creation. It should be able to serve you as a generic backend for any data annotation task. This implementation should be suitable for running locally and self-hosting with small teams.

Commands
--------

There are three primary commands for the ``gonk-api`` application. 

Initialization
~~~~~~~~~~~~~~

``gonk-api init --username USERNAME`` - This will initialize the web application with an initial user named *USERNAME*.

User Management
~~~~~~~~~~~~~~~

``gonk-api users add USERNAME`` - Add a user and print their API key.

``gonk-api users rekey USERNAME`` - Regenerate a user's API key and print it out.

``gonk-api users list`` - List users.

Running
~~~~~~~

``gonk-api run`` - This will run the Flask application in development mode.

API Endpoints
-------------

.. contents:: Table of Contents
    :local:
    :depth: 2

``/datasets``
~~~~~~~~~~~~~

**POST** - Dataset Create
^^^^^^^^^^^^^^^^^^^^^^^^^
    Request Body
        .. code-block:: json

            {
                "name": "dataset-name",
            }

    Response
        .. code-block:: json

            {
                "dataset": "dataset-name"
            }

    Code Example
        .. code-block:: python

            def create_dataset(host, dataset_name):
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

**GET** - Datasets List
^^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "datasets": ["dataset-name"]
            }

    Code Example
        .. code-block:: python

            def list_datasets(host):
                resp = requests.get(
                    f"http://{host}/datasets", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/schemas``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** Dataset name.

**POST** - Schema Create
^^^^^^^^^^^^^^^^^^^^^^^^
    Request Body
        .. code-block:: json

            {
                "name": "schema-name",
                "schema": "YmFzZTY0IGVuY29kZWQgSlNPTiBTY2hlbWEgZGVmaW5pdGlvbiBnb2VzIGhlcmU=",
            }

        Fields:
            **name (string):** Schema name. *Must be prefixed with* ``schema-``.
            
            **schema (string):** Base64 encoded JSON Schema.

    Response
        .. code-block:: json

            {
                "name": "schema-example", 
                "uuid": "82512635-040d-415c-934d-c8af96f25545", 
                "versions": 1
            }

    Code Example
        .. code-block:: python

            def schema_create(host, dataset_name):
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
                        "name": "schema-example",
                        "schema": base64.b64encode(schema_buf).decode(),
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**GET** - Schemas List
^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            [
                {
                    "name": "schema-example", 
                    "uuid": "82512635-040d-415c-934d-c8af96f25545", 
                    "versions": 1
                }
            ]

    Code Example
        .. code-block:: python

            def schema_list(host, dataset_name):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/schemas", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/schemas/<schema_name>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** Dataset name.

        **schema_name:** Schema name.

**GET** - Schema Info
^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "name": "schema-example", 
                "uuid": "82512635-040d-415c-934d-c8af96f25545", 
                "versions": 1
            }

    Code Example
        .. code-block:: python

            def schema_info(host, dataset_name, schema_name):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/schemas/{schema_name}",
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**PATCH** - Schema Update
^^^^^^^^^^^^^^^^^^^^^^^^^
    Request Body
        .. code-block:: json

            {
                "schema": "YmFzZTY0IGVuY29kZWQgSlNPTiBTY2hlbWEgZGVmaW5pdGlvbiBnb2VzIGhlcmU=",
            }

    Response
        .. code-block:: json

            {
                "name": "schema-example", 
                "uuid": "82512635-040d-415c-934d-c8af96f25545", 
                "versions": 2
            }

    Code Example
        .. code-block:: python

            def schema_update(host, dataset_name, schema_name):
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

``/datasets/<dataset_name>/schemas/<schema_status>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** The dataset to list schemas in.

        **schema_status:** The status of schemas to list.

            Valid statuses are ``accepted``, ``pending``, ``deprecated``, ``rejected``.

**GET** - Schemas List by Status
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            [
                {
                    "uuid": "82512635-040d-415c-934d-c8af96f25545", 
                    "name": "schema-example",
                    "version": 0
                },
                {
                    "uuid": "82512635-040d-415c-934d-c8af96f25545", 
                    "name": "schema-example",
                    "version": 1
                }
            ]

    Code Example
        .. code-block:: python

            def schema_list_status(host, dataset_name, schema_status):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/schemas/{schema_status}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/schemas/<schema_name>/<schema_version>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** The dataset from which to retrieve a schema.

        **schema_name:** The name of the schema to retrieve.

        **schema_version:** The specific version of that schema to retrieve.

**GET** - Schema Load
^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "schema": {
                    "format": "application/schema+json",
                    "hash": "3cc74a17c988639b288637004d86a2334cf1d50a6b0e7edc827449c7918bcf1c",
                    "hash_type": 1,
                    "name": "schema-bounding-box",
                    "size": 47,
                    "uuid": "82512635-040d-415c-934d-c8af96f25545",
                    "version": 0
                },
                "bytes": "YmFzZTY0IGVuY29kZWQgSlNPTiBTY2hlbWEgZGVmaW5pdGlvbiBnb2VzIGhlcmU="
            }

    Code Example
        .. code-block:: python

            def schema_details(host, dataset_name, schema_name, schema_version):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/schemas/{schema_name}/{schema_version}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**DELETE** - Schema Deprecate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "uuid": "82512635-040d-415c-934d-c8af96f25545",
                "version": 0,
                "name": "schema-example"
            }

    Code Example
        .. code-block:: python

            def schema_deprecate(host, dataset_name, schema_name, schema_version):
                resp = requests.delete(
                    f"http://{host}/datasets/{dataset_name}/schemas/{schema_name}/{schema_version}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/owners``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** The dataset to list owners for.

**GET** - Owners List
^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            [
                "user-one"
            ]

    Code Example
        .. code-block:: python

            def owner_list(host, dataset_name):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/owners", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/owners/<user>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** Dataset name.

        **user:** The username or other identifier.

**PUT** - Owner Add
^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "user": "user-two",
            }

    Code Example
        .. code-block:: python

            def owner_add(host, dataset_name, user):
                resp = requests.put(
                    f"http://{host}/datasets/{dataset_name}/owners/{user}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**DELETE** - Owner Remove
^^^^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "user": "user-two",
            }

    Code Example
        .. code-block:: python

            def owner_remove(host, dataset_name, user):
                resp = requests.delete(
                    f"http://{host}/datasets/{dataset_name}/owners/{user}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/objects``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   Arguments:
        **dataset_name:** Dataset name.

**POST** - Object Create
^^^^^^^^^^^^^^^^^^^^^^^^
    Request Body
        .. code-block:: json

            {
                "name": "filename.ext",
                "mimetype": "mime/type",
                "object": "YmFzZTY0IGVuY29kZWQgZmlsZSBieXRlcyBnbyBoZXJl"
            }

    Response
        .. code-block:: json

            {
                "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116",
                "version": 0
            }

    Code Example
        .. code-block:: python

            def object_create(host, dataset_name):
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
                print(resp.status_code, resp_data)

**GET** - Objects List
^^^^^^^^^^^^^^^^^^^^^^
    Query String Parameters:
        **after:** Object UUID after which to list more objects (pagination).

    Response
        .. code-block:: json

            {
                "object_infos": [
                    {
                        "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116", 
                        "versions": 1
                    }
                ]
            }

    Code Example
        .. code-block:: python

            def objects_list(host, dataset_name):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/objects", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/objects/<object_uuid>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** Dataset name.

        **object_uuid:** Object UUID.

**GET** - Object Info
^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "object_info": {
                    "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116", 
                    "versions": 1
                }
            }

    Code Example
        .. code-block:: python

            def object_info(host, dataset_name, object_uuid):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/objects/{object_uuid}",
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**PATCH** - Object Update
^^^^^^^^^^^^^^^^^^^^^^^^^
    Request Body
        .. code-block:: json

            {
                "name": "filename.ext",
                "mimetype": "mime/type",
                "object": "YmFzZTY0IGVuY29kZWQgZmlsZSBieXRlcyBnbyBoZXJl"
            }

    Response
        .. code-block:: json

            {
                "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116",
                "version": 1
            }

    Code Example
        .. code-block:: python

            def object_update(host, dataset_name, object_uuid):
                file_buf = b"""
                         ////    ////    ////            ////
                        (o o)   (o o)   (o o)           (o o) 
                       (  V  ) (  V  ) (  V  )         (  V  )
                      /--m-m-----m-m-----m-m-------------m-m--/
                """

                resp = requests.patch(
                    f"http://{host}/datasets/{dataset_name}/objects/{object_uuid}", 
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

``/datasets/<dataset_name>/objects/<object_status>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** The dataset to list objects in.

        **object_status:** The status of objects to list.

            Valid statuses are ``accepted``, ``pending``, ``deleted``, ``rejected``.

**GET** - Objects List by Status
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Query String Parameters:
        **after:** Object UUID after which to list more objects (pagination).

    Response
        .. code-block:: json

            [
                {
                    "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116", 
                    "version": 0
                },
                {
                    "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116", 
                    "version": 1
                }
            ]

    Code Example
        .. code-block:: python

            def objects_list_status(host, dataset_name, object_status):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/objects/{object_status}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/objects/<object_uuid>/<object_version>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** Dataset name.

        **object_uuid:** Object UUID.

        **object_version:** Object version.


**GET** - Object Load
^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "object": {
                    "format": "text/plain",
                    "hash": "53e547e0ce81e73a132b5468ed83531fdebe1f7c11e911ddd339a12574debb43",
                    "hash_type": 1,
                    "name": "birds.txt",
                    "size": 209,
                    "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116",
                    "version": 1
                },
                "bytes": "cHJldGVuZCB0aGF0IGJpcmRzLnR4dCBpcyBlbmNvZGVkIGhlcmU=",
                "events": [{
                    "review": "PENDING", 
                    "type": "ObjectCreateEvent", 
                    "uuid": "84ecfacd-e404-4e3c-94a4-8c939cd9159d"
                }],
                "annotations": [{
                    "uuid": "704e816c-30ae-4184-a4ed-eee9efe589be", 
                    "versions": 1
                }],
            }

    Code Example
        .. code-block:: python

            def object_details(host, dataset_name, object_uuid, object_version):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/objects/{object_uuid}/{object_version}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**DELETE** - Object Delete
^^^^^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116",
                "version": 1,
            }

    Code Example
        .. code-block:: python

            def object_delete(host, dataset_name, object_uuid, object_version):
                resp = requests.delete(
                    f"http://{host}/datasets/{dataset_name}/objects/{object_uuid}/{object_version}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/events``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** The dataset to list events in.

**GET** - Events List
^^^^^^^^^^^^^^^^^^^^^
    Query String Parameters:
        **after:** Event UUID after which to list more events (pagination).

    Response
        .. code-block:: json

            [
                {
                    "author": "user-one",
                    "integrity": "6d4e3364c396240fe6d4274fe0e9e2872872a30a0c061e727379e5e66e7c8044",
                    "owner": "user-one",
                    "owner_action": 1,
                    "timestamp": "2001-09-11T03:44:37.229078Z",
                    "type": "OwnerAddEvent",
                    "uuid": "3fcfcfd4-09c7-4b57-92f0-6390a94152ee"
                },
                {
                    "action": 1,
                    "author": "user-one",
                    "integrity": "fa8703478a5b3fb29dd7c49b7442ac7046954a08a36d02d86d02e978e1fea7f4",
                    "object": {
                        "format": "application/schema+json",
                        "hash": "3cc74a17c988639b288637004d86a2334cf1d50a6b0e7edc827449c7918bcf1c",
                        "hash_type": 1,
                        "name": "schema-bounding-box",
                        "size": 47,
                        "uuid": "82512635-040d-415c-934d-c8af96f25545",
                        "version": 0
                    },
                    "timestamp": "2001-09-11T03:44:37.245083Z",
                    "type": "ObjectCreateEvent",
                    "uuid": "998cc56b-ce12-448b-afa4-9e72379e1958"
                }
            ]

    Code Example
        .. code-block:: python

            def events_list(host, dataset_name):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/events", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, json.dumps(resp_data, indent=4))

``/datasets/<dataset_name>/events/<event_uuid>/accept``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** The dataset to accept an event in.

        **event_uuid:** The UUID of the event.

**PUT** - Event Accept
^^^^^^^^^^^^^^^^^^^^^^

    Response
        .. code-block:: json

            {
                "uuid": "998cc56b-ce12-448b-afa4-9e72379e1958",
            }

    Code Example
        .. code-block:: python

            def event_accept(host, dataset_name, event_uuid):
                resp = requests.put(
                    f"http://{host}/datasets/{dataset_name}/events/{event_uuid}/accept", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/events/<event_uuid>/reject``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** The dataset to reject an event in.

        **event_uuid:** The UUID of the event.

**PUT** - Event Reject
^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "uuid": "998cc56b-ce12-448b-afa4-9e72379e1958",
            }

    Code Example
        .. code-block:: python

            def event_reject(host, dataset_name, event_uuid):
                resp = requests.put(
                    f"http://{host}/datasets/{dataset_name}/events/{event_uuid}/reject", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/annotations``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** Dataset name.

**POST** - Annotation Create
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    Request Body
        .. code-block:: json

            {
                "schema": {
                    "name": "schema-example", 
                    "version": 2
                },
                "object_identifiers": [
                    {
                        "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116", 
                        "version": 1
                    },
                ],
                "annotation": "cHJldGVuZCB0aGF0IHRoZSBhbm5vdGF0aW9uIGlzIGVuY29kZWQgaGVyZQ=="
            }

    Response
        .. code-block:: json

            {
                "uuid": "704e816c-30ae-4184-a4ed-eee9efe589be",
                "version": 0,
            }

    Code Example
        .. code-block:: python

            def annotation_create(host, dataset_name, object_uuid, object_version):
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
                                "uuid": object_uuid, 
                                "version": object_version
                            },
                        ],
                        "annotation": base64.b64encode(annotation_buf).decode(),
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**GET** - Annotations List
^^^^^^^^^^^^^^^^^^^^^^^^^^
    Query String Parameters:
        **after:** Annotations UUID after which to list more annotations (pagination).

    Response
        .. code-block:: json

            [
                {
                    "uuid": "704e816c-30ae-4184-a4ed-eee9efe589be", 
                    "versions": 1
                }
            ]

    Code Example
        .. code-block:: python

            def annotations_list(host, dataset_name):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/annotations", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/annotations/<annotation_uuid>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** Dataset name.

        **annotation_uuid:** Annotation UUID.

**GET** - Annotation Info
^^^^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "uuid": "704e816c-30ae-4184-a4ed-eee9efe589be", 
                "versions": 1
            }

    Code Example
        .. code-block:: python

            def annotation_info(host, dataset_name, annotation_uuid):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/annotations/{annotation_uuid}",
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**PATCH** - Annotation Update
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Request Body
        .. code-block:: json

            {
                "schema": {
                    "name": "schema-example", 
                    "version": 2
                },
                "annotation": "cHJldGVuZCB0aGF0IHRoZSBhbm5vdGF0aW9uIGlzIGVuY29kZWQgaGVyZQ=="
            }

    Response
        .. code-block:: json

            {
                "uuid": "704e816c-30ae-4184-a4ed-eee9efe589be",
                "version": 1,
            }

    Code Example
        .. code-block:: python

            def annotation_update(host, dataset_name, annotation_uuid):
                annotation = {
                    "label": "bird",
                    "points": [
                        {"x": 8, "y": 0},
                        {"x": 15, "y": 5},
                    ]
                }

                annotation_buf = json.dumps(annotation).encode()

                resp = requests.patch(
                    f"http://{host}/datasets/{dataset_name}/annotations/{annotation_uuid}", 
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
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/annotations/<annotation_status>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** The dataset to list annotations in.

        **annotation_status:** The status of annotations to list.

            Valid statuses are ``accepted``, ``pending``, ``deleted``, ``rejected``.

**GET** - Annotation List by Status
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Query String Parameters:
        **after:** Annotation UUID after which to list more annotations (pagination).

    Response
        .. code-block:: json

            [
                {
                    "uuid": "704e816c-30ae-4184-a4ed-eee9efe589be", 
                    "version": 0
                },
                {
                    "uuid": "704e816c-30ae-4184-a4ed-eee9efe589be", 
                    "version": 1
                }
            ]

    Code Example
        .. code-block:: python

            def objects_list_status(host, dataset_name, annotation_status):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/annotations/{annotation_status}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/annotations/<annotation_uuid>/<annotation_version>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Arguments:
        **dataset_name:** Dataset name.

        **annotation_uuid:** Annotation UUID.

        **annotation_version:** Annotation version.

**GET** - Annotation Load
^^^^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "annotation": {
                    "hash": "154b716261fa69284dabac3d6a3a28b93e1c2b6596f60245da8cbaa12b8db2dd",
                    "hash_type": 1,
                    "schema": {
                        "uuid": "82512635-040d-415c-934d-c8af96f25545",
                        "version": 1
                    },
                    "size": 65,
                    "uuid": "704e816c-30ae-4184-a4ed-eee9efe589be",
                    "version": 0
                },
                "bytes": "eyJsYWJlbCI6ICJiaXJkIiwgInBvaW50cyI6IFt7IngiOiAxLCAieSI6IDJ9LCB7IngiOiAzLCAieSI6IDR9XX0=",
                "events": [
                    {
                        "review": "PENDING",
                        "type": "AnnotationCreateEvent",
                        "uuid": "040573d5-6008-4cca-b25a-97d4e5976bf8"
                    },
                    {
                        "review": "PENDING",
                        "type": "AnnotationDeleteEvent",
                        "uuid": "7f3229d1-27ce-4af4-9bcc-95869550e53e"
                    }
                ],
                "objects": [
                    {
                        "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116",
                        "version": 0
                    }
                ]
            }

    Code Example
        .. code-block:: python

            def annotation_details(host, dataset_name, annotation_uuid, annotation_version):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/annotations/{annotation_uuid}/{annotation_version}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**DELETE** - Annotation Delete
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Response
        .. code-block:: json

            {
                "uuid": "704e816c-30ae-4184-a4ed-eee9efe589be",
                "version": 1,
            }

    Code Example
        .. code-block:: python

            def annotation_delete(host, dataset_name, annotation_uuid, annotation_version):
                resp = requests.delete(
                    f"http://{host}/datasets/{dataset_name}/annotations/{annotation_uuid}/{annotation_version}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

.. 
    ``/endpoint/<arg>``
    ~~~~~~~~~~~~~~~~~~~
        Arguments:
            **arg:** A description of arg.

    **METHOD**
    ^^^^^^^^^^
        Query String Parameters:
            **param:** A description of param.

        Request Body
            .. code-block:: json

                {
                    "key": "value"
                }

        Response
            .. code-block:: json

                {
                    "key": "value"
                }

        Code Example
            .. code-block:: python

                request.get()