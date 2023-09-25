Gonk API
========

Gonk API is a Flask application that exposes a REST interface for dataset creation. It should be able to serve you as a generic backend for any data annotation task. This implementation should be suitable for running locally and self-hosting with small teams.

Running
-------

There are three primary commands for the ``gonk-api`` application. 

Initialization
~~~~~~~~~~~~~~

``gonk-api init --username USERNAME`` - This will initialize the web application with an initial user named *USERNAME*.

User Management
~~~~~~~~~~~~~~~

``gonk-api users add USERNAME`` - Add a user and print their API key.

``gonk-api users rekey USERNAME`` - Regenerate a user's API key and print it out.

``gonk-api users rekey USERNAME`` - List users.

Execution
~~~~~~~~~

``gonk-api run`` - This will run the Flask application in development mode.

API Endpoints
-------------

.. contents:: Table of Contents
    :local:
    :depth: 2

``/endpoint/<arg>``
~~~~~~~~~~~~~~~~~~~

**METHOD**
^^^^^^^^^^

    Arguments:
        **arg:** A description of arg.

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
                    f"http://{host}/datasets/{dataset_name}", 
                    headers={
                        "x-api-key": key,
                    },
                    json={
                        "name": "dataset-name",
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

**POST** - Schema Create
^^^^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset to which the schema will be added.

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
                "name": "schema-name",
                "version": 0,
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
                        },
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
    Arguments:
        **dataset_name:** The dataset to list schemas for.

    Response
        .. code-block:: json

            {
                "schema_infos": [{
                    "name": "schema-example", 
                    "uuid": "82512635-040d-415c-934d-c8af96f25545", 
                    "versions": 1
                }]
            }

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

**GET** - Schema Info
^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset from which to retrieve schema info.

        **schema_name:** The schema to retrieve info for.

    Response
        .. code-block:: json

            {
                "schema_info": {
                    "name": "schema-example", 
                    "uuid": "82512635-040d-415c-934d-c8af96f25545", 
                    "versions": 1
                }
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
    Arguments:
        **dataset_name:** The dataset to update a schema in.

        **schema_name:** The schema to update.

    Request Body
        .. code-block:: json

            {
                "schema": "YmFzZTY0IGVuY29kZWQgSlNPTiBTY2hlbWEgZGVmaW5pdGlvbiBnb2VzIGhlcmU=",
            }

    Response
        .. code-block:: json

            {
                "schema_info": {
                    "name": "schema-example", 
                    "uuid": "82512635-040d-415c-934d-c8af96f25545", 
                    "versions": 2
                }
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

``/datasets/<dataset_name>/schemas/<schema_name>/<schema_version>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**GET** - Schema Load
^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset from which to retrieve a schema.

        **schema_name:** The name of the schema to retrieve.

        **schema_version:** The specific version of that schema to retrieve.

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

``/datasets/<dataset_name>/owners``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**GET** - Owners List
^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset to list owners for.

    Response
        .. code-block:: json

            {
                "owners": ["user-one"],
            }

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

**PUT** - Owner Add
^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset to add an owner to.
        **user:** The username or other identifier to add.

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
    Arguments:
        **dataset_name:** The dataset to remove an owner from.

        **user:** The username or other identifier to add.

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

**POST** - Object Create
^^^^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset to create an object in.

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
    Arguments:
        **dataset_name:** The dataset to list objects in.

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
                print(resp.status_code, resp_data)]

``/datasets/<dataset_name>/objects/<object_uuid>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**GET** - Object Info
^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset to get an object info from.

        **object_uuid:** The object UUID to get info about.

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
    Arguments:
        **dataset_name:** The dataset to update an object in.

        **object_uuid:** The object UUID to update.

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

                resp = requests.post(
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

**GET** - Objects List by Status
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset to list objects in.

        **object_status:** The status of objects to list.

            Valid statuses are ``accepted``, ``pending``, ``deleted``, ``rejected``.

    Query String Parameters:
        **after:** Object UUID after which to list more objects (pagination).

    Response
        .. code-block:: json

            {
                "identifiers": [
                    {
                        "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116", 
                        "version": 0
                    },
                    {
                        "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116", 
                        "version": 1
                    }
                ]
            }

    Code Example
        .. code-block:: python

            def objects_list_status(host, dataset_name, object_status):
                resp = requests.get(
                    f"http://{host}/datasets/{dataset_name}/objects/{object_status}", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)]

``/datasets/<dataset_name>/objects/<object_uuid>/<object_version>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**GET** - Object Load
^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset from which to retrieve an object.

        **object_uuid:** The UUID of the object to retrieve.

        **object_version:** The specific version of the object to retrieve.

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
    Arguments:
        **dataset_name:** The dataset from which to delete the object.

        **object_uuid:** The UUID of the object to delete.

        **object_version:** The specific version of the object to delete.

    Response
        .. code-block:: json

            {
                "uuid": "0d21d5a7-fe93-4618-a122-7ca9a2ee5116",
                "version": 1,
            }

    Code Example
        .. code-block:: python

            def owner_remove(host, dataset_name, object_uuid, object_version):
                resp = requests.delete(
                    f"http://{host}/datasets/{dataset_name}/objects/{object_uuid}/{object_version}", 
                    headers={
                        "x-api-key": key,
                    })
                resp_data = resp.json()
                print(resp.status_code, resp_data)

``/datasets/<dataset_name>/events``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**GET** - Events List
^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset to list events in.

    Query String Parameters:
        **after:** Event UUID after which to list more events (pagination).

    Response
        .. code-block:: json

            {
                "events": [
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
            }

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

**PUT** - Event Accept
^^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset to accept an event in.

        **event_uuid:** The UUID of the event.

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

**PUT** - Event Reject
^^^^^^^^^^^^^^^^^^^^^^
    Arguments:
        **dataset_name:** The dataset to reject an event in.

        **event_uuid:** The UUID of the event.

    Response
        .. code-block:: json

            {
                "uuid": "998cc56b-ce12-448b-afa4-9e72379e1958",
            }

    Code Example
        .. code-block:: python

            def event_accept(host, dataset_name, event_uuid):
                resp = requests.put(
                    f"http://{host}/datasets/{dataset_name}/events/{event_uuid}/reject", 
                    headers={
                        "x-api-key": key,
                    })

                resp_data = resp.json()
                print(resp.status_code, resp_data)

**METHOD** ``/endpoint/<arg>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Arguments:
        **arg:** A description of arg.

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

**METHOD** ``/endpoint/<arg>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Arguments:
        **arg:** A description of arg.

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

**METHOD** ``/endpoint/<arg>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Arguments:
        **arg:** A description of arg.

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

**METHOD** ``/endpoint/<arg>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Arguments:
        **arg:** A description of arg.

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

**METHOD** ``/endpoint/<arg>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Arguments:
        **arg:** A description of arg.

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

**METHOD** ``/endpoint/<arg>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Arguments:
        **arg:** A description of arg.

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

**METHOD** ``/endpoint/<arg>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Arguments:
        **arg:** A description of arg.

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



..
    URL with arguments
    Argument descriptions
    Query string parameters
    JSON body
    Reponse
    Sample code
