Gonk API
========

Gonk API is a Flask application that exposes a REST interface for dataset creation. It should be able to serve you as a generic backend for any data annotation task. This implementation should be suitable for running locally and with small teams.

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

**METHOD** ``/endpoint/<arg>``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Arguments:
        arg - A description of arg.

    Query String Parameters:
        param - A description of param.

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
        arg - A description of arg.

    Query String Parameters:
        param - A description of param.

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
