Gonkumentation!
===============

Gonk is a backend for building and versioning deep learning datasets. Its goal is to do the heavy lifting for storage, validation, and approval workflows to make labeling high-quality datasets more efficient.

.. contents:: Table of Contents
    :local:
    :depth: 1

About Gonk
----------

Features
~~~~~~~~

    * Works with any file type
    * Strongly defined annotation formats using JSON Schema
    * Complete dataset version history through event sourcing
    * Change approval to enable collaboration with untrusted third parties
    * Point-in-time release tagging
    * Reproducible dataset releases
    * Cloning the full dataset history

API Workflow
~~~~~~~~~~~~

   1) Create a dataset
   2) Design an annotation format in `JSON Schema <https://json-schema.org/learn/getting-started-step-by-step.html>`
   3) Add your schema to the dataset
   4) Add objects to the dataset
   5) Annotate objects
   6) Approve or reject changes
   7) Generate a release

Gonk API Docs
-------------

.. toctree::
   :maxdepth: 3

   gonk-api <api>

Module Docs
-----------

Core
~~~~

.. toctree::
   :maxdepth: 2

   gonk.core.interfaces <modules/core/interfaces>
   gonk.core.events <modules/core/events>
   gonk.core.integrity <modules/core/integrity>
   gonk.core.validators <modules/core/validators>
   gonk.core.exceptions <modules/core/exceptions>

Implementations
~~~~~~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   gonk.impl.fs <modules/impl/fs>
   gonk.impl.sq3 <modules/impl/sq3>

Index
-----

* :ref:`genindex`
* :ref:`modindex`