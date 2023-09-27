## Gonk

Gonk is a backend for building and versioning deep learning datasets. 

### Features

* Works with any file type
* Strongly defined annotation formats using JSON Schema
* Complete dataset version history through event sourcing
* Change approval to enable collaboration with untrusted third parties

#### In Progress

* Point-in-time release tagging
* Reproducible dataset releases
* Cloning the full dataset history

### Installation

This will install the packages as well as an application `gonk-api`.

#### PyPI

```bash
pip install gonk-ai
```

#### Source

```bash
git clone https://github.com/ComputeHeavy/gonk.git
cd gonk
pip install .
```

### Running

The command `gonk-api` will run the Flask API.

To initialize, go to the folder you would like everything stored in and use -

```bash
gonk-api init --username USERNAME
```

You can manage users with - 

```bash
gonk-api users list
gonk-api users add USERNAME
gonk-api users rekey USERNAME
```

When you add a user their API key will be printed once. Give that to them. If they lose it or you want to disable their access you can use `rekey`.

You can run the server with -

```bash
gonk-api run
```

This will spawn the Flask application on `localhost:5000`. This is running in Flask's default development mode and should not be used for production, but it is probably suitable for individuals small teams. We will have a more robust solution in the future.

## Documentation

These cover the API as well as all modules.

[gonk-ai.readthedocs.io](https://gonk-ai.readthedocs.io/en/latest/)

## Design

The first two files to look at are `interfaces.py` and `events.py`. The three main interfaces are the `RecordKeeper`, `Depot`, and `State`. The `RecordKeeper` is the event storage, it acts as a linear log of events. The `Depot` stores objects (files and annotations). With those two you have a complete history of the dataset. The `State` acts as the application service, validating and processing events, maintaining the current state of the dataset. 

The file `integrity.py` has two methods for maintaining event integrity. The default is hash-chaining, where the event being added is serialized to bytes and hashed in conjunction with the previous event's hash. The other method is for signatures, which will play a larger role in a peer to peer implementation.

There are currently two implementations. There is a file system backed `Depot` and `RecordKeeper`. Then there is also a SQLite backed `RecordKeeper` and `State`. An immediate plan is to add PostgreSQL and S3 implementations for a higher scale (hosted) service.

