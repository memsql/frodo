# Frodo Developer Notes

This documents contains useful information for developers that want to help in the development of Frodo.

# Dependencies

Check [setup.cfg](setup.cfg) file for the list of dependencies.

# Contributing Guidelines

 1. Use type annotations: it is easier to read the code in local context
 1. Any python tests should be placed in the `tests` directory.
 1. Always run `tox` before submitting your contributions.


# API

To use `frodo` as a library, here are the necessary steps:

 1. Generate a history using `gen_history`. An isolation level and a list of `DBConn`s are required. There are many other possible knobs, including a `nemesis`, which injects faults while the test is running.
 1. Check that history using `check_history`. An isolation level and a `History` are required. It is also possible to output elements of the graph as `DOT` files.
 1. If there is no need to separate the two phases, `test_isolation` combines the two calls, returning both the `History` and the list of `Anomaly`s.

# Package Structure

There are some important types in `frodo`:

 * `History` (defined in `history.py`): logged results of each operation and transcaction, which exposes a query interface.
 * `Anomaly` (defined in `history.py`): represents an anomaly, which can then be printed as an explanation.
 * `DSG` (defined in `cycle.py`): implements the _Direct Serialization Graph_, as well as cycle enumeration and conversions from cycles to `Anomaly`s.
 * `Nemesis` (defined in `nemesis.py`): API for fault injection. By subclassing and overriding its two methods (`inject` and `heal`) this will introduce faults while generating the `History`.

|File | Description|
|-----|----------|
|`checker.py` | Check whether a history is valid under some isolation level. Includes definitions of the isolation levels. It is also possible to output a [`DOT`](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) representation of the graph and its anomalies|
|`cycle.py` | Construct the DSG. It also enumerates cycles in the graph. Cyclic anomalies are also described in this file |
|`db.py` | Define the interface for adapting databases. Implements that interface for `mysql` compatible DB's |
|`domain.py` | General purpose domain types (`DBObject`, `Operation`, `Transaction`, `Result`) |
|`driver.py` | Concatenation of the history generation and anomaly checking |
|`generator.py` | Generation and execution of the workload and subsequent production of the `History` |
|`history.py` | Defines the `History` and `Anomaly` types |
|`nemesis.py` | `Nemesis` API for fault injection |
|`non_cycle.py` | Detection of non cyclical anomalies |
