# Frodo

A black-box isolation checker for SQL databases, using [Adya](http://www.pmg.csail.mit.edu/papers/adya-phd.pdf) histories.
Written in python3.

This is work in progress, feel free to open issues/contribute!

# Setup

We recommend you use a Python virtual environment to run this tool:

```bash
virtualenv --python=python3 venv
source .venv/bin/activate
pip install --editable .
pip install '.[dev]'  # optional: installs development dependencies
```

# Usage

## Generate a history

This generates a history with 20 transactions and 5 objects on `READ COMMITTED` isolation, saving the results in
`hist.pickle`.
It assumes there is a database running on `127.0.0.1:3306`.

```bash
frodo generate -t 20 -n 5 -v --nodes 127.0.0.1:3306 'read committed' hist.pickle
frodo generate --help  # for more options
```

## Check a history

This checks a history saved in `hist.pickle`, checking it for `SERIALIZABLE` isolation, and outputing the anomaly
graph to `hist.dot`. It limits the number of found anomalies to 1.

```bash
frodo check -ti serializable -l 1 -g hist.dot hist.pickle
frodo check --help  # for more options
```

## Generate and Check in a single command

If there is no need to save the history and doing intermediate steps, `frodo test-isolation` command does the full test.
This command generates a history with 20 transactions and 5 objects under `SERIALIZABLE` isolation, printing at most
one anomaly.
It assumes there is a database node on `127.0.0.1:3306`.

```bash
frodo test-isolation -t 20 -n 5 --nodes 127.0.0.1:3306 -l 1 serializable
frodo test-isolation --help  # for more options
```

# Goal

`frodo` is a black-box tool for checking isolation guarantees of SQL databases.
This means that it tries to *find* isolation violations by querying the database
using its SQL API, without any internal visibility. Its intendend use-case is for
engineers to gain confidence in the correctness implementation of isolation.

It can be used either as a library, a framework or a standalone tool, aiming to give
developers as much flexibility as possible, through a simple API.

# Design Overview

`frodo` follows the following steps to verify the isolation guarantees of the DBMS:

 1. Generate a SQL workload
 1. Execute the workload, logging the results in a *history*
 1. Analyze the history and compute the *dependencies* between transactions.
 A dependency is created by some set of operations across two transactions which
 forces one to happen before the other (eg: one transaction writes a value and
 another reads that value)
 1. Search for *anomalies* in the dependency graph (_Direct Serialization Graph_, or DSG).
 An anomaly usually manifests itself as a cycle in the DSG.

# Anomaly Detection

We detect anomalies described by Atul Adya in his [PhD Thesis](http://www.pmg.csail.mit.edu/papers/adya-phd.pdf)). There are several anomalies:

| Anomaly | Description | Implemented? |
|---------|-------------|--------------|
| G0      | Write Cycle: cycle of WW edges | Y |
| G1a     | Aborted Read: a value which belongs to an aborted transaction was read | Y |
| G1b     | Intermediate Read: an intermediate value of a transaction was read | Y |
| G1c     | Dependency Cycle: cycle of WW and WR edges | Y |
| G2-item | Anti-Dependency Cycle: cycle of WW, WR and RW edges | Y |
| G2      | Predicate Anti-Dependency Cycle: cycle of WW, WR, RW and PRW edges | Y |
| G-single | Single Anti-Dependency Cycle: cycle of WW, WR and exactly one RW or PRW edge | Y |
| G-cursor | Labeled Single Anti-Dependency Cycle: cycle of WW, and exactly one RW, all with the same label | N |
| G-MSRa | Action Interference: when a transaction T has a read dependency to a read node in SUSG(H, T), without there being a start dependency| N |
| G-MSRb | Action Missed: there is a cycle in SUSG(H, T) with exactly one anti dependency, from a read node to T | N |
| G-monotonic | Monotonic Read: there is a cycle in USG(H, TA) with exactly one anti dependency from a read node to TB | N |
| G-SIa | Interference: there is a dependency edge in SSG(H) between A and B, but there isn't a start dependency from TA to TB | N |
| G-SIb | Missed Effects: there is a cycle edge in SSG(H) with one dependency edge | N |
| G-update | Single Anti-Dependency Cycle with Update Transactions: similar to G-single, where the DSG only includes update transactions | N |

Note that there are other serialization graphs other than the DSG:
 * _Unfolded Serialization Graph_ (USG): a graph, considering a specific transaction, unfolds that transaction in its operations, adding the corresponding edges
 * _Start Ordered Serialization Graph_ (SSG): similar to the DSG, but includes start dependency edges (ie: Ta depends on Tb if Tb committed before Ta started); this graph establishes a temporal order
 * _Start Ordered Serialization Graph_ (SUSG): a mixture of a USG and a SSG

# Contributing Guidelines

Check [HACKING.md](HACKING.md) file for development information.
