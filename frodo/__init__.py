##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

from typing import List

import click
import pickle
import pkg_resources

from .checker import check_history, parse_isolation_level
from .db import MySQLConn, DBConn
from .driver import test_isolation
from .generator import gen_history


@click.group()
@click.version_option(str(pkg_resources.get_distribution("frodo")), message="%(version)s")
def cli() -> int:
    """
    Welcome to the frodo tool.

    A black-box isolation checker for SQL databases.

    """
    return 0


def _decorator_composer(decorator_list, func):  # type: ignore
    if not decorator_list:
        return func
    head, *tail = decorator_list
    return _decorator_composer(tail, head(func))  # type: ignore


def common_options(func):  # type: ignore
    click_options = [
        click.option("-v", "--verbose/--no-verbose", help="print history to stdout"),
    ]
    return _decorator_composer(click_options, func)  # type: ignore


def generate_options(func):  # type: ignore
    click_options = [
        click.option(
            "-a",
            "--abort-rate",
            type=click.FLOAT,
            default=0.15,
            help="abort rate of the transactions",
        ),
        click.option(
            "-c",
            "--connections",
            type=click.INT,
            default=5,
            help="number of connections",
        ),
        click.option(
            "--for-update/--no-for-update",
            default=False,
            help="add `FOR UPDATE` clause to `SELECT`s",
        ),
        click.option("-n", "--objects", type=click.INT, default=16, help="number of objects"),
        click.option(
            "-pr",
            "--predicate-read-rate",
            type=click.FLOAT,
            default=0.1,
            help="predicate read rate of the operations",
        ),
        click.option(
            "-t",
            "--transactions",
            type=click.INT,
            default=100,
            help="number of transactions",
        ),
        click.option(
            "-w",
            "--write-rate",
            type=click.FLOAT,
            default=0.33,
            help="write rate of the operations",
        ),
        click.option(
            "--nodes",
            multiple=True,
            default=["127.0.0.1:3306"],
            help="list of datatabase hosts to run the queries against, format: <hostname:port>",
        ),
    ]
    return _decorator_composer(click_options, func)  # type: ignore


def check_options(func):  # type: ignore
    click_options = [
        click.option(
            "-ti",
            "--target-isolation",
            default=None,
            type=click.Choice(
                [
                    "PL-0",
                    "PL-1",
                    "read uncommitted",
                    "PL-2",
                    "read committed",
                    "PL-CS",
                    "cursor stability",
                    "PL-2L",
                    "monotonic view",
                    "PL-MSR",
                    "monotonic snapshot reads" "PL-2+",
                    "consistent view",
                    "PL-FCV",
                    "forward consistent view",
                    "PL-SI",
                    "snapshot isolation",
                    "PL-2.99",
                    "repeatable read",
                    "PL-3",
                    "serializable",
                ]
            ),
            help="isolation level which the history will be checked against",
        ),
        click.option("--full-graph/--no-full-graph", help="print the full graph"),
        click.option("-g", "--graph", help="filename for generating a graph in DOT format"),
        click.option(
            "-l",
            "--limit",
            type=click.INT,
            default=None,
            help="limit the number of anomalies reported",
        ),
        click.option(
            "-s",
            "--separate-cycles/--no-separate-cycles",
            help="separate the cycles in files named <n>_<graph_filename>.dot",
        ),
    ]
    return _decorator_composer(click_options, func)  # type: ignore


def open_connections(nodes: List[str], connections: int) -> List[DBConn]:
    addr_list = list()
    for addr in nodes:
        if len(addr.strip().split(":")) != 2:
            raise ValueError("Node addresses need to be of the form `<ip>:<port>`: {}".format(addr))

        ip, port = addr.strip().split(":")
        addr_list.append((ip, int(port)))

    if len(nodes) > connections:
        return [MySQLConn(host=node[0], port=int(node[1])) for node in nodes]
    else:
        return [
            MySQLConn(
                host=addr_list[i % len(addr_list)][0],
                port=addr_list[i % len(addr_list)][1],
            )
            for i in range(connections)
        ]


@cli.command()
@common_options
@generate_options
@click.argument(
    "isolation-level",
    metavar="isolation-level",  # type: ignore
    type=click.Choice(
        [
            "read uncommitted",
            "read committed",
            "repeatable read",
            "snapshot",
            "serializable",
        ]
    ),
)
@click.argument("output", type=click.File("wb"))
def generate(
    abort_rate,
    connections,
    for_update,
    objects,
    predicate_read_rate,
    verbose,
    transactions,
    write_rate,
    nodes,
    isolation_level,
    output,
):
    """
    Runs an SQL workload under a specific isolation level and generates an execution history.
    The history is stored in a file specified in `OUTPUT`.

    \b
    Possible isolation level values are:
      'read uncommitted'
      'read committed'
      'repeatable read'
      'snapshot'
      'serializable'
    """

    conn_list = open_connections(nodes, connections)
    history = gen_history(
        conn_list,
        isolation_level=isolation_level,
        abort_rate=abort_rate,
        write_rate=write_rate,
        predicate_read_rate=predicate_read_rate,
        n_objs=objects,
        transaction_limit=transactions,
        for_update=for_update,
    )

    if verbose:
        for el in history:
            print(el)

    pickle.dump(history, output)


@cli.command()
@common_options
@check_options
@click.argument("history-file", type=click.File("rb"))
def check(target_isolation, full_graph, graph, limit, separate_cycles, verbose, history_file):  # type: ignore
    """
    Verifies a history generated by the `generate` command.
    """
    history = pickle.load(history_file)

    if verbose:
        for txn_id in range(history.txn_range()[0], history.txn_range()[1] + 1):
            print(history.get_observed_txn(txn_id))

    print(
        "\n".join(
            repr(anom)
            for anom in check_history(
                history,
                parse_isolation_level(target_isolation),
                limit=limit,
                graph_filename=graph,
                full_graph=full_graph,
                separate_cycles=separate_cycles,
            )
        )
    )


@cli.command(name="test-isolation")
@generate_options
@check_options
@click.option("-o", "--output", type=click.File("wb"), help="save history to file")
@click.argument(
    "isolation-level",
    metavar="isolation-level",  # type: ignore
    type=click.Choice(
        [
            "read uncommitted",
            "read committed",
            "repeatable read",
            "snapshot",
            "serializable",
        ]
    ),
)
def test_isolation_cmd(
    abort_rate,
    connections,
    for_update,
    objects,
    predicate_read_rate,
    transactions,
    write_rate,
    nodes,
    target_isolation,
    full_graph,
    graph,
    limit,
    separate_cycles,
    isolation_level,
    output,
):
    """
    Runs an SQL workload under a specific isolation level, generates an execution history, and checks the history
    against the target isolation level (specified using --target-isolation).

    The history is stored in a file specified in `OUTPUT`.

    \b
    Possible isolation level values are:
      'read uncommitted'
      'read committed'
      'repeatable read'
      'snapshot'
      'serializable'
    """
    conn_list = open_connections(nodes, connections)
    anomalies, history = test_isolation(
        conn_list,
        test_isolation_level=isolation_level or target_isolation or "read committed",
        target_isolation_level=target_isolation or isolation_level or "read committed",
        abort_rate=abort_rate,
        write_rate=write_rate,
        predicate_read_rate=predicate_read_rate,
        n_objs=objects,
        transaction_limit=transactions,
        for_update=for_update,
        limit=limit,
        graph_filename=graph,
        full_graph=full_graph,
        separate_cycles=separate_cycles,
    )

    if output:
        pickle.dump(history, output)

    print("\n".join(repr(a) for a in anomalies))


def frodo_main() -> int:
    return cli(prog_name="frodo")
