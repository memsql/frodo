##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
driver.py

Integrates the generator with the checker
"""

from frodo.db import DBConn
from frodo.history import Anomaly, History
from frodo.generator import gen_history
from frodo.nemesis import Nemesis
from frodo.checker import check_history, parse_isolation_level
from typing import List, Optional, Tuple


def test_isolation(
    conn_list: List[DBConn],
    test_isolation_level: str,
    target_isolation_level: str,
    abort_rate: float = 0.15,
    write_rate: float = 0.33,
    predicate_read_rate: float = 0.10,
    n_objs: Optional[int] = 16,
    n_tables: Optional[int] = 3,
    seed: Optional[int] = None,
    transaction_limit: Optional[int] = 100,
    time_limit_sec: Optional[int] = None,
    db_name: Optional[str] = None,
    table_names: Optional[List[str]] = None,
    nemesis: Optional[Nemesis] = None,
    for_update: bool = False,
    teardown: bool = True,
    limit: Optional[int] = None,
    graph_filename: Optional[str] = None,
    full_graph: bool = False,
    separate_cycles: bool = False,
) -> Tuple[List[Anomaly], History]:
    """
    Test isolation level

    Generates a history and checks for the isolation level
        <test_isolation_level>: isolation level with which the history will be generated
        <target_isolation_level>: isolation level against which the history will be checked

    Check gen_history() and check_history() for more details on the other arguments
    """

    history: History = gen_history(
        conn_list,
        test_isolation_level,
        abort_rate=abort_rate,
        write_rate=write_rate,
        n_objs=n_objs,
        n_tables=n_tables,
        seed=seed,
        transaction_limit=transaction_limit,
        time_limit_sec=time_limit_sec,
        db_name=db_name,
        table_names=table_names,
        nemesis=nemesis,
        for_update=for_update,
        teardown=teardown,
    )

    return (
        check_history(
            history,
            parse_isolation_level(target_isolation_level),
            limit,
            graph_filename,
            full_graph,
            separate_cycles,
        ),
        history,
    )
