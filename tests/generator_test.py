##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
generator_test.py

generator.py unit tests
"""


from frodo.nemesis import Nemesis
from frodo.db import DBConn
from frodo.generator import gen_history

from typing import Any, Callable, Optional, List, Tuple

import coloredlogs  # type: ignore
import logging
import random
import time

import unittest


class TestHistoryGeneration(unittest.TestCase):
    class Conn(DBConn):
        def reset(self) -> None:
            return

        def execute(self, stmt: str) -> Optional[List[Tuple[Any, ...]]]:
            if "begin" in stmt.lower() or "commit" in stmt.lower() or "rollback" in stmt.lower():
                return None
            elif "insert into" in stmt.lower():
                return None
            elif "select" in stmt.lower():
                if "len" in stmt.lower():
                    return [(1, "1,2,3"), (2, "0,1,2")]
                else:
                    return [("1,2,3",)]
            else:
                return None

    class FaultyConn(DBConn):
        def reset(self) -> None:
            return

        def execute(self, stmt: str) -> Optional[List[Tuple[Any, ...]]]:
            def raise_error() -> None:
                if random.random() > 0.95:
                    raise TimeoutError("Connection Reset")

            if "begin" in stmt.lower() or "commit" in stmt.lower() or "rollback" in stmt.lower():
                raise_error()
                return None
            elif "insert into" in stmt.lower():
                raise_error()
                return None
            elif "select" in stmt.lower():
                if "len" in stmt.lower():
                    return [(1, "1,2,3"), (2, "0,1,2")]
                else:
                    return [("1,2,3",)]
            else:
                return None

    class SampleNemesis(Nemesis):
        def __init__(self) -> None:
            self._logger = logging.getLogger("nemesis")
            coloredlogs.install(level="INFO")
            self._logger.setLevel(logging.INFO)

        def inject(self) -> None:
            time.sleep(3)
            self._logger.info("injected a fault")

        def heal(self) -> None:
            self._logger.info("system healed")

    def template_test_args(self, conn: DBConn) -> None:
        """Verify that arguments are correctly checked"""
        with self.assertRaises(ValueError):
            gen_history(list(), "read committed")
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", abort_rate=-0.1)
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", abort_rate=1.1)
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", write_rate=-0.1)
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", write_rate=1.1)
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", n_objs=-1)
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", n_objs=0)
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", n_tables=-1)
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", n_tables=0)
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", table_names=["", "table"])
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", n_tables=1, table_names=["a", "b"])
        with self.assertRaises(ValueError):
            gen_history([conn], "read committed", time_limit_sec=None, transaction_limit=None)

    def template_test_generation(self, conn_gen: Callable[[], DBConn], faulty: bool = False) -> None:
        """Verify that test generation connection is ok"""
        TRANSACTION_LIMIT: int = 100
        N_CONNS: int = 10
        conn_list: List[DBConn] = [conn_gen() for _ in range(N_CONNS)]
        history = gen_history(
            conn_list,
            "read committed",
            transaction_limit=TRANSACTION_LIMIT,
            nemesis=TestHistoryGeneration.SampleNemesis(),
        )
        self.assertTrue(0 < len(history), "#history = {}".format(len(history)))
        for el in history:
            print(el)
            self.assertTrue(el.conn_id in range(len(conn_list)))
            # take into account initial and final txns
            self.assertTrue(el.txn_id in range(0, TRANSACTION_LIMIT + 2))
            if not faulty:
                self.assertTrue(el.res.is_ok())

    def test_args(self) -> None:
        self.template_test_args(TestHistoryGeneration.Conn())
        self.template_test_args(TestHistoryGeneration.FaultyConn())

    def test_generation(self) -> None:
        self.template_test_generation(lambda: TestHistoryGeneration.Conn())
        self.template_test_generation(lambda: TestHistoryGeneration.FaultyConn(), True)
