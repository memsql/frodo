##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
db.py

Database interface for frodo.

This should be a simple shim for the DB connector
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple

import coloredlogs  # type: ignore
import logging
import mysql.connector as mysql  # type: ignore
import unittest


class DBConn(ABC):
    """
    Interface for DB connections

    Initialization is the responsibility of the `__init__` method
    The `reset` method permits recovering after an error
    The `is_connected` method can be queried to assert whether the connection is live. If not, the `reset` mehtod should be used
    The `process_exception` method ingests an Exception and modifies the internal state, possible killing the connection

    It should be possible to execute SQL statements as a string, via the `execute` method, one at a time
    """

    @abstractmethod
    def reset(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def process_exception(self, e: Exception) -> None:
        return

    @abstractmethod
    def execute(self, stmt: str) -> Optional[List[Tuple[Any, ...]]]:
        pass


class MySQLConn(DBConn):
    """
    Adapter for mysql-like databases

    In fact, this is probably similar to any python DB module, so is easily extensible to other interfaces
    """

    def __init__(self, host: str = "localhost", port: int = 3306, user: str = "root"):
        """
        Create the connection
        """
        self._host: str = host
        self._port: int = port
        self._user: str = user
        self._logger: logging.Logger = logging.getLogger("db|{}@{}:{}".format(user, host, port))
        coloredlogs.install(level="INFO")
        self._logger.setLevel(logging.INFO)
        self._cur: Optional[mysql.MySQLCursor]
        self._conn: mysql.MySQLConnection
        self.connect()

    def __del__(self) -> None:
        self._cur = None
        self._conn.close()

    def connect(self) -> None:
        self._conn = mysql.connect(host=self._host, port=self._port, user=self._user)
        self._cur = self._conn.cursor()
        self._logger.info("connected to DB")

    def reset(self) -> None:
        self._conn.reconnect()
        self._cur = self._conn.cursor()
        self._logger.info("connection to DB reset")

    def process_exception(self, e: Exception) -> None:
        # TODO: get more fine grained exception processing
        # eg: some errors (like a ProgrammingError) should cause immediate
        #     termination of the process
        #
        if isinstance(e, mysql.Error):
            self._logger.info("DB Error({}): {} [{}]".format(e.errno, e.msg, e.sqlstate))
            self._cur.execute("rollback")
        else:
            self._conn.close()
            self._cur = None
            self._logger.info("closed connection to DB (Exception: {}) > {}".format(type(e), e))

    def is_connected(self) -> bool:
        return bool(self._conn.is_connected())

    def execute(self, stmt: str) -> Optional[List[Tuple[Any, ...]]]:
        if self._cur is None:
            return None

        self._logger.debug("executed '{}' in DB".format(stmt))
        self._cur.execute(stmt)
        res: List[Tuple[Any, ...]] = [r for r in iter(self._cur.fetchone, None)]

        if "select" in stmt.lower():
            return res
        else:
            return res if len(res) > 0 else None
