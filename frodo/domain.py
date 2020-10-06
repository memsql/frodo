##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
domain.py

core classes:
    - DBObject: representation of an object
    - Operation: SQL operation
    - Transaction: group of Operations (for generation purposes)
    - Result: result of an operation
"""

import enum
from typing import Any, Dict, Optional, List, Tuple


class DBObject:
    """
    Object in the database
    """

    def __init__(self, id: int, table: str):
        self._id: int = id
        self._table: str = table

    @property
    def id(self) -> int:
        return self._id

    @property
    def table(self) -> str:
        return self._table


class Operation:
    """
    SQL Operation (possibly on an object)
    """

    class Type(enum.Enum):
        SET_ISOLATION = enum.auto()
        BEGIN = enum.auto()
        COMMIT = enum.auto()
        ROLLBACK = enum.auto()
        READ = enum.auto()
        WRITE = enum.auto()
        PREDICATE_READ = enum.auto()

    def __init__(
        self,
        optype: Type,
        isolation_level: Optional[str] = None,
        tables: Optional[List[str]] = None,
        obj: Optional[DBObject] = None,
        for_update: bool = False,
        value: Optional[int] = None,
    ):
        """
        Initialize an operation

        Note that <_value>, when set, is the value which will be appended to the end of the value.
        OTOH, <_value_written> is the actual value which was written, which is only known at runtime
        """
        if optype == Operation.Type.SET_ISOLATION and isolation_level is None:
            raise ValueError("Operation {} requires an isolation level, had None".format(optype))

        if (optype == Operation.Type.READ or optype == Operation.Type.WRITE) and obj is None:
            raise ValueError("Operation {} requires an object, had None".format(optype))

        if (optype == Operation.Type.PREDICATE_READ) and tables is None:
            raise ValueError("Operation {} requires a list of tables, had None".format(optype))

        if (optype == Operation.Type.PREDICATE_READ) and value is None:
            raise ValueError("Operation {} requires a value, had None".format(optype))

        if optype == Operation.Type.WRITE and value is None:
            raise ValueError("Operation {} requires a value, had None".format(optype))

        self._type: Operation.Type = optype
        self._tables: Optional[List[str]] = tables
        self._obj: Optional[DBObject] = obj
        self._for_update: bool = for_update
        self._value: Optional[int] = value
        self._value_written: Optional[List[int]] = None
        self._isolation_level: Optional[str] = isolation_level

    def stmt(self, prev_ver: Optional[List[int]] = None) -> str:
        if self._type == Operation.Type.SET_ISOLATION:
            return "set transaction isolation level {}".format(self._isolation_level)
        elif self._type == Operation.Type.BEGIN:
            return "begin"
        elif self._type == Operation.Type.COMMIT:
            return "commit"
        elif self._type == Operation.Type.ROLLBACK:
            return "rollback"
        elif self._type == Operation.Type.READ:
            return "select (value) from {} as t where t.id = {}".format(self._obj.table, self._obj.id) + (
                " for update" if self._for_update else ""
            )
        elif self._type == Operation.Type.PREDICATE_READ:
            # doing this counts the number of objects (ie: length(version)),
            # which is more sensible than reasoning in terms of string length
            # assumes that there is at least one object (which is correct, since
            # the first value is "0" on all rows)
            #
            return "select id, value from ({}) where length(value) - length(replace(value,',','')) + 1 > {}".format(
                " union ".join("select id, value from {}".format(t) for t in self._tables), self._value
            ) + (" for update" if self._for_update else "")
        else:  # self._type == Operation.Type.WRITE:
            if prev_ver is None:
                raise ValueError("Cannot write without knowing the previous version")

            self._value_written = prev_ver + [self._value]
            value: str = ",".join(repr(a) for a in self._value_written)
            # This creates an object if it doesn't exist
            # Note that we cannot rely on the object being created if obj_ver[obj_id] > 0.
            # This is because obj_ver denotes the order in which the statements are *generated* not executed
            # It is incremental to ensure *uniqueness*, not *order*
            # For instance, "1,2,0,4,3" is a valid value for an object, but "1,2,1,4,3" is not
            #
            return "insert into {}(id, value) values ({}, '{}') on duplicate key update value='{}'".format(
                self._obj.table, self._obj.id, value, value
            )

    @property
    def type(self) -> Type:
        return self._type

    @property
    def tables(self) -> List[str]:
        if self._tables is None:
            raise KeyError("{} operations don't have a table".format(self._type))
        return self._tables

    @property
    def obj(self) -> DBObject:
        if self._obj is None:
            raise KeyError("{} operations don't have an object".format(self._type))
        return self._obj

    @property
    def value(self) -> int:
        if self._value is None:
            raise KeyError("{} operations don't have a value".format(self._type))
        return self._value

    @property
    def value_written(self) -> List[int]:
        if self._value is None:
            if self._type == Operation.Type.WRITE:
                raise KeyError("{} operations don't *yet* have a value".format(self._type))
            else:
                raise KeyError("{} operations don't have a value".format(self._type))

        return self._value_written

    def __repr__(self) -> str:
        if self._type == Operation.Type.SET_ISOLATION:
            return "set isolation"
        elif self._type == Operation.Type.BEGIN:
            return "begin"
        elif self._type == Operation.Type.COMMIT:
            return "commit"
        elif self._type == Operation.Type.ROLLBACK:
            return "rollback"
        elif self._type == Operation.Type.READ:
            return "r({})".format(self._obj.id)
        elif self._type == Operation.Type.WRITE:
            return "w({}, {})".format(self._obj.id, self._value)
        else:  # self._type == Operation.Type.PREDICATE_READ:
            return "pr(len > {})".format(self._value)


class Transaction:
    """
    Transaction representation for generation
    """

    def __init__(self, id: int, ops: List[Operation]):
        self._id = id
        self._ops = ops

    @property
    def id(self) -> int:
        return self._id

    @property
    def ops(self) -> List[Operation]:
        return self._ops


class Result:
    """
    Result of a SQL Query
    """

    class Type(enum.Enum):
        EMPTY = enum.auto()
        VALUE = enum.auto()
        VALUES = enum.auto()

    def __init__(self, value: Optional[List[Tuple[Any, ...]]] = None, exception: Optional[Exception] = None):

        self._type: Result.Type
        if value is None or exception is not None:
            self._type = Result.Type.EMPTY

        self._err: Optional[Exception] = exception
        self._value: Optional[List[int]] = None
        self._values: Optional[List[Tuple[int, List[int]]]] = None

        if value is not None:
            if len(value) != 1 or len(value[0]) != 1:
                # predicate read
                #
                if not all(len(row) == 2 for row in value):
                    raise ValueError("Predicate reads need to return rows with 2 columns: {}".format(value))
                elif not all(isinstance(row[0], int) for row in value):
                    raise ValueError("Predicate reads need to return an integer in the first column: {}".format(value))
                elif not all(isinstance(row[1], str) for row in value):
                    raise ValueError("Predicate reads need to return a string in the second column: {}".format(value))

                self._type = Result.Type.VALUES
                self._values = [(row[0], [int(v) for v in row[1].strip().split(",")]) for row in value]
            else:
                # item read
                #
                if not isinstance(value[0][0], str):
                    raise ValueError("Item reads should return strings: {}".format(value[0][0]))
                self._type = Result.Type.VALUE
                self._value = [int(v) for v in value[0][0].strip().split(",")]

    def is_ok(self) -> bool:
        return self._err is None

    def is_err(self) -> bool:
        return not self.is_ok()

    def is_value(self) -> bool:
        return self._type == Result.Type.VALUE

    def value(self) -> List[int]:
        if self._type != Result.Type.VALUE:
            raise TypeError("Cannot obtain value from {}".format(self._type))
        if self.is_err():
            raise ValueError("Cannot obtain value: this is an error:\n{}".format(self._err))
        return self._value

    def is_values(self) -> bool:
        return self._type == Result.Type.VALUES

    def values(self) -> List[Tuple[int, List[int]]]:
        if self._type != Result.Type.VALUES:
            raise TypeError("Cannot obtain values from {}".format(self._type))
        if self.is_err():
            raise ValueError("Cannot obtain values: this is an error:\n{}".format(self._err))
        return self._values

    def __repr__(self) -> str:
        if self._type == Result.Type.EMPTY:
            return "OK" if self.is_ok() else "ERR({})".format(self._err)
        elif self._type == Result.Type.VALUE:
            return repr(self.value()) if self.is_ok() else "ERR({})".format(self._err)
        else:  # self._type == Result.Type.VALUES:
            return repr(self.values()) if self.is_ok() else "ERR({})".format(self._err)
