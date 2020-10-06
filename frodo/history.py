##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
history.py

representation of a history, with useful queries (which can be memoized)
other important types are also included (eg: HistoryElem, Anomaly)
"""

from frodo.domain import DBObject, Operation, Result
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import enum
import time


class HistoryElem:
    """
    Element of a history, encapsulating:
      - operation executed
      - return value
      - id of the connection from which it was made
      - id of the transaction
      - possible exception (indicating either abort or timeout)
    """

    def __init__(
        self,
        op: Operation,
        result: Result,
        conn_id: int,
        txn_id: int,
        invoc_time: float,
        resp_time: float = time.time(),
    ):
        self._op: Operation = op
        self._res: Result = result
        self._conn_id: int = conn_id
        self._txn_id: int = txn_id
        self._invoc: float = invoc_time
        self._resp: float = resp_time

    @property
    def op(self) -> Operation:
        """SQL statement executed"""
        return self._op

    @property
    def res(self) -> Result:
        """Return the result from the statement"""
        return self._res

    @property
    def conn_id(self) -> int:
        """ID of the connection used for the statement"""
        return self._conn_id

    @property
    def txn_id(self) -> int:
        """ID of the transaction"""
        return self._txn_id

    @property
    def invoc(self) -> float:
        """Invocation timestamp of the operation"""
        return self._invoc

    @property
    def resp(self) -> float:
        """Response timestamp of the operation"""
        return self._resp

    def __repr__(self) -> str:
        return "[conn {}, T{}]: @{}--{}: {} => {}".format(
            self.conn_id, self.txn_id, self.invoc, self.resp, self.op, self.res
        )


class ObservedTransaction:
    """
    Represent a transaction as an observed set of operations
    """

    def __init__(self, hist_list: List[HistoryElem]):
        if len(hist_list) > 0:
            txn_id = hist_list[0].txn_id
            if not all(el.txn_id == txn_id for el in hist_list):
                raise ValueError("History fragment has elements from multiple transactions")

        self._hist_list = hist_list

    @property
    def id(self) -> int:
        return self._hist_list[0].txn_id

    @property
    def hist(self) -> List[HistoryElem]:
        return self._hist_list

    def __repr__(self) -> str:
        return "T{}: {}".format(
            self.id,
            ", ".join(
                "{} -> {}".format(el.op, el.res)
                for el in filter(lambda x: x.op.type != Operation.Type.SET_ISOLATION, self._hist_list)
            ),
        )


class Anomaly(ABC):
    """
    An isolation anomaly.

    This is intended to be subclassed if there are other interesting invariants
    (eg: cyclical dependencies)

    An anomaly needs to be able to explain itself
    """

    class Type(ABC):
        """
        Anomaly type: provide simple description
        """

        @abstractmethod
        def description(cls) -> str:
            """
            Textual description of the anomaly
            """
            pass

    class G1(Type):
        @classmethod
        def description(cls) -> str:
            return "G1: dirty reads"

    @abstractmethod
    def type(self) -> Type:
        pass

    @abstractmethod
    def txns(self) -> List[ObservedTransaction]:
        pass

    @abstractmethod
    def explanation(self) -> List[str]:
        pass

    def __repr__(self) -> str:
        """
        Explain the anomaly
        """

        return """+--------------------------
| Anomaly type: {}
|
| Let:
|{}
|
| Then:
|{}
+--------------------------""".format(
            self.type().description(),
            "\n|".join("\t {}".format(txn) for txn in self.txns()),
            "\n|".join("\t {}: {}".format(idx + 1, m) for idx, m in enumerate(self.explanation())),
        )


class History:
    """
    Model a history and implement functions to query that history (with memoization)

    Available queries (some ommitted):
        - get_observed_txn: recover an ObservedTransaction from the id
        - txn_state: understand what the status of the txn is
        - final_version: get the final version an object got
        - committed_versions: get all the versions of an object which were committed
        - who_wrote: history element which wrote a particular version of an object
        - who_read: all the history elements which read a particular version of an object
        - trace: all history elements which wrote to an object
        - reads_from: all history elements which read from an object
        - is_aborted_ver: whether a particular version is from an aborted txn
        - is_installed_ver: whether a particular version was installed
        - is_intermediate_ver: whether a particular version was an intermediate value in a txn
    """

    class TransactionState(enum.Enum):
        """
        A transaction has three possible states
        """

        COMMITTED = 1  # it is guaranteed to have committed (`commit` received the ack)
        ABORTED = 2  # it is guaranteed to have aborted (`rollback` received the ack, or error implies abort)
        INDETERMINATE = 3  # no guarantees can be made (eg: communication was lost)

    def __init__(self, hist: List[HistoryElem]):
        if len(hist) < 1:
            raise ValueError("Empty history")

        # proper members
        self._hist: List[HistoryElem] = hist
        # relies on the fact that the first and last txns operate in isolation
        self._txn_range: Tuple[int, int] = (hist[0].txn_id, hist[-1].txn_id)
        if self._txn_range[1] < self._txn_range[0]:
            raise ValueError("Last transaction has a smaller id than the first")

        # memoization tables
        self._txn_state: Dict[int, History.TransactionState] = dict()
        self._who_wrote: Dict[Tuple[int, int], HistoryElem] = dict()
        self._who_read: Dict[Tuple[int, int], List[HistoryElem]] = dict()
        self._trace: Dict[int, List[HistoryElem]] = dict()

    def __repr__(self) -> str:
        return "\n".join(repr(el) for el in self._hist)

    def __iter__(self) -> Iterator[HistoryElem]:
        return iter(self._hist)

    def __len__(self) -> int:
        return len(self._hist)

    def __getitem__(self, idx: int) -> HistoryElem:
        return self._hist[idx]

    def get_objs(self) -> List[int]:
        """
        Get list of object ids from this history
        Relies on the fact that the first transaction writes to all objects
        """
        return list(
            map(
                lambda el: el.op.obj.id,
                filter(
                    lambda x: x.op.type in [Operation.Type.WRITE, Operation.Type.READ]
                    and x.txn_id == self._txn_range[0],
                    self._hist,
                ),
            )
        )

    def txn_range(self) -> Tuple[int, int]:
        return self._txn_range

    def get_observed_txn(self, txn_id: int) -> ObservedTransaction:
        return ObservedTransaction(list(filter(lambda el: el.txn_id == txn_id, self._hist)))

    def txn_state(self, txn_id: int) -> TransactionState:
        """
        Find the state of the transaction
        """
        if txn_id in self._txn_state:
            return self._txn_state[txn_id]

        if txn_id not in range(self._txn_range[0], self._txn_range[1] + 1):
            raise ValueError("T{} outside of accepted range {}".format(txn_id, self._txn_range))

        elem: Optional[HistoryElem] = next(filter(lambda x: x.txn_id == txn_id, self._hist[::-1]), None)

        if elem is None:
            raise ValueError("T{} never wrote to history".format(txn_id))

        # TODO: check which errors imply that the txn aborted, should narrow down
        # this is not necessary for correctness, but helps
        #
        if elem.op.type == Operation.Type.COMMIT and elem.res.is_ok():
            self._txn_state[txn_id] = History.TransactionState.COMMITTED
        elif elem.op.type == Operation.Type.ROLLBACK and elem.res.is_ok():
            self._txn_state[txn_id] = History.TransactionState.ABORTED
        else:
            self._txn_state[txn_id] = History.TransactionState.INDETERMINATE

        return self._txn_state[txn_id]

    def final_version(self, obj_id: int) -> List[int]:
        """
        Compute the final version of an object
        """
        # No point in memoizing this (this is linear in the number of objects)
        #

        elem: Optional[HistoryElem] = next(
            filter(
                lambda x: x.op.type == Operation.Type.READ and x.op.obj.id == obj_id,
                self._hist[::-1],
            ),
            None,
        )

        if elem is None:
            raise KeyError("Object {} does not exist".format(obj_id))

        if elem.res.is_err():
            raise ValueError("Last read of object {} returned an error: {}".format(obj_id, elem.res))

        return elem.res.value()

    def committed_versions(self, obj_id: int) -> List[List[int]]:
        """
        Compute all the committed versions of an object
        """
        # No point in memoizing this (this is linear in the number of objects)
        #

        elems: List[HistoryElem] = list(
            filter(
                lambda el: el.op.type == Operation.Type.WRITE
                and el.op.obj.id == obj_id
                and el.res.is_ok()
                and self.txn_state(el.txn_id) == History.TransactionState.COMMITTED,
                self._hist,
            )
        )
        if len(elems) == 0:
            raise KeyError("Object {} was never written (probably doesn't exist)".format(obj_id))

        filtered_elems: Dict[int, HistoryElem] = dict()
        for el in elems:
            filtered_elems[el.txn_id] = el

        return list(map(lambda el: el.op.value_written, filtered_elems.values()))

    def who_wrote(self, obj_id: int, version: int) -> HistoryElem:
        """
        History elements which wrote a version (indicated by the last int of the version list)

        No inference is made on whether the version was ever installed (ie: committed by a transaction)
        """
        if (obj_id, version) in self._who_wrote:
            return self._who_wrote[obj_id, version]
        elif obj_id in self._trace:
            elem: Optional[HistoryElem] = next(filter(lambda x: x.op.value == version, self._trace[obj_id]), None)
            if elem is not None:
                return elem

        elem = next(
            filter(
                lambda x: x.op.type == Operation.Type.WRITE and x.op.obj.id == obj_id and x.op.value == version,
                self._hist,
            ),
            None,
        )

        if elem is None:
            raise KeyError("Version {} was never written for object {}".format(version, obj_id))

        self._who_wrote[obj_id, version] = elem
        return elem

    def who_read(self, obj_id: int, version: int) -> List[HistoryElem]:
        """
        History elements who read a version (indicated by the last int of the version list)

        No inference is made on whether the version was ever installed (ie: committed by a transaction)
        """
        if (obj_id, version) in self._who_read:
            return self._who_read[obj_id, version]

        if obj_id not in self.get_objs():
            raise KeyError("Object {} does not exist".format(obj_id))

        hist: List[HistoryElem] = list(
            filter(
                lambda x: x.op.type == Operation.Type.READ
                and x.op.obj.id == obj_id
                and x.res.is_ok()
                and x.res.value()[-1] == version,
                self.reads_from(obj_id),
            )
        )

        self._who_read[obj_id, version] = hist
        return hist

    def trace(self, obj_id: int) -> List[HistoryElem]:
        """
        Compute the different versions an object undertook
        """
        if obj_id in self._trace:
            return self._trace[obj_id]

        committed_versions: List[List[int]] = self.committed_versions(obj_id)

        checked_versions: Set[int] = set()
        trace: List[HistoryElem] = list()

        for ver in committed_versions:
            for v in ver:
                if v not in checked_versions:
                    trace.append(self.who_wrote(obj_id, v))
                    checked_versions.add(v)

        self._trace[obj_id] = trace
        return trace

    def reads_from(self, obj_id: int) -> List[HistoryElem]:
        """
        Return the set of elements where *successful* reads to an object were made
        """
        return list(
            filter(
                lambda x: x.op.type == Operation.Type.READ and x.op.obj.id == obj_id and x.res.is_ok(),
                self._hist,
            )
        )

    def is_aborted_ver(self, obj_id: int, version: int) -> bool:
        """
        Check whether this version belongs to an aborted txn
        """
        elem = self.who_wrote(obj_id, version)
        return self.txn_state(elem.txn_id) == History.TransactionState.ABORTED

    def is_installed_ver(self, obj_id: int, version: int) -> bool:
        """
        Check whether this version is installed

        An object is installed if it is committed.
        For instance, given the following (committed) transaction:
          T: begin, w(1, "0,1"), w(1, "0,1,2"), commit

        "0,1,2" is an installed version, whilst "0,1" is merely an intermediate version
        """

        if self.is_aborted_ver(obj_id, version):
            return False

        trace: List[HistoryElem] = self.trace(obj_id)
        for idx, elem in enumerate(trace):
            if elem.op.value == version:
                # if this is the last version from a committed txn, this is installed
                #
                return (
                    all(e.txn_id != elem.txn_id for e in trace[idx + 1 :])
                    and self.txn_state(elem.txn_id) == History.TransactionState.COMMITTED
                )

        return False

    def is_intermediate_ver(self, obj_id: int, version: int) -> bool:
        """
        Check whether a version is an intermediate value
        """
        return (
            not self.is_aborted_ver(obj_id, version)
            and not self.is_installed_ver(obj_id, version)
            and self.txn_state(self.who_wrote(obj_id, version).txn_id) == History.TransactionState.COMMITTED
        )
