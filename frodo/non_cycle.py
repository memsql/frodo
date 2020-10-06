##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
non_cycle.py

Detect non-cyclical anomalies from a history

There are two types of non-cyclical anomalies:
    G1a - Aborted Reads: a transaction observes a value written by an aborted transaction
      eg: T1 writes w(1, 0); T2 reads r(1) -> [0]; T1 aborts

    G1b - Intermediate Reads: a transaction observes a value written (but not committed) by an ongoing txn
      eg: T1 writes w(1, 0); T1 writes w(1, 1); T1 commits; T2 reads r(1) -> [0]

"""

from frodo.domain import DBObject, Operation, Result
from frodo.history import Anomaly, History, HistoryElem, ObservedTransaction
from typing import Any, List

from abc import abstractmethod
import coloredlogs  # type: ignore
import enum
import logging

# setup logger
logger: logging.Logger = logging.getLogger(__name__)
coloredlogs.install(level="INFO")
logger.setLevel(logging.INFO)


class NonCyclicalAnomaly(Anomaly):
    class NonCyclicalAnomalyType(Anomaly.Type):
        @abstractmethod
        def description(self) -> str:
            pass

    class G1A(NonCyclicalAnomalyType):
        @classmethod
        def description(cls) -> str:
            return "G1a: read aborted write"

    class G1B(NonCyclicalAnomalyType):
        @classmethod
        def description(cls) -> str:
            return "G1b: read intermediate write"

    def __init__(self, type: Any, hist: History, reader: HistoryElem, writer: HistoryElem):
        self._type: Any = type
        self._reader: HistoryElem = reader
        self._writer: HistoryElem = writer
        self._reader_txn: ObservedTransaction = hist.get_observed_txn(self._reader.txn_id)
        self._writer_txn: ObservedTransaction = hist.get_observed_txn(self._writer.txn_id)

    def type(self) -> Any:
        return self._type

    def txns(self) -> List[ObservedTransaction]:
        return [self._reader_txn, self._writer_txn]

    def explanation(self) -> List[str]:
        return [
            "T{} reads r({}) -> {}".format(self._reader.txn_id, self._reader.op.obj.id, self._reader.res.value()),
            "{} -> {} was written by T{} which {}".format(
                self._writer.op.obj.id,
                self._writer.op.value_written,
                self._writer.txn_id,
                "aborted" if self._type == NonCyclicalAnomaly.G1A else "is an intermediate value",
            ),
        ]


def find_g1a(hist: History) -> List[Anomaly]:
    """
    Finds G1a anomalies: just needs to see if any value read was aborted
    """

    logger.info("finding G1a anomalies")
    anomalies: List[Anomaly] = list()
    for obj_id in hist.get_objs():
        for el in hist.reads_from(obj_id):
            ver: int = el.res.value()[-1]
            if hist.is_aborted_ver(obj_id, ver) and hist.who_wrote(obj_id, ver).txn_id != el.txn_id:
                anomalies.append(NonCyclicalAnomaly(NonCyclicalAnomaly.G1A, hist, el, hist.who_wrote(obj_id, ver)))

    logger.info("found {} G1a anomalies".format(len(anomalies)))

    return anomalies


def find_g1b(hist: History) -> List[Anomaly]:
    """
    Finds G1b anomalies: just needs to see if any value read was an intermediate value
    """

    logger.info("finding G1b anomalies")
    anomalies: List[Anomaly] = list()
    for obj_id in hist.get_objs():
        for el in hist.reads_from(obj_id):
            ver: int = el.res.value()[-1]
            if hist.is_intermediate_ver(obj_id, ver) and hist.who_wrote(obj_id, ver).txn_id != el.txn_id:
                anomalies.append(NonCyclicalAnomaly(NonCyclicalAnomaly.G1B, hist, el, hist.who_wrote(obj_id, ver)))

    logger.info("found {} G1b anomalies".format(len(anomalies)))

    return anomalies
