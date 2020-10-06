##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
checker.py

Verify that a history is correct for a given isolation level

basic API:

    - check_history(history, isolation_level)
        * can also place a limit on the number of anomalies found
        * can output the node cycles and the graph as DOT (for visualization)
"""

from frodo.history import Anomaly, History
from frodo.cycle import DSG
from frodo.non_cycle import NonCyclicalAnomaly, find_g1a, find_g1b

import enum
from typing import Any, List, Optional, Dict


class IsolationLevel(enum.Enum):
    """
    Isolation levels as defined by Adya in his PhD thesis
    """

    PL0 = enum.auto()
    PL1 = enum.auto()

    PL2 = enum.auto()

    PLCS = enum.auto()
    PL2L = enum.auto()

    PLMSR = enum.auto()
    PL2plus = enum.auto()

    PLFCV = enum.auto()
    PLSI = enum.auto()

    PL299 = enum.auto()
    PL3U = enum.auto()

    PL3 = enum.auto()
    PLSS = enum.auto()

    def __repr__(self) -> str:
        if self.value == IsolationLevel.PL0.value:
            return "PL-0"
        elif self.value == IsolationLevel.PL1.value:
            return "PL-1"

        elif self.value == IsolationLevel.PL2.value:
            return "PL-2"

        elif self.value == IsolationLevel.PLCS.value:
            return "PL-CS - Cursor Stability"
        elif self.value == IsolationLevel.PL2L.value:
            return "PL-2L - Monotic View"

        elif self.value == IsolationLevel.PLMSR.value:
            return "PL-MSR - Monotonic Snapshot Reads"
        elif self.value == IsolationLevel.PL2plus.value:
            return "PL-2+ - Consistent View"

        elif self.value == IsolationLevel.PLFCV.value:
            return "PL-FCV - Forward Consistent View"
        elif self.value == IsolationLevel.PLSI.value:
            return "PL-SI - Snapshot Isolation"

        elif self.value == IsolationLevel.PL299.value:
            return "PL-2.99 - Repeatable Read"
        elif self.value == IsolationLevel.PL3U.value:
            return "PL-3U - Update Serializability"

        elif self.value == IsolationLevel.PL3.value:
            return "PL-3 - Full Serializability"
        elif self.value == IsolationLevel.PLSS.value:
            return "PL-SS - Strict Serializability"
        else:
            raise ValueError("Unknown IsolationLevel: {}".format(self))


def parse_isolation_level(isolation_lvl: Optional[str]) -> IsolationLevel:
    """
    Convert textual description to an isolation level
    """
    if isolation_lvl is None or len(isolation_lvl) < 2:
        return IsolationLevel.PL0

    isolation_lvl = isolation_lvl.strip().upper()
    if isolation_lvl[:2] == "PL":
        suffix: str = isolation_lvl[2:]
        if "SS" in suffix:
            return IsolationLevel.PLSS
        elif "3U" in suffix:
            return IsolationLevel.PL3U
        elif "99" in suffix:
            return IsolationLevel.PL299
        elif "SI" in suffix:
            return IsolationLevel.PLSI
        elif "FCV" in suffix:
            return IsolationLevel.PLFCV
        elif "+" in suffix or "PLUS" in suffix:
            return IsolationLevel.PL2plus
        elif "MSR" in suffix:
            return IsolationLevel.PLMSR
        elif "2L" in suffix:
            return IsolationLevel.PL2L
        elif "3" == suffix[-1]:
            return IsolationLevel.PL3
        elif "2" == suffix[-1]:
            return IsolationLevel.PL2
        elif "1" == suffix[-1]:
            return IsolationLevel.PL1
        elif "0" == suffix[-1]:
            return IsolationLevel.PL0
        else:
            raise ValueError("Unknown PL isolation level: {}".format(isolation_lvl))
    else:
        if "CURSOR" in isolation_lvl and "STABILITY" in isolation_lvl:
            return IsolationLevel.PLCS
        elif "MONOTONIC" in isolation_lvl and "VIEW" in isolation_lvl:
            return IsolationLevel.PL2L
        elif "MONOTONIC" in isolation_lvl and "SNAPSHOT" in isolation_lvl and "READS" in isolation_lvl:
            return IsolationLevel.PLMSR
        elif "CONSISTENT" in isolation_lvl and "VIEW" in isolation_lvl:
            return IsolationLevel.PLFCV if "FORWARD" in isolation_lvl else IsolationLevel.PL2plus
        elif "SNAPSHOT" in isolation_lvl and "ISOLATION" in isolation_lvl:
            return IsolationLevel.PLSI
        elif "REPEATABLE" in isolation_lvl and "READ" in isolation_lvl:
            return IsolationLevel.PL299
        elif "SERIALIZIBILITY" in isolation_lvl or "SERIALIZABLE" in isolation_lvl:
            if "UPDATE" in isolation_lvl:
                return IsolationLevel.PL3U
            elif "STRICT" in isolation_lvl:
                return IsolationLevel.PLSS
            else:
                return IsolationLevel.PL3
        elif "READ" in isolation_lvl:
            if "UNCOMMITTED" in isolation_lvl:
                return IsolationLevel.PL1  # same guess as Aphyr
            elif "COMMITTED" in isolation_lvl:
                return IsolationLevel.PL2  # same guess as Aphyr

        raise ValueError(
            "Unknown isolation level: {}\nKnown Isolation Levels:\n{}".format(
                isolation_lvl, "\n".join(repr(a) for a in IsolationLevel)
            )
        )


def proscribed_anomalies(isolation_lvl: IsolationLevel) -> List[Anomaly.Type]:
    """
    An isolation level is defined by proscribing certain anomalies

    This function encodes that information
    """
    mapping: Dict[IsolationLevel, List[Any]] = {
        IsolationLevel.PL0: [],
        IsolationLevel.PL1: [DSG.CyclicalAnomaly.G0],
        IsolationLevel.PL2: [Anomaly.G1],
        IsolationLevel.PLCS: [Anomaly.G1, DSG.CyclicalAnomaly.Gcursor],
        IsolationLevel.PL2L: [Anomaly.G1, DSG.CyclicalAnomaly.Gmonotonic],
        IsolationLevel.PLMSR: [Anomaly.G1, DSG.CyclicalAnomaly.GMSR],
        IsolationLevel.PL2plus: [Anomaly.G1, DSG.CyclicalAnomaly.Gsingle],
        IsolationLevel.PLFCV: [Anomaly.G1, DSG.CyclicalAnomaly.GSIB],
        IsolationLevel.PLSI: [Anomaly.G1, DSG.CyclicalAnomaly.GSI],
        IsolationLevel.PL299: [Anomaly.G1, DSG.CyclicalAnomaly.G2item],
        IsolationLevel.PL3U: [Anomaly.G1, DSG.CyclicalAnomaly.Gupdate],
        IsolationLevel.PL3: [Anomaly.G1, DSG.CyclicalAnomaly.G2],
        # the added restriction is that for Strict serializability this needs to be consistent with the real order
        # trivially done by constructing the RSG - Real Time Serialization Graph, which includes real time ordering edges
        #
        IsolationLevel.PL3: [Anomaly.G1, DSG.CyclicalAnomaly.G2],
    }
    return mapping[isolation_lvl]


def implies(anomaly_type: Any) -> List[Any]:
    """
    An anomaly can imply a list of other anomalies
    """
    if anomaly_type == NonCyclicalAnomaly.G1A:
        return [Anomaly.G1]
    if anomaly_type == NonCyclicalAnomaly.G1B:
        return [Anomaly.G1]
    if anomaly_type == Anomaly.G1:
        return []
    return DSG.CyclicalAnomaly.cyclical_implies(anomaly_type)


def closure(anomaly_type: Any) -> List[Any]:
    """
    Transitive closure of the `implies` relationship
    """

    def aux(l: List[Any], additions: List[Any]) -> List[Any]:
        if len(additions) == 0:
            return l
        return aux(l + additions, sum(map(lambda y: implies(y), additions), []))

    return aux(list(), [anomaly_type])


def output_dot(
    dsg: DSG,
    anomaly_types: List[Any],
    graph_filename: Optional[str] = None,
    full_graph: bool = False,
    separate_cycles: bool = False,
) -> None:
    """
    Output a DSG as a DOT graph

    If the filename is not present, do nothing.
    If <full_graph> is true, output the full DSG, not just the transactions involved in anomalies
    If <separate_cycles> is true, also ouput separate DOT files with each node cycle
    """
    if graph_filename is not None:
        if separate_cycles:
            for n, dot in enumerate(dsg.dump_dots(anomaly_types)):
                with open("{}_{}".format(n, graph_filename), "w") as f:
                    f.write(dot)
        with open(graph_filename, "w") as f:
            f.write(dsg.dump_dot(anomaly_types, full_graph))


def check_history(
    hist: History,
    isolation_level: IsolationLevel,
    limit: Optional[int] = None,
    graph_filename: Optional[str] = None,
    full_graph: bool = False,
    separate_cycles: bool = False,
) -> List[Anomaly]:
    """
    Verify that a history is valid under some isolation level

    Using <limit> the number of found anomalies can be tuned, since
    sometimes a lot of them are found, which can be quite noisy.

    Check output_dot() for the semantics of the other arguments
    """

    anomaly_types: List[Any] = proscribed_anomalies(isolation_level)
    cyclical_anomaly_types: List[Any] = list(
        filter(
            lambda anom_type: issubclass(anom_type, DSG.CyclicalAnomaly.CyclicalAnomalyType),
            anomaly_types,
        )
    )

    dsg: DSG = DSG(hist)
    anomalies: List[Anomaly] = list()

    if Anomaly.G1 in anomaly_types:
        anomalies += find_g1a(hist) + find_g1b(hist)

    for a in dsg.find_anomalies(cyclical_anomaly_types):
        anomalies.append(a)
        if limit is not None and len(anomalies) >= limit:
            break

    output_dot(dsg, cyclical_anomaly_types, graph_filename, full_graph, separate_cycles)
    return anomalies
