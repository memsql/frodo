##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
cycle.py

Detect cyclical anomalies from a history

A cyclical anomaly is a cycle in a Direct Serialization Graph (DGS) of the history,
which encodes dependencies between txns.

There are three types of item dependencies:
    Write-depends (ww): T0 writes x_i and T_1 writes x_{i + 1}. There is a dependency from T0 to T1
    Read-depends  (wr): T0 writes x_i and T_1 reads x_i. There is a dependency from T0 to T1
    Anti-depends  (rw): T0 reads x_i and T_1 writes x_{i + 1}. There is a dependency from T0 to T1

There are also predicate dependencies. However, these are more difficult to inspect.

For instance, write predicate queries require visibility to what values were
written and, more importantly, which values were *considered* when trying to match.

Predicate write-read dependencies are also difficult, for the same reason: the
only advantage compared to regular item WR dependencies is to the values that
didn't match the predicate, to which we have no visibility; and the ones that
did match already appear in the history as regular item reads. The interesting
case is predicate anti-dependencies (which interestingly are the separation
between REPEATABLE READ and SERIALIZABLE isolation). There is an anti dependency
from the predicate read to the future writes which update objects to values
which *would have matched* the predicate.

These dependencies represent edges on the DSG. One could think of a dependency
from TA to TB as `TA needs to happen before TB`. As such, a topological order
would give a (possibly) valid ordering of the txns. However, cycles make it
impossible for such ordering to exist.

There are several of cyclical anomalies, whose description can be found
in Atul Adya's PhD thesis. Some examples:
    G0 - cycle of ww dependencies
      eg:   T1 writes w(0, 0) (0 -> [0])       |
            T2 writes w(0, 1) (0 -> [0,1])     | ww T1 -> T2
            T1 writes w(0, 2) (0->[0,1,2])     | ww T2 -> T1
            T1 commits, T2 commits.            |

    G1b - cycle of ww and wr dependencies
      eg:   T1 writes w(0, 0) (0 -> [0])       |
            T2 writes w(0, 1) (0 -> [0,1])     | ww T1 -> T2
            T1 reads  r(0) -> [0,1]            | wr T2 -> T1
            T1 commits, T2 commits.            |

    G2 - cycle with at least one anti dependency
      eg:   T1 writes w(0, 0) (0 -> [0])       |
            T2 writes w(0, 1) (0 -> [0,1])     | ww T1 -> T2
            T3 writes w(0, 2) (0 -> [0,1,2])   | ww T2 -> T3
            T3 reads  r(0) -> [0,1,2]          |
            T1 writes w(0, 3) (0 -> [0,1,2,3]) | rw T3 -> T1

    Gsingle - G2 where the cycle has exactly one anti dependency
"""

from frodo.domain import DBObject, Operation, Result
from frodo.history import Anomaly, History, HistoryElem, ObservedTransaction
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from abc import abstractmethod

import coloredlogs  # type: ignore
import enum
import logging
import networkx as nx  # type: ignore

# setup logger
logger: logging.Logger = logging.getLogger(__name__)
coloredlogs.install(level="INFO")
logger.setLevel(logging.INFO)


class DSG:
    """
    Direct Serialization Graph of a history

    The direct serialization graph includes as nodes the transactions and
    as edges the dependencies. It is constructed directly from a recorded
    history of transactions.

    This representation allows for 3 high-level APIs:
        - find_cycles(): returns the list of *node* cycles
        - find_anomalies(anomaly_list): returns an iterable to the anomalies
                found in the graph which belong to the list
        - dump_dot() and dump_dots(): return DOT representation of the graph and
                the cycles, respectively. Accept a list of anomalies to match against
    """

    class Edge:
        """
        Edge in the DSG

        Note: all edges are directional
        """

        class Type(enum.Enum):
            WW = enum.auto()
            WR = enum.auto()
            RW = enum.auto()
            PRW = enum.auto()

            def __repr__(self) -> str:
                if self.value == DSG.Edge.Type.WW.value:
                    return "ww"
                elif self.value == DSG.Edge.Type.WR.value:
                    return "wr"
                elif self.value == DSG.Edge.Type.RW.value:
                    return "rw"
                else:  # self.value == DSG.Edge.Type.PRW.value:
                    return "prw"

        def __init__(self, etype: Type, target: Any):  # cannot forward reference Node
            self._type: DSG.Edge.Type = etype
            self._target: Any = target  # cannot forward reference Node

        def __repr__(self) -> str:
            return "-> {}".format(self.target)

        @property
        def type(self) -> Type:
            return self._type

        @property
        def target(self) -> Any:  # cannot forward reference Node
            return self._target

    class Node:
        """
        Node in the DSG
        """

        def __init__(self, txn: ObservedTransaction):
            self._txn = txn
            self._edges: List[DSG.Edge] = list()

        def add_edge(self, etype: Any, target: Any) -> None:  # surprisingly, neither Edge nor Node are in context here
            if (
                len(
                    tuple(
                        filter(
                            lambda e: e.type == etype and e.target.txn.id == target.txn.id,
                            self._edges,
                        )
                    )
                )
                == 0
                and self.txn.id != target.txn.id
            ):  # prevent loops
                self._edges.append(DSG.Edge(etype, target))

        def __repr__(self) -> str:
            return "T{}".format(self.txn.id)

        @property
        def txn(self) -> ObservedTransaction:
            return self._txn

        @property
        def edges(self) -> List[Any]:  # Edge not in context
            return self._edges

        def neighbours(self, edge_types: Set[Any]) -> List[Any]:  # Edge and Node not in context
            """
            Node neighbours reachable by one of the edge types in <edge_types>
            """
            return list(
                set(
                    map(
                        lambda edge: edge.target,
                        filter(lambda x: x.type in edge_types, self._edges),
                    )
                )
            )  # remove duplicates

    class CyclicalAnomaly(Anomaly):
        """
        An anomaly comprised of a cycle.
        It receives and identifies a cycle.
        """

        # Anomaly Types
        #
        class CyclicalAnomalyType(Anomaly.Type):
            """
            Abstract class for representing a cyclical anomaly

            The type needs:
                - provide a short description
                - be able to identify a cycle as matching its type
                - give the type of edges which can exist in its cycles
                    (this is useful for limiting the cycles in the DSG)
            """

            @abstractmethod
            def description(cls) -> str:
                pass

            @abstractmethod
            def identify_cycle(cls, node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                pass

            @abstractmethod
            def edge_types(cls) -> List[Any]:
                pass

        class G0(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G0: write cycles"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return all(e.type in DSG.CyclicalAnomaly.G0.edge_types() for e in edge_cycle)

            @classmethod
            def edge_types(cls) -> List[Any]:
                return [DSG.Edge.Type.WW]

        class G1C(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G1c: circular information flow"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return all(e.type in DSG.CyclicalAnomaly.G1C.edge_types() for e in edge_cycle)

            @classmethod
            def edge_types(cls) -> List[Any]:
                return [DSG.Edge.Type.WW, DSG.Edge.Type.WR]

        class G2item(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G2-item: item anti dependency cycle"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return all(e.type in DSG.CyclicalAnomaly.G2item.edge_types() for e in edge_cycle) and any(
                    e.type == DSG.Edge.Type.RW for e in edge_cycle
                )

            @classmethod
            def edge_types(cls) -> List[Any]:
                return [DSG.Edge.Type.WW, DSG.Edge.Type.WR, DSG.Edge.Type.RW]

        class Gsingle(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-single: single anti dependency cycle"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return (
                    all(e.type in DSG.CyclicalAnomaly.Gsingle.edge_types() for e in edge_cycle)
                    and sum(
                        map(
                            lambda e: 1 if e.type in [DSG.Edge.Type.RW, DSG.Edge.Type.PRW] else 0,
                            edge_cycle,
                        )
                    )
                    == 1
                )

            @classmethod
            def edge_types(cls) -> List[Any]:
                return [
                    DSG.Edge.Type.WW,
                    DSG.Edge.Type.WR,
                    DSG.Edge.Type.RW,
                    DSG.Edge.Type.PRW,
                ]

        class Gsingleitem(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-single-item: single item anti dependency cycle"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return (
                    all(e.type in DSG.CyclicalAnomaly.Gsingleitem.edge_types() for e in edge_cycle)
                    and sum(map(lambda e: 1 if e.type == DSG.Edge.Type.RW else 0, edge_cycle)) == 1
                )

            @classmethod
            def edge_types(cls) -> List[Any]:
                return [DSG.Edge.Type.WW, DSG.Edge.Type.WR, DSG.Edge.Type.RW]

        class G2(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G2: anti dependency cycle"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return all(e.type in DSG.CyclicalAnomaly.G2.edge_types() for e in edge_cycle) and any(
                    e.type in [DSG.Edge.Type.RW, DSG.Edge.Type.PRW] for e in edge_cycle
                )

            @classmethod
            def edge_types(cls) -> List[Any]:
                return [
                    DSG.Edge.Type.WW,
                    DSG.Edge.Type.WR,
                    DSG.Edge.Type.RW,
                    DSG.Edge.Type.PRW,
                ]

        class Gcursor(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-cursor: labeled single anti dependency cycle"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return False  # TODO: unimplemented

            @classmethod
            def edge_types(cls) -> List[Any]:
                return []  # TODO: unimplemented

        class GMSRA(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-MSRa: action interference"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return False  # TODO: unimplemented

            @classmethod
            def edge_types(cls) -> List[Any]:
                return []  # TODO: unimplemented

        class GMSRB(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-MSRb: action missed"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return False  # TODO: unimplemented

            @classmethod
            def edge_types(cls) -> List[Any]:
                return []  # TODO: unimplemented

        class GMSR(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-MSRb: action missed"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return DSG.CyclicalAnomaly.GMSRA.identify_cycle(
                    node_cycle, edge_cycle
                ) or DSG.CyclicalAnomaly.GMSRB.identify_cycle(node_cycle, edge_cycle)

            @classmethod
            def edge_types(cls) -> List[Any]:
                return []  # TODO: unimplemented

        class Gmonotonic(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-monotonic: monotonic reads"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return False  # TODO: unimplemented

            @classmethod
            def edge_types(cls) -> List[Any]:
                return []  # TODO: unimplemented

        class GSIA(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-SIa: interference"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return False  # TODO: unimplemented

            @classmethod
            def edge_types(cls) -> List[Any]:
                return []  # TODO: unimplemented

        class GSIB(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-SIb: missed effects"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return False  # TODO: unimplemented

            @classmethod
            def edge_types(cls) -> List[Any]:
                return []  # TODO: unimplemented

        class GSI(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-SI: snapshot isolation violation"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return DSG.CyclicalAnomaly.GSIA.identify_cycle(
                    node_cycle, edge_cycle
                ) or DSG.CyclicalAnomaly.GSIB.identify_cycle(node_cycle, edge_cycle)

            @classmethod
            def edge_types(cls) -> List[Any]:
                return []  # TODO: unimplemented

        class Gupdate(CyclicalAnomalyType):
            @classmethod
            def description(cls) -> str:
                return "G-update: single anti dependency cycle with update transmission"

            @staticmethod
            def identify_cycle(node_cycle: List[Any], edge_cycle: List[Any]) -> bool:
                return False  # TODO: unimplemented

            @classmethod
            def edge_types(cls) -> List[Any]:
                return []  # TODO: unimplemented

        @staticmethod
        def safe_implies(anomaly_type: Any) -> Optional[List[Any]]:
            """
            Implication between cyclical dependencies without throwing exceptions
            """
            if anomaly_type == DSG.CyclicalAnomaly.G0:
                return [DSG.CyclicalAnomaly.G1C]
            if anomaly_type == DSG.CyclicalAnomaly.G1C:
                return [DSG.CyclicalAnomaly.G1]
            if anomaly_type == DSG.CyclicalAnomaly.Gmonotonic:
                return [DSG.CyclicalAnomaly.G2item]
            if anomaly_type == DSG.CyclicalAnomaly.Gcursor:
                return [DSG.CyclicalAnomaly.G2item, DSG.CyclicalAnomaly.Gsingle]
            if anomaly_type == DSG.CyclicalAnomaly.GMSRA:
                return [DSG.CyclicalAnomaly.GMSR]
            if anomaly_type == DSG.CyclicalAnomaly.GMSRB:
                return [DSG.CyclicalAnomaly.GMSR]
            if anomaly_type == DSG.CyclicalAnomaly.GSIA:
                return [DSG.CyclicalAnomaly.GSI]
            if anomaly_type == DSG.CyclicalAnomaly.GSIB:
                return [DSG.CyclicalAnomaly.GSI]
            if anomaly_type == DSG.CyclicalAnomaly.Gupdate:
                return [DSG.CyclicalAnomaly.G2]
            if anomaly_type == DSG.CyclicalAnomaly.GMSR:
                return [DSG.CyclicalAnomaly.G2]
            if anomaly_type == DSG.CyclicalAnomaly.GSI:
                return [DSG.CyclicalAnomaly.G2]
            if anomaly_type == DSG.CyclicalAnomaly.Gsingleitem:
                return [DSG.CyclicalAnomaly.Gsingle, DSG.CyclicalAnomaly.G2item]
            if anomaly_type == DSG.CyclicalAnomaly.Gsingle:
                return [DSG.CyclicalAnomaly.G2]
            if anomaly_type == DSG.CyclicalAnomaly.G2item:
                return [DSG.CyclicalAnomaly.G2]
            if anomaly_type == DSG.CyclicalAnomaly.G2:
                return []
            return None

        @staticmethod
        def cyclical_implies(cls: Any) -> List[Any]:
            """
            Implication between cyclical dependencies
            """
            implies: List[Any] = DSG.CyclicalAnomaly.safe_implies(cls)
            if implies is None:
                raise ValueError("Unknown anomaly type: {}".format(cls))
            return implies

        @staticmethod
        def cyclical_closure(a: Any) -> List[Any]:
            """
            Compute transitive closure of a cyclical anomaly type
            """

            def aux(l: List[Any], additions: List[Any]) -> List[Any]:
                if len(additions) == 0:
                    return l
                return aux(
                    l + additions,
                    list(
                        sum(
                            filter(
                                lambda x: x is not None,
                                map(
                                    lambda y: DSG.CyclicalAnomaly.safe_implies(y),
                                    additions,
                                ),
                            ),
                            [],
                        )
                    ),
                )

            return aux(list(), [a])

        def __init__(
            self,
            dsg: Any,
            node_cycle: List[Any],
            edge_cycle: List[Any],
            final_txn: ObservedTransaction,
        ):
            if len(node_cycle) != len(edge_cycle):
                raise ValueError(
                    "Node cycle and edge cycles need to have the same size: {} vs {}".format(node_cycle, edge_cycle)
                )
            elif len(node_cycle) < 2:
                raise ValueError("Need at least two nodes in the cycle: {}".format(node_cycle))
            elif edge_cycle[-1].target != node_cycle[0]:
                raise ValueError(
                    "If the target of the last edge is not the first node, this isn't a cycle: {} vs {}".format(
                        edge_cycle[-1].target, node_cycle[0]
                    )
                )

            self._node_cycle: List[DSG.Node] = node_cycle
            self._edge_cycle: List[DSG.Edge] = edge_cycle
            self._final_txn: ObservedTransaction = final_txn
            self._dsg: DSG = dsg

        def type(self) -> CyclicalAnomalyType:
            """
            To identify the cycle, it gives it to each anomaly type
            After getting the matches, it finds the one which isn't implied by any

            This assumes that there are no cycles the implication graph of anomalies.
            Moreover, one cycle should have one (and only one) fundamental anomaly
            (ie: it shouldn't match unrelated anomalies)
            """
            possible_types: List[Any] = [
                DSG.CyclicalAnomaly.G0,
                DSG.CyclicalAnomaly.G1C,
                DSG.CyclicalAnomaly.Gmonotonic,
                DSG.CyclicalAnomaly.Gcursor,
                DSG.CyclicalAnomaly.GMSRA,
                DSG.CyclicalAnomaly.GMSRB,
                DSG.CyclicalAnomaly.GSIA,
                DSG.CyclicalAnomaly.GSIB,
                DSG.CyclicalAnomaly.Gupdate,
                DSG.CyclicalAnomaly.GMSR,
                DSG.CyclicalAnomaly.GSI,
                DSG.CyclicalAnomaly.Gsingleitem,
                DSG.CyclicalAnomaly.Gsingle,
                DSG.CyclicalAnomaly.G2item,
                DSG.CyclicalAnomaly.G2,
            ]

            matched_types: List[Any] = list(
                filter(
                    lambda x: x.identify_cycle(self._node_cycle, self._edge_cycle),
                    possible_types,
                )
            )
            if len(matched_types) == 0:
                raise ValueError("Unknown anomaly: {}, {}".format(self._node_cycle, self._edge_cycle))

            minimal_types: List[Any] = list()
            for idx, t in enumerate(matched_types):
                if all(
                    t not in DSG.CyclicalAnomaly.cyclical_closure(s)
                    for s in matched_types[:idx] + matched_types[idx + 1 :]
                ):
                    minimal_types.append(t)

            if len(minimal_types) != 1:
                raise ValueError(
                    "It should be impossible for there to be more than one minimal type: {}".format(minimal_types)
                )

            return minimal_types[0]

        def txns(self) -> List[ObservedTransaction]:
            if any(node.txn.id == self._final_txn.id for node in self._node_cycle):
                return [node.txn for node in self._node_cycle]
            else:
                return [node.txn for node in self._node_cycle] + [self._final_txn]

        def explanation(self) -> List[str]:
            def explain_dependency(dsg: DSG, orig: DSG.Node, edge: DSG.Edge) -> str:
                """
                Explain a dependency
                """
                obj_id: Optional[int]
                ver: Optional[List[int]]
                _, _, obj_id, ver = next(
                    filter(
                        lambda x: x[0] == edge.type and x[1] == edge.target.txn.id,
                        dsg.find_dependencies(orig),
                    ),
                    (None, None, None, None),
                )

                if obj_id is None:
                    raise RuntimeError(
                        "The {} dependency from T{} to T{} could not be recovered".format(
                            edge.type, orig.txn.id, edge.target.txn.id
                        )
                    )

                dep_msg: str
                if edge.type == DSG.Edge.Type.WW:
                    dep_msg = "T{} wrote version {} and T{} wrote version {} [object {}] (Write dependency)".format(
                        orig.txn.id, ver[:-1], edge.target.txn.id, ver, obj_id
                    )
                elif edge.type == DSG.Edge.Type.WR:
                    dep_msg = "T{} wrote version {} and T{} read version {} [object {}] (Read dependency)".format(
                        orig.txn.id, ver, edge.target.txn.id, ver, obj_id
                    )
                elif edge.type == DSG.Edge.Type.RW:
                    dep_msg = "T{} read version {} and T{} wrote version {} [object {}] (Item Anti dependency)".format(
                        orig.txn.id, ver[:-1], edge.target.txn.id, ver, obj_id
                    )
                elif edge.type == DSG.Edge.Type.PRW:
                    dep_msg = "T{} didn't read the object because it was two small (required len > {}), and T{} wrote the first version which matched: {} [object {}] (Predicate Anti dependency)".format(
                        orig.txn.id, len(ver) - 1, edge.target.txn.id, ver, obj_id
                    )

                return "T{} < T{}, because {}".format(orig.txn.id, edge.target.txn.id, dep_msg)

            msgs: List[str] = [
                explain_dependency(self._dsg, orig, edge) for orig, edge in zip(self._node_cycle, self._edge_cycle)
            ]
            msgs[-1] = "But {}".format(msgs[-1])
            msgs.append("This means we have a cycle (and an anomaly)")
            return msgs

    def __init__(self, hist: History):
        """
        Initialize the DSG:
            - uncommitted transactions are filtered out
            -
        """
        self._hist: History = hist
        self._nodes: List[DSG.Node] = list()
        for txn_id in range(self._hist.txn_range()[0], self._hist.txn_range()[1] + 1):
            txn: ObservedTransaction = self._hist.get_observed_txn(txn_id)
            if self._hist.txn_state(txn_id) == History.TransactionState.COMMITTED:
                self._nodes.append(DSG.Node(txn))

        self._nodes = sorted(self._nodes, key=lambda n: n.txn.id)
        self._cycles: Dict[Tuple[Any, ...], List[List[DSG.Node]]] = dict()  # memoization
        self._dependencies: Dict[DSG.Node, List[Tuple[DSG.Edge.Type, int, int, List[int]]]] = dict()  # memoization

        logger.info("finding dependencies between {} transactions".format(len(self._nodes)))
        for node in self._nodes:
            for etype, txn_id, _, _ in self.find_dependencies(node):
                node.add_edge(etype, self.get_node(txn_id))
        logger.info("constructed the Direct Serialization Graph")

    def find_dependencies(self, node: Node) -> List[Tuple[Edge.Type, int, int, List[int]]]:
        """
        Finds the dependencies originating from the node
        Returns the edge type, target txn id, obj_id, version
        """

        def longest_ver(vers: List[List[int]], val: int) -> List[int]:
            """
            find the longest version containing <val>
            """

            longest_ver: List[int] = list()
            for ver in filter(lambda v: val in v, vers):
                if len(ver) > len(longest_ver):
                    longest_ver = ver

            if len(longest_ver) == 0:
                raise ValueError("Could not find any version containing {}: {}".format(val, vers))

            return longest_ver

        def is_prefix(a: List[Any], b: List[Any]) -> bool:
            if len(a) > len(b):
                return False
            return all(a_el == b_el for a_el, b_el in zip(a, b))

        if node in self._dependencies:
            return self._dependencies[node]

        deps: List[Tuple[DSG.Edge.Type, int, int, List[int]]] = list()
        for el in filter(
            lambda e: e.op.type
            in [
                Operation.Type.WRITE,
                Operation.Type.READ,
                Operation.Type.PREDICATE_READ,
            ],
            node.txn.hist,
        ):
            if el.op.type in [Operation.Type.WRITE, Operation.Type.READ]:
                committed_vers: List[List[int]] = self._hist.committed_versions(el.op.obj.id)
                idx: int
                ver: List[int]
                if el.op.type == Operation.Type.WRITE:
                    ver = longest_ver(committed_vers, el.op.value)
                    idx = ver.index(el.op.value)
                    if idx + 1 < len(ver):
                        deps.append(
                            (
                                DSG.Edge.Type.WW,
                                self._hist.who_wrote(el.op.obj.id, ver[idx + 1]).txn_id,
                                el.op.obj.id,
                                ver[: idx + 2],
                            )
                        )

                    for next_el in self._hist.who_read(el.op.obj.id, el.op.value):
                        deps.append(
                            (
                                DSG.Edge.Type.WR,
                                next_el.txn_id,
                                el.op.obj.id,
                                ver[: idx + 1],
                            )
                        )
                elif el.op.type == Operation.Type.READ:
                    ver = longest_ver(committed_vers, el.res.value()[-1])
                    idx = ver.index(el.res.value()[-1])
                    if idx + 1 < len(ver):
                        deps.append(
                            (
                                DSG.Edge.Type.RW,
                                self._hist.who_wrote(el.op.obj.id, ver[idx + 1]).txn_id,
                                el.op.obj.id,
                                ver[: idx + 2],
                            )
                        )
            elif el.op.type == Operation.Type.PREDICATE_READ:
                boundary_len = el.op.value
                # We have a predicate anti-dependency to operations
                # which write the first version of an object which would be inserted
                # but wasn't seen by the predicate read
                #
                # We ignore predicate read-dependencies because they are invisible
                # (ie: Tj pred-read-depends on Ti if Ti writes xi and Tj accesses
                #      xi but doens't match the predicate).
                #
                # The values which *are* matched, have direct item-read-dependencies
                #
                for dep in filter(
                    lambda x: x.op.type == Operation.Type.WRITE
                    and len(x.op.value_written) == boundary_len + 1
                    and not any(is_prefix(x.op.value_written, v[1]) for v in el.res.values()),
                    self._hist,
                ):
                    deps.append(
                        (
                            DSG.Edge.Type.PRW,
                            dep.txn_id,
                            dep.op.obj.id,
                            dep.op.value_written,
                        )
                    )

        result: List[Tuple[DSG.Edge.Type, int, int, List[int]]] = list(
            filter(
                lambda x: self._hist.txn_state(x[1]) == History.TransactionState.COMMITTED,
                deps,
            )
        )
        self._dependencies[node] = result
        return result

    def get_node(self, txn_id: int) -> Node:
        node: Optional[DSG.Node] = next(filter(lambda n: n.txn.id == txn_id, self._nodes), None)
        if node is None:
            raise KeyError("Transaction T{} does not exist in the graph".format(txn_id))
        return node

    def find_cycles(self, anom_types: List[Any]) -> Iterable[List[Node]]:
        """
        Find cycles in the DSG
        """

        def convert_cycle(node_map: Dict[int, DSG.Node], cycle: List[int]) -> List[DSG.Node]:
            """
            Convert a cycle as a list of ints (txn ids) to nodes
            """
            return [node_map[txn_id] for txn_id in cycle]

        anom_types = sorted(anom_types, key=lambda x: repr(x))
        if tuple(anom_types) in self._cycles:
            for c in self._cycles[tuple(anom_types)]:
                yield c
            return None

        edge_types: Set[DSG.Edge.Type] = set(sum((anomaly.edge_types() for anomaly in anom_types), []))

        graph: nx.DiGraph = nx.DiGraph()
        for node in self._nodes:
            graph.add_node(node.txn.id)
            for neigh in node.neighbours(edge_types):
                graph.add_edge(node.txn.id, neigh.txn.id)

        logger.info("finding cyclic anomalies (this might take a while)")
        cycles: List[List[DSG.Node]] = list()
        node_map: Dict[int, DSG.Node] = {n.txn.id: n for n in self._nodes}
        for cycle in nx.simple_cycles(graph):
            converted: List[DSG.Node] = convert_cycle(node_map, cycle)
            cycles.append(converted)
            yield converted

        self._cycles[tuple(anom_types)] = cycles
        logger.info("found {} node cycles in the DSG".format(len(self._cycles[tuple(anom_types)])))

    def find_anomalies(self, anomalies: List[Any]) -> Iterable[Anomaly]:
        def classify_cycle(node_cycle: List[DSG.Node], final_txn: ObservedTransaction) -> Iterable[Anomaly]:
            """
            classify a cycle, turning it into a list of anomalies

            return the list of anomalies from the cycle
            note that since to transactions can be connected by more than one
            edge (of different types), it needs to be possible to return more
            than one anomaly from that cycle
            """

            edge_cycles: List[List[DSG.Edge]] = [[]]
            for u, v in zip(node_cycle, node_cycle[1:] + node_cycle[0:]):
                edges: List[DSG.Edge] = list(filter(lambda e: e.target == v, u.edges))
                for edge_cycle in edge_cycles:
                    # necessary because we add to edge_cycles in this loop
                    #
                    if len(edge_cycle) > 0 and edge_cycle[-1].target == v:
                        continue

                    edge_cycle.append(edges[0])
                    for e in edges[1:]:
                        edge_cycles.append(edge_cycle[:-1] + [e])

            for edge_cycle in edge_cycles:
                yield DSG.CyclicalAnomaly(self, node_cycle, edge_cycle, final_txn)

        for node_cycle in self.find_cycles(anomalies):
            for a in classify_cycle(node_cycle, self._hist.get_observed_txn(self._hist.txn_range()[1])):
                if any(anom in anomalies for anom in DSG.CyclicalAnomaly.cyclical_closure(a.type())):
                    yield a

    @staticmethod
    def list_to_dot(stmts: List[Tuple[Node, Edge]]) -> str:
        """
        Convert a list of (node, edge) pairs into a DOT representation
        WW and WR conflicts have solid arrows
        RW and PRW have dashed arrows (similar to how Adya represents them)
        """

        return """
digraph DSG {{
{}
edge [style=dashed]
{}
}}
""".format(
            "\t"
            + "\n\t".join(
                "{} -> {} [label={}];".format(n, edge.target, repr(edge.type))
                for n, edge in filter(
                    lambda x: x[1].type not in [DSG.Edge.Type.RW, DSG.Edge.Type.PRW],
                    stmts,
                )
            ),
            "\t"
            + "\n\t".join(
                "{} -> {} [label={}];".format(n, edge.target, repr(edge.type))
                for n, edge in filter(lambda x: x[1].type in [DSG.Edge.Type.RW, DSG.Edge.Type.PRW], stmts)
            ),
        )

    def dump_dot(self, anomalies: List[Any], full: bool = False) -> str:
        """
        Dump a DOT file with the DSG
        If <full> include all *committed* transactions. Otherwise, only the cycles.
        """

        stmts: List[Tuple[DSG.Node, DSG.Edge]] = list()
        if full:
            for node in self._nodes:
                for edge in node.edges:
                    stmts.append((node, edge))
        else:
            cycle_nodes: Set[DSG.Node] = set(sum((c for c in self.find_cycles(anomalies)), []))
            for node in cycle_nodes:
                for edge in filter(lambda e: e.target in cycle_nodes, node.edges):
                    stmts.append((node, edge))

        return DSG.list_to_dot(stmts)

    def dump_dots(self, anomalies: List[Any]) -> List[str]:
        """
        Dump a DOT file for each cycle
        """
        dot_list: List[str] = list()
        for cycle in self.find_cycles(anomalies):
            stmts: List[Tuple[DSG.Node, DSG.Edge]] = list()
            for node in cycle:
                for edge in filter(lambda e: e.target in cycle, node.edges):
                    stmts.append((node, edge))

            dot_list.append(DSG.list_to_dot(stmts))

        return dot_list
