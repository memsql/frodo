##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
cycle_test.py

cycle.py unit tests

In this test, several hand-generated histories are checked
"""
import unittest

from frodo.cycle import DSG
from frodo.domain import DBObject, Operation, Result
from frodo.history import Anomaly, History, HistoryElem


class TestCyclicalAnomalies(unittest.TestCase):
    def get_g0_anomaly_hist(self) -> History:
        obj: DBObject = DBObject(0, "tab")
        hist: History = History(
            [
                # 0
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0
                ),
                # 1
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 2
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=0), Result(), 0, 0, 0.0, 0.0),
                # 3
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 4
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0
                ),
                # 5
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 6
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=1), Result(), 0, 1, 0.0, 0.0),
                # 7
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=4), Result(), 0, 1, 0.0, 0.0),
                # 8
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 9
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0
                ),
                # 10
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 11
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=2), Result(), 0, 2, 0.0, 0.0),
                # 12
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 13
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 14
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 15
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=3), Result(), 0, 3, 0.0, 0.0),
                # 16
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 17
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0
                ),
                # 18
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
                # 19
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1,2,3,4",)]), 0, 4, 0.0, 0.0),
                # 20
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
            ]
        )

        hist[2].op.stmt([])
        hist[6].op.stmt([0])
        hist[7].op.stmt([0, 1, 2, 3])
        hist[11].op.stmt([0, 1])
        hist[15].op.stmt([0, 1, 2])
        return hist

    def get_g1c_anomaly_hist(self) -> History:
        obj: DBObject = DBObject(0, "tab")
        hist: History = History(
            [
                # 0
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0
                ),
                # 1
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 2
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=0), Result(), 0, 0, 0.0, 0.0),
                # 3
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 4
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0
                ),
                # 5
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 6
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=1), Result(), 0, 1, 0.0, 0.0),
                # 7
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1,2,3",)]), 0, 1, 0.0, 0.0),
                # 8
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 9
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0
                ),
                # 10
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 11
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=2), Result(), 0, 2, 0.0, 0.0),
                # 12
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 13
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 14
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 15
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=3), Result(), 0, 3, 0.0, 0.0),
                # 16
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 17
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0
                ),
                # 18
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
                # 19
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1,2,3",)]), 0, 4, 0.0, 0.0),
                # 20
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
            ]
        )

        hist[2].op.stmt([])
        hist[6].op.stmt([0])
        hist[11].op.stmt([0, 1])
        hist[15].op.stmt([0, 1, 2])
        return hist

    def get_g2_anomaly_hist(self) -> History:
        obj: DBObject = DBObject(0, "tab")
        hist: History = History(
            [
                # 0
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0
                ),
                # 1
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 2
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=0), Result(), 0, 0, 0.0, 0.0),
                # 3
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 4
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0
                ),
                # 5
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 6
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=1), Result(), 0, 1, 0.0, 0.0),
                # 7
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 8
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0
                ),
                # 9
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 10
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=2), Result(), 0, 2, 0.0, 0.0),
                # 11
                HistoryElem(
                    Operation(Operation.Type.PREDICATE_READ, tables=["tab"], value=3), Result(value=[]), 0, 2, 0.0, 0.0
                ),
                # 12
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 13
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 14
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 15
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=3), Result(), 0, 3, 0.0, 0.0),
                # 16
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0",)]), 0, 3, 0.0, 0.0),
                # 17
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 18
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0
                ),
                # 19
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
                # 20
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1,2,3",)]), 0, 4, 0.0, 0.0),
                # 21
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
            ]
        )

        hist[2].op.stmt([])
        hist[6].op.stmt([0])
        hist[10].op.stmt([0, 1])
        hist[15].op.stmt([0, 1, 2])
        return hist

    def get_g2item_anomaly_hist(self) -> History:
        obj: DBObject = DBObject(0, "tab")
        hist: History = History(
            [
                # 0
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0
                ),
                # 1
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 2
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=0), Result(), 0, 0, 0.0, 0.0),
                # 3
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 4
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0
                ),
                # 5
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 6
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=1), Result(), 0, 1, 0.0, 0.0),
                # 7
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 8
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0
                ),
                # 9
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 10
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=2), Result(), 0, 2, 0.0, 0.0),
                # 11
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1,2",)]), 0, 2, 0.0, 0.0),
                # 12
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 13
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 14
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 15
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=3), Result(), 0, 3, 0.0, 0.0),
                # 16
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0",)]), 0, 3, 0.0, 0.0),
                # 17
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 18
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0
                ),
                # 19
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
                # 20
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1,2,3",)]), 0, 4, 0.0, 0.0),
                # 21
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
            ]
        )

        hist[2].op.stmt([])
        hist[6].op.stmt([0])
        hist[10].op.stmt([0, 1])
        hist[15].op.stmt([0, 1, 2])
        return hist

    def get_gsingle_anomaly_hist(self) -> History:
        obj: DBObject = DBObject(0, "tab")
        hist: History = History(
            [
                # 0
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0
                ),
                # 1
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 2
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=0), Result(), 0, 0, 0.0, 0.0),
                # 3
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 4
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0
                ),
                # 5
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 6
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=1), Result(), 0, 1, 0.0, 0.0),
                # 7
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 8
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0
                ),
                # 9
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 10
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=2), Result(), 0, 2, 0.0, 0.0),
                # 11
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 12
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 13
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 14
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=3), Result(), 0, 3, 0.0, 0.0),
                # 15
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0",)]), 0, 3, 0.0, 0.0),
                # 16
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 17
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0
                ),
                # 18
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
                # 19
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1,2,3",)]), 0, 4, 0.0, 0.0),
                # 20
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
            ]
        )

        hist[2].op.stmt([])
        hist[6].op.stmt([0])
        hist[10].op.stmt([0, 1])
        hist[14].op.stmt([0, 1, 2])
        return hist

    def get_no_anomaly_hist(self) -> History:
        obj: DBObject = DBObject(0, "tab")
        hist: History = History(
            [
                # 0
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0
                ),
                # 1
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 2
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=0), Result(), 0, 0, 0.0, 0.0),
                # 3
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 4
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0
                ),
                # 5
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 6
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=1), Result(), 0, 1, 0.0, 0.0),
                # 7
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 8
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0
                ),
                # 9
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 10
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=2), Result(), 0, 2, 0.0, 0.0),
                # 11
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 12
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 13
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 14
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=3), Result(), 0, 3, 0.0, 0.0),
                # 15
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 16
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0
                ),
                # 17
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
                # 18
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1,2,3",)]), 0, 4, 0.0, 0.0),
                # 19
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
            ]
        )

        hist[2].op.stmt([])
        hist[6].op.stmt([0])
        hist[10].op.stmt([0, 1])
        hist[14].op.stmt([0, 1, 2])
        return hist

    def test_anomalies(self) -> None:
        g0_dsg: DSG = DSG(self.get_g0_anomaly_hist())
        g1c_dsg: DSG = DSG(self.get_g1c_anomaly_hist())
        g2item_dsg: DSG = DSG(self.get_g2item_anomaly_hist())
        g2_dsg: DSG = DSG(self.get_g2_anomaly_hist())
        gsingle_dsg: DSG = DSG(self.get_gsingle_anomaly_hist())
        no_anomaly_dsg: DSG = DSG(self.get_no_anomaly_hist())

        self.assertTrue(
            sum(
                map(
                    lambda x: 1,
                    filter(
                        lambda a: DSG.CyclicalAnomaly.G0 in DSG.CyclicalAnomaly.cyclical_closure(a.type()),
                        g0_dsg.find_anomalies([DSG.CyclicalAnomaly.G0]),
                    ),
                )
            )
            > 0
        )
        self.assertTrue(
            sum(
                map(
                    lambda x: 1,
                    filter(
                        lambda a: DSG.CyclicalAnomaly.G1C in DSG.CyclicalAnomaly.cyclical_closure(a.type()),
                        g0_dsg.find_anomalies([DSG.CyclicalAnomaly.G1C]),
                    ),
                )
            )
            > 0
        )
        self.assertTrue(
            sum(
                map(
                    lambda x: 1,
                    filter(
                        lambda a: DSG.CyclicalAnomaly.G1C in DSG.CyclicalAnomaly.cyclical_closure(a.type()),
                        g1c_dsg.find_anomalies([DSG.CyclicalAnomaly.G1C]),
                    ),
                )
            )
            > 0
        )
        self.assertTrue(
            sum(
                map(
                    lambda x: 1,
                    filter(
                        lambda a: DSG.CyclicalAnomaly.G2item in DSG.CyclicalAnomaly.cyclical_closure(a.type()),
                        g2item_dsg.find_anomalies([DSG.CyclicalAnomaly.G2item]),
                    ),
                )
            )
            > 0
        )
        self.assertTrue(
            sum(
                map(
                    lambda x: 1,
                    filter(
                        lambda a: DSG.CyclicalAnomaly.G2 in DSG.CyclicalAnomaly.cyclical_closure(a.type()),
                        g2item_dsg.find_anomalies([DSG.CyclicalAnomaly.G2]),
                    ),
                )
            )
            > 0
        )
        self.assertTrue(
            sum(
                map(
                    lambda x: 1,
                    filter(
                        lambda a: DSG.CyclicalAnomaly.Gsingle in DSG.CyclicalAnomaly.cyclical_closure(a.type()),
                        gsingle_dsg.find_anomalies([DSG.CyclicalAnomaly.Gsingle]),
                    ),
                )
            )
            > 0
        )
        self.assertTrue(
            sum(
                map(
                    lambda x: 1,
                    filter(
                        lambda a: DSG.CyclicalAnomaly.G2 in DSG.CyclicalAnomaly.cyclical_closure(a.type()),
                        gsingle_dsg.find_anomalies([DSG.CyclicalAnomaly.G2]),
                    ),
                )
            )
            > 0
        )
        self.assertTrue(
            sum(
                map(
                    lambda x: 1,
                    filter(
                        lambda a: DSG.CyclicalAnomaly.G2 in DSG.CyclicalAnomaly.cyclical_closure(a.type()),
                        g2_dsg.find_anomalies([DSG.CyclicalAnomaly.G2]),
                    ),
                )
            )
            > 0
        )
        self.assertEqual(sum(map(lambda x: 1, no_anomaly_dsg.find_anomalies([DSG.CyclicalAnomaly.G2]))), 0)

        for dsg in [g0_dsg, g1c_dsg, g2item_dsg, g2_dsg, gsingle_dsg, no_anomaly_dsg]:
            for anom in dsg.find_anomalies([DSG.CyclicalAnomaly.G2]):
                print(anom)
