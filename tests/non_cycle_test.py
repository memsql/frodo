##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
non_cycle_test.py

non_cycle.py unit test
"""

from frodo.domain import DBObject, Operation, Result
from frodo.history import History, HistoryElem
from frodo.non_cycle import find_g1a, find_g1b

import unittest


class TestNonCyclicalAnomalies(unittest.TestCase):
    def get_g1a_anomaly_hist(self) -> History:
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
                HistoryElem(
                    Operation(Operation.Type.ROLLBACK, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0
                ),
                # 8
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0
                ),
                # 9
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 10
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1",)]), 0, 2, 0.0, 0.0),
                # 11
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 12
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 13
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 14
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0",)]), 0, 3, 0.0, 0.0),
                # 15
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 8, 0.0, 0.0),
            ]
        )
        hist[2].op.stmt([])
        hist[6].op.stmt([0])
        return hist

    def get_g1b_anomaly_hist(self) -> History:
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
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj, value=2), Result(), 0, 1, 0.0, 0.0),
                # 8
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 9
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0
                ),
                # 10
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 11
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0,1",)]), 0, 2, 0.0, 0.0),
                # 12
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 13
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 14
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 15
                HistoryElem(Operation(Operation.Type.READ, obj=obj), Result(value=[("0",)]), 0, 3, 0.0, 0.0),
                # 16
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 8, 0.0, 0.0),
            ]
        )
        hist[2].op.stmt([])
        hist[6].op.stmt([0])
        hist[7].op.stmt([0, 1])
        return hist

    def test_g1a_anomaly(self) -> None:
        wrong_hist: History = self.get_g1a_anomaly_hist()
        correct_hist: History = self.get_g1b_anomaly_hist()
        self.assertEqual(len(find_g1a(wrong_hist)), 1)
        self.assertEqual(len(find_g1a(correct_hist)), 0)
        print(find_g1a(wrong_hist)[0])

    def test_g1b_anomaly(self) -> None:
        wrong_hist: History = self.get_g1b_anomaly_hist()
        correct_hist: History = self.get_g1a_anomaly_hist()
        self.assertEqual(len(find_g1b(wrong_hist)), 1)
        self.assertEqual(len(find_g1b(correct_hist)), 0)
        print(find_g1b(wrong_hist)[0])
