##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
history_test.py

history.py unit tests
"""

from frodo.domain import DBObject, Operation, Result
from frodo.history import History, HistoryElem

from typing import List, Set

import unittest


class TestHistory(unittest.TestCase):
    def get_history(self) -> History:
        obj_list = [
            DBObject(0, "tab"),
            DBObject(1, "tab"),
            DBObject(2, "tab"),
        ]
        self._obj_list = obj_list
        hist: History = History(
            [
                # 0
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0
                ),
                # 1
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 2
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[0], value=0), Result(), 0, 0, 0.0, 0.0),
                # 3
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[1], value=0), Result(), 0, 0, 0.0, 0.0),
                # 4
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[2], value=0), Result(), 0, 0, 0.0, 0.0),
                # 5
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 0, 0.0, 0.0),
                # 6
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0
                ),
                # 7
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 8
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[0], value=1), Result(), 0, 1, 0.0, 0.0),
                # 9
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[1], value=3), Result(), 0, 1, 0.0, 0.0),
                # 9
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 1, 0.0, 0.0),
                # 11
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0
                ),
                # 12
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 13
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[1], value=1), Result(), 0, 2, 0.0, 0.0),
                # 14
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[1], value=2), Result(), 0, 2, 0.0, 0.0),
                # 15
                HistoryElem(Operation(Operation.Type.READ, obj=obj_list[0]), Result(value=[("0,1",)]), 0, 2, 0.0, 0.0),
                # 16
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 2, 0.0, 0.0),
                # 17
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 18
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0),
                # 19
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[2], value=1), Result(), 0, 3, 0.0, 0.0),
                # 20
                HistoryElem(
                    Operation(Operation.Type.ROLLBACK, isolation_level="serializable"), Result(), 0, 3, 0.0, 0.0
                ),
                # 21
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0
                ),
                # 22
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 4, 0.0, 0.0),
                # 23
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[2], value=2), Result(), 0, 4, 0.0, 0.0),
                # 24
                HistoryElem(
                    Operation(Operation.Type.COMMIT, isolation_level="serializable"),
                    Result(exception=TimeoutError("Connection Reset")),
                    0,
                    4,
                    0.0,
                    0.0,
                ),
                # 25
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 5, 0.0, 0.0
                ),
                # 26
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 5, 0.0, 0.0),
                # 27
                HistoryElem(Operation(Operation.Type.WRITE, obj=obj_list[2], value=3), Result(), 0, 5, 0.0, 0.0),
                # 28
                HistoryElem(
                    Operation(Operation.Type.ROLLBACK, isolation_level="serializable"),
                    Result(exception=TimeoutError("Connection Reset")),
                    0,
                    5,
                    0.0,
                    0.0,
                ),
                # 29
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 6, 0.0, 0.0
                ),
                # 30
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 6, 0.0, 0.0),
                # 31
                HistoryElem(
                    Operation(Operation.Type.WRITE, obj=obj_list[2], value=4),
                    Result(exception=TimeoutError("Connection Reset")),
                    0,
                    6,
                    0.0,
                    0.0,
                ),
                # 32
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 7, 0.0, 0.0
                ),
                # 33
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 7, 0.0, 0.0),
                # 34
                HistoryElem(
                    Operation(Operation.Type.READ, obj=obj_list[2]),
                    Result(exception=TimeoutError("Connection Reset")),
                    0,
                    7,
                    0.0,
                    0.0,
                ),
                # 35
                HistoryElem(
                    Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"), Result(), 0, 8, 0.0, 0.0
                ),
                # 36
                HistoryElem(Operation(Operation.Type.BEGIN, isolation_level="serializable"), Result(), 0, 8, 0.0, 0.0),
                # 37
                HistoryElem(Operation(Operation.Type.READ, obj=obj_list[0]), Result(value=[("0,1",)]), 0, 8, 0.0, 0.0),
                # 38
                HistoryElem(
                    Operation(Operation.Type.READ, obj=obj_list[1]), Result(value=[("0,1,2",)]), 0, 8, 0.0, 0.0
                ),
                # 39
                HistoryElem(Operation(Operation.Type.READ, obj=obj_list[2]), Result(value=[("0",)]), 0, 8, 0.0, 0.0),
                # 40
                HistoryElem(Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 8, 0.0, 0.0),
            ]
        )

        hist[2].op.stmt([])
        hist[3].op.stmt([])
        hist[4].op.stmt([])
        hist[8].op.stmt([0])
        hist[9].op.stmt([0])
        hist[13].op.stmt([0])
        hist[14].op.stmt([0, 1])

        return hist

    def test_init(self) -> None:
        h: History

        # empty history
        #
        with self.assertRaises(ValueError):
            h = History([])

        # transaction order
        #
        with self.assertRaises(ValueError):
            h = History(
                [
                    HistoryElem(
                        Operation(Operation.Type.COMMIT, isolation_level="serializable"), Result(), 0, 8, 0.0, 0.0
                    ),
                    HistoryElem(
                        Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"),
                        Result(),
                        0,
                        0,
                        0.0,
                        0.0,
                    ),
                ]
            )

        h = self.get_history()

    def test_txn_state(self) -> None:
        hist: History = self.get_history()

        with self.assertRaises(ValueError):
            hist.txn_state(-1)
        self.assertEqual(hist.txn_state(0), History.TransactionState.COMMITTED)
        self.assertEqual(hist.txn_state(1), History.TransactionState.COMMITTED)
        self.assertEqual(hist.txn_state(2), History.TransactionState.COMMITTED)
        self.assertEqual(hist.txn_state(3), History.TransactionState.ABORTED)
        self.assertEqual(hist.txn_state(4), History.TransactionState.INDETERMINATE)
        self.assertEqual(hist.txn_state(5), History.TransactionState.INDETERMINATE)
        self.assertEqual(hist.txn_state(6), History.TransactionState.INDETERMINATE)
        self.assertEqual(hist.txn_state(7), History.TransactionState.INDETERMINATE)
        self.assertEqual(hist.txn_state(8), History.TransactionState.COMMITTED)
        with self.assertRaises(ValueError):
            hist.txn_state(9)

    def test_final_version(self) -> None:
        hist: History = self.get_history()
        self.assertEqual(hist.final_version(0), [0, 1])
        self.assertEqual(hist.final_version(1), [0, 1, 2])
        self.assertEqual(hist.final_version(2), [0])
        with self.assertRaises(KeyError):
            hist.final_version(3)

    def test_committed_versions(self) -> None:
        hist: History = self.get_history()
        self.assertEqual(set(tuple(a) for a in hist.committed_versions(0)), set(a for a in ((0,), (0, 1))))
        self.assertEqual(
            set(tuple(a) for a in hist.committed_versions(1)), set(a for a in ((0,), (0, 3), (0, 1, 2)))
        )  # version 0,3 was lost!
        self.assertEqual(set(tuple(a) for a in hist.committed_versions(2)), set(a for a in ((0,),)))
        with self.assertRaises(KeyError):
            hist.final_version(3)

    def test_who_read(self) -> None:
        def convert(l: List[HistoryElem]) -> Set[int]:
            """convert list of history elems to list of txns"""
            return set(map(lambda x: x.txn_id, l))

        hist: History = self.get_history()
        self.assertEqual(convert(hist.who_read(0, 0)), set())
        self.assertEqual(convert(hist.who_read(0, 1)), {2, 8})

        self.assertEqual(convert(hist.who_read(1, 0)), set())
        self.assertEqual(convert(hist.who_read(1, 1)), set())
        self.assertEqual(convert(hist.who_read(1, 2)), {8})

        self.assertEqual(convert(hist.who_read(2, 0)), {8})
        self.assertEqual(convert(hist.who_read(2, 1)), set())
        self.assertEqual(convert(hist.who_read(2, 2)), set())
        self.assertEqual(convert(hist.who_read(2, 3)), set())
        self.assertEqual(convert(hist.who_read(2, 4)), set())

        with self.assertRaises(KeyError):
            hist.who_read(3, 0)

    def test_who_wrote(self) -> None:
        hist: History = self.get_history()
        self.assertEqual(hist.who_wrote(0, 0).txn_id, 0)
        self.assertEqual(hist.who_wrote(0, 1).txn_id, 1)

        self.assertEqual(hist.who_wrote(1, 0).txn_id, 0)
        self.assertEqual(hist.who_wrote(1, 2).txn_id, 2)

        self.assertEqual(hist.who_wrote(2, 0).txn_id, 0)
        self.assertEqual(hist.who_wrote(2, 1).txn_id, 3)  # rollback
        self.assertEqual(hist.who_wrote(2, 2).txn_id, 4)  # commit fail
        self.assertEqual(hist.who_wrote(2, 3).txn_id, 5)  # rollback fail
        self.assertEqual(hist.who_wrote(2, 4).txn_id, 6)  # write fails

        with self.assertRaises(KeyError):
            hist.who_wrote(3, 0)

        with self.assertRaises(KeyError):
            hist.who_wrote(1, 4)

    def test_trace(self) -> None:
        def test_obj_trace(
            self: TestHistory, computedTrace: List[HistoryElem], expectedTrace: List[HistoryElem]
        ) -> None:
            self.assertEqual(len(computedTrace), len(expectedTrace))
            for a, b in zip(computedTrace, expectedTrace):
                self.assertEqual(a, b)

        hist: History = self.get_history()

        test_obj_trace(
            self,
            hist.trace(0),
            [
                hist[2],
                hist[8],
            ],
        )
        test_obj_trace(
            self,
            hist.trace(1),
            [
                hist[3],
                hist[9],
                hist[13],
                hist[14],
            ],
        )
        test_obj_trace(
            self,
            hist.trace(2),
            [
                hist[4],
            ],
        )

    def test_reads_from(self) -> None:
        def test_reads_from_obj(
            self: TestHistory, readHist: List[HistoryElem], expectedHist: List[HistoryElem]
        ) -> None:

            self.assertEqual(len(readHist), len(expectedHist))
            for elem in readHist:
                self.assertTrue(elem in expectedHist)

        hist: History = self.get_history()
        test_reads_from_obj(self, hist.reads_from(0), [hist[15], hist[37]])
        test_reads_from_obj(self, hist.reads_from(1), [hist[38]])
        test_reads_from_obj(self, hist.reads_from(2), [hist[39]])

    def test_is_aborted_version(self) -> None:
        hist: History = self.get_history()
        self.assertFalse(hist.is_aborted_ver(0, 0))
        self.assertFalse(hist.is_aborted_ver(0, 1))

        self.assertFalse(hist.is_aborted_ver(1, 0))
        self.assertFalse(hist.is_aborted_ver(1, 2))

        self.assertFalse(hist.is_aborted_ver(2, 0))
        self.assertTrue(hist.is_aborted_ver(2, 1))

    def test_is_installed_version(self) -> None:
        hist: History = self.get_history()
        self.assertTrue(hist.is_installed_ver(0, 0))
        self.assertTrue(hist.is_installed_ver(0, 1))

        self.assertTrue(hist.is_installed_ver(1, 0))
        self.assertFalse(hist.is_installed_ver(1, 1))
        self.assertTrue(hist.is_installed_ver(1, 2))

        self.assertTrue(hist.is_installed_ver(2, 0))
        self.assertFalse(hist.is_installed_ver(2, 1))

    def test_is_intermediate_ver(self) -> None:
        hist: History = self.get_history()
        self.assertFalse(hist.is_intermediate_ver(0, 0))
        self.assertFalse(hist.is_intermediate_ver(0, 1))

        self.assertFalse(hist.is_intermediate_ver(1, 0))
        self.assertTrue(hist.is_intermediate_ver(1, 1))
        self.assertFalse(hist.is_intermediate_ver(1, 2))

        self.assertFalse(hist.is_intermediate_ver(2, 0))
        self.assertFalse(hist.is_intermediate_ver(2, 1))
