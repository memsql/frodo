##########################################################################################
# Copyright (c) MemSQL. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for license information.
##########################################################################################

"""
generator.py

Test generator

This exposes one function: `gen_history()` which returns a history, receiving a
list of DB connections
"""

from frodo.domain import DBObject, Operation, Result, Transaction
from frodo.history import History, HistoryElem
from frodo.nemesis import Nemesis
from frodo.db import DBConn

import coloredlogs  # type: ignore
import multiprocessing
import logging
import random
import string
import time
import queue
from typing import Any, Dict, Iterator, Optional, List, Tuple

# setup logger
logger: logging.Logger = logging.getLogger(__name__)
coloredlogs.install(level="INFO")
logger.setLevel(logging.INFO)

# version map for objects
obj_ver: Dict[int, int] = dict()

# TODO: make size of the transactions tunable
#
MIN_TXN_SIZE: int = 3
MAX_TXN_SIZE: int = 10


def gen_history(
    conn_list: List[DBConn],
    isolation_level: str,
    abort_rate: float = 0.15,
    write_rate: float = 0.33,
    predicate_read_rate: float = 0.10,
    n_objs: Optional[int] = 16,
    n_tables: Optional[int] = 3,
    seed: Optional[int] = None,
    transaction_limit: Optional[int] = 100,
    time_limit_sec: Optional[int] = None,
    db_name: Optional[str] = None,
    table_names: Optional[List[str]] = None,
    nemesis: Optional[Nemesis] = None,
    for_update: bool = False,
    teardown: bool = True,
) -> History:
    """
    Generates a history from the list of connections

      <isolation_level>: isolation level in which to run the txns
      <abort_rate>: ratio of txns that artificially abort
      <write_rate>: ratio of ops that write
      <predicate_read_rate>: ratio of ops that do predicate reads
      <n_objs>: number of objects to create and operate on
      <n_tables>: sets the number of tables being tested
      <seed>: seed for the PRNG
      <transaction_limit>: limits the number of transactions
      <time_limit_sec>: time limit for the test (in seconds)
      <db_name>: name of the db to use; if it doesn't exist it will be created
      <table_names>: name of the tables to use; each table needs to have a `value` column with type which supports CONCAT and an `id` int field (needs to be not null and unique).
                     Note that `text` is recommended and assumed, if the limit is exceeded the behaviour is undefined
      <nemesis>: fault injector
      <for_update>: whether to append selects with the 'for update' clause
      <teardown>: whether to delete the database after the test

    If both <transaction_limit> and <time_limit_sec> are provided, the first to run out is enforced.
    If both <n_tables> and <table_names> are provided they need to be compatible
    """

    def check_args(
        conn_list: List[DBConn],
        isolation_level: str,
        abort_rate: float,
        write_rate: float,
        predicate_read_rate: float,
        n_objs: Optional[int],
        n_tables: Optional[int],
        seed: Optional[int],
        transaction_limit: Optional[int],
        time_limit_sec: Optional[int],
        db_name: Optional[str],
        table_names: Optional[List[str]],
        nemesis: Optional[Nemesis],
        teardown: bool,
    ) -> None:
        """
        Verify integrity of the arguments
        """

        if n_tables is not None and table_names is not None:
            if n_tables != len(table_names if table_names else []):
                raise ValueError(
                    "Provided table names ({}) and number of tables ({}) are not compatible".format(
                        table_names, n_tables
                    )
                )

        if not n_tables and not table_names:
            raise ValueError("One of <table_names> and <n_tables> need to be non-None")

        if n_tables and n_tables < 1:
            raise ValueError("Need at least one table: {}".format(n_tables))

        if table_names:
            if not all(table_names):  # check that all elements are non None and not ''
                raise ValueError("A table name needs at least one character: {}".format(table_names))

        if transaction_limit is None and time_limit_sec is None:
            raise ValueError("One of <transaction_limit> and <time_limit_sec> need to be non-None")

        if n_objs is not None and n_objs < 1:
            raise ValueError("Need at least one object")

        if not (0.0 <= abort_rate <= 1.0):
            raise ValueError("The abort rate needs to be in the [0.0, 1.0] range: {}".format(abort_rate))

        if not (0.0 <= write_rate <= 1.0):
            raise ValueError("The write rate needs to be in the [0.0, 1.0] range: {}".format(write_rate))

        if not (0.0 <= predicate_read_rate <= 1.0):
            raise ValueError(
                "The predicate read rate needs to be in the [0.0, 1.0] range: {}".format(predicate_read_rate)
            )

        if not (0.0 <= write_rate + predicate_read_rate <= 1.0):
            raise ValueError(
                "The write rate + the predicate read rate needs to be in the [0.0, 1.0] range: {}".format(
                    write_rate + predicate_read_rate
                )
            )

        if len(conn_list) < 1:
            raise ValueError("Need at least one valid connection")

    def setup(
        conn_list: List[DBConn],
        n_objs: Optional[int],
        seed: Optional[int],
        n_tables: Optional[int],
        db_name: Optional[str],
        table_names: Optional[List[str]],
    ) -> Tuple[int, str, List[str]]:
        """
        Sets up the environment for the test
        Returns n_objs, db_name, table_names
        """

        if not seed:
            seed = int(time.time() * 1e6)

        random.seed(seed)

        if not db_name:
            db_name = "db_" + "".join(random.choices(string.ascii_lowercase, k=32))

        if not table_names:
            table_names = [
                "table_" + "".join(random.choices(string.ascii_lowercase, k=32))
                for _ in range(n_tables if n_tables else 0)
            ]

        if not n_objs:
            n_objs = random.randint(min(len(table_names), 4), 64)

        for obj_id in range(n_objs):
            obj_ver[obj_id] = 0

        conn_list[0].execute("set global lock_wait_timeout=7")
        conn_list[0].execute("create database if not exists {}".format(db_name))
        for conn in conn_list:
            conn.execute("use {}".format(db_name))

        for table in table_names:
            conn_list[0].execute(
                "create table if not exists {}(id int not null, value text, primary key (id))".format(table)
            )

        logger.info(
            "[+] starting history generation (seed: {}, n_objs: {} DB: {}, tables: {})".format(
                seed, n_objs, db_name, ", ".join(table_names)
            )
        )

        return n_objs, db_name, table_names

    def partition_ids(n_objs: int, table_names: List[str]) -> List[DBObject]:
        """
        (Randomly) partition the object ids between the different tables
        """

        rev_map: Dict[str, int] = {name: 1 for name in table_names}
        while sum(rev_map.values()) < n_objs:
            rev_map[random.choice(table_names)] += 1

        obj_list: List[DBObject] = list()
        for table, n in rev_map.items():
            for _ in range(n):
                obj_list.append(DBObject(len(obj_list), table))

        return obj_list

    def gen_transaction(
        txn_id: int,
        obj_list: List[DBObject],
        table_names: List[str],
        isolation_level: str,
        min_size: int,
        max_size: int,
        abort_rate: float,
        write_rate: float,
        predicate_read_rate: float,
        for_update: bool,
    ) -> Transaction:
        """
        Generates a list of SQL statemtents for a transaction

         <obj_list>: list of objects
         <isolation_level>: isolation level for the transaction
         <min_size>: minimum size of the transaction (in number of operations)
         <max_size>: maximum size of the transaction (in number of operations)
         <abort_rate>: abort rate (domain = [0.0, 1.0])
         <write_rate>: write rate (domain = [0.0, 1.0])
         <predicate_read_rate>: predicate read rate (domain = [0.0, 1.0])
        """

        def gen_op(
            obj_list: List[DBObject],
            table_names: List[str],
            write_rate: float,
            predicate_read_rate: float,
            for_update: bool,
            chosen_len: int,
        ) -> List[Operation]:
            """
            Generate a single operation

            By fixing a chosen len across a transaction, it makes it more likely for there to be conflicts
            """
            rnd: float = random.random()
            if rnd < write_rate:
                obj: DBObject = random.choice(obj_list)
                # This creates an object if it doesn't exist
                # Note that we cannot rely on the object being created if obj_ver[obj_id] > 0.
                # This is because obj_ver denotes the order in which the statements are *generated* not executed
                # It is incremental to ensure *uniqueness*, not *order*
                # For instance, "1,2,0,4,3" is a valid value for an object, but "1,2,1,4,3" is not
                #
                obj_ver[obj.id] += 1
                return [
                    Operation(Operation.Type.READ, obj=obj, for_update=for_update),
                    Operation(Operation.Type.WRITE, obj=obj, value=obj_ver[obj.id]),
                ]
            elif write_rate <= rnd < write_rate + predicate_read_rate:
                return [
                    Operation(
                        Operation.Type.PREDICATE_READ,
                        tables=table_names,
                        value=chosen_len,
                        for_update=for_update,
                    )
                ]
            else:
                return [
                    Operation(
                        Operation.Type.READ,
                        obj=random.choice(obj_list),
                        for_update=for_update,
                    )
                ]

        size: int = random.randint(min_size, max_size)
        # How many times, on average, each txn will write to an object
        #
        AVG_WRITE_PER_OBJECT_PER_TXN: float = (write_rate * 0.5 * (min_size + max_size)) / len(obj_list)

        # This is a bit hacky, but multiplying AVG_WRITE_PER_OBJECT_PER_TXN by
        # the transaction id gives the approximate average size of each object at this point
        # since it approximates sum([AVG_WRITE_PER_OBJECT_PER_TXN] * N_TXN_UNTIL_THIS_POINT)
        #
        AVG_OBJECT_SIZE: int = int(AVG_WRITE_PER_OBJECT_PER_TXN * txn_id)

        ops: List[Operation] = [
            Operation(Operation.Type.SET_ISOLATION, isolation_level=isolation_level),
            Operation(Operation.Type.BEGIN),
        ]

        for _ in range(size):
            # Using this hacky math makes the predicate reads more likely to return
            # interesting queries
            #
            # We intentionally skew in favour of returning less values, which
            # makes this more prone to returning less values, and consequently
            # generating more anti-dependencies
            #
            for op in gen_op(
                obj_list,
                table_names,
                write_rate,
                predicate_read_rate,
                for_update,
                random.randint(int(AVG_OBJECT_SIZE * 0.85), int(AVG_OBJECT_SIZE * 1.35)),
            ):
                ops.append(op)

        if random.random() < abort_rate:
            ops.append(Operation(Operation.Type.ROLLBACK))
        else:
            ops.append(Operation(Operation.Type.COMMIT))

        return Transaction(txn_id, ops)

    def gen_init_txn(txn_id: int, obj_list: List[DBObject]) -> Transaction:
        """
        Generate initial transaction to set initial value for objects
        This transaction should happend without concurrency
        """
        ops: List[Operation] = [
            Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"),
            Operation(Operation.Type.BEGIN),
        ]
        for obj in obj_list:
            ops.append(Operation(Operation.Type.WRITE, obj=obj, value=obj_ver[obj.id]))

        ops.append(Operation(Operation.Type.COMMIT))
        return Transaction(txn_id, ops)

    def gen_final_txn(txn_id: int, obj_list: List[DBObject]) -> Transaction:
        """
        Generate final transaction to reads all objects
        This transaction should happend without concurrency
        """
        ops: List[Operation] = [
            Operation(Operation.Type.SET_ISOLATION, isolation_level="serializable"),
            Operation(Operation.Type.BEGIN),
        ]
        for obj in obj_list:
            ops.append(Operation(Operation.Type.READ, obj=obj))

        ops.append(Operation(Operation.Type.COMMIT))
        return Transaction(txn_id, ops)

    def do_teardown(conn: DBConn, db_name: str) -> None:
        conn.execute("drop database {}".format(db_name))

    def process_txn(obj_list: List[DBObject], conn: DBConn, conn_id: int, txn: Transaction) -> Iterator[HistoryElem]:
        """
        Process a transaction as an iterator
        """
        object_versions: Dict[int, List[int]] = dict()
        try:
            for op in txn.ops:
                invoc: float = time.time()
                ret: Optional[List[Tuple[Any, ...]]]
                if op.type == Operation.Type.WRITE:
                    prev_version = object_versions[op.obj.id] if op.obj.id in object_versions else list()
                    ret = conn.execute(op.stmt(prev_version))
                else:
                    ret = conn.execute(op.stmt())

                if op.type == Operation.Type.PREDICATE_READ:
                    resp: float = time.time()
                    yield HistoryElem(op, Result(value=ret), conn_id, txn.id, invoc, resp)
                    for tup in ret:
                        object_versions[tup[0]] = [int(v) for v in tup[1].strip().split(",")]
                        yield HistoryElem(
                            Operation(Operation.Type.READ, obj=obj_list[tup[0]]),
                            Result(value=[(tup[1],)]),
                            conn_id,
                            txn.id,
                            invoc,
                            resp,
                        )
                else:
                    res: Result = Result(value=ret) if ret is not None else Result()
                    if res.is_ok() and res.is_value():
                        object_versions[op.obj.id] = res.value()
                    yield HistoryElem(op, res, conn_id, txn.id, invoc)
        except Exception as e:
            conn.process_exception(e)
            yield HistoryElem(op, Result(exception=e), conn_id, txn.id, invoc)

    def connection_work(
        conn: DBConn,
        conn_id: int,
        obj_list: List[DBObject],
        in_queue: multiprocessing.Queue,  # type: ignore
        out_queue: multiprocessing.Queue,  # type: ignore
        time_limit_sec: Optional[int],
        done_ctr: multiprocessing.Value,
    ) -> None:
        """
        Main function of the connection "thread"
        Pops values from the input queue and places hitory elements in the outgoing queue
        """

        time.sleep(3)
        begin_ts = time.time()
        logger.info("[{}]: started thread".format(conn_id))

        empty_cnt: int = 0

        while time_limit_sec is None or time.time() < begin_ts < time_limit_sec:
            try:
                txn = in_queue.get(timeout=2)
            except queue.Empty:
                empty_cnt += 1
                if empty_cnt > 3:
                    break
                else:
                    continue

            logger.info("[{}]: poped transaction {} (size = {})".format(conn_id, txn.id, in_queue.qsize()))
            for hist_elem in process_txn(obj_list, conn, conn_id, txn):
                out_queue.put(hist_elem)
            logger.info("[{}]: finished transaction {}".format(conn_id, txn.id))

        logger.info("[{}]: closing queue (size = {})".format(conn_id, out_queue.qsize()))
        out_queue.close()
        with done_ctr.get_lock():
            done_ctr.value += 1
        logger.info("[{}]: finished thread (done ctr at {})".format(conn_id, done_ctr.value))
        time.sleep(3)

    def drive_nemesis(nemesis: Nemesis, done_ctr: multiprocessing.Value, n_conns: int) -> None:
        logger.info("[nemesis]: started")
        while done_ctr.value < n_conns:
            nemesis.inject()

        logger.info("[nemesis]: begin system healing")
        nemesis.heal()
        logger.info("[nemesis]: system healed")

    # setup phase
    check_args(
        conn_list,
        isolation_level,
        abort_rate,
        write_rate,
        predicate_read_rate,
        n_objs,
        n_tables,
        seed,
        transaction_limit,
        time_limit_sec,
        db_name,
        table_names,
        nemesis,
        teardown,
    )
    n_objs, db_name, table_names = setup(conn_list, n_objs, seed, n_tables, db_name, table_names)
    obj_list: List[DBObject] = partition_ids(n_objs, table_names)

    # init values for objects
    history: List[HistoryElem] = list()
    txn_id: int = 0
    init_txn: Transaction = gen_init_txn(txn_id, obj_list)
    txn_id += 1

    logger.info("[{}]: started initial transaction {}".format(0, init_txn.id))
    for hist_elem in process_txn(obj_list, conn_list[0], 0, init_txn):
        history.append(hist_elem)
    logger.info("[{}]: finished transaction {}".format(0, init_txn.id))

    txn_queue: multiprocessing.Queue[Transaction] = multiprocessing.Queue()
    hist_queue: multiprocessing.Queue[HistoryElem] = multiprocessing.Queue()
    done_ctr: multiprocessing.Value = multiprocessing.Value("i", 0)

    for _ in range(transaction_limit if transaction_limit else 10 * len(conn_list)):
        # Here, if there is no limit we add 10 times the number of connections to make sure
        # they don't starve before the time limit ends
        #
        txn_queue.put(
            gen_transaction(
                txn_id,
                obj_list,
                table_names,
                isolation_level,
                MIN_TXN_SIZE,
                MAX_TXN_SIZE,
                abort_rate,
                write_rate,
                predicate_read_rate,
                for_update,
            )
        )
        txn_id += 1

    procs: List[multiprocessing.Process] = [
        multiprocessing.Process(
            target=connection_work,
            args=(
                conn,
                conn_id,
                obj_list,
                txn_queue,
                hist_queue,
                time_limit_sec,
                done_ctr,
            ),
        )
        for conn_id, conn in enumerate(conn_list)
    ]
    if nemesis is not None:
        procs += [multiprocessing.Process(target=drive_nemesis, args=(nemesis, done_ctr, len(conn_list)))]

    for p in procs:
        p.start()

    logger.info("started {} procs".format(len(procs)))

    while done_ctr.value < len(conn_list):
        if transaction_limit is None:
            # We ensure that the queue is always confortably full
            #
            qsize: int = txn_queue.qsize()
            if qsize < 10 * len(conn_list):
                for _ in range(10 * len(conn_list)):
                    txn_queue.put(
                        gen_transaction(
                            txn_id,
                            obj_list,
                            table_names,
                            isolation_level,
                            MIN_TXN_SIZE,
                            MAX_TXN_SIZE,
                            abort_rate,
                            write_rate,
                            predicate_read_rate,
                            for_update,
                        )
                    )
                    txn_id += 1

    txn_queue.close()

    while not hist_queue.empty():
        history.append(hist_queue.get())

    hist_queue.close()

    for idx, p in enumerate(procs):
        p.join()
        logger.info("joined proc {}".format(idx))

    logger.info("joined all {} procs".format(len(procs)))

    final_txn: Transaction = gen_final_txn(txn_id, obj_list)
    logger.info("[{}]: started final transaction {}".format(0, final_txn.id))
    for hist_elem in process_txn(obj_list, conn_list[0], 0, final_txn):
        history.append(hist_elem)
    logger.info("[{}]: finished transaction {}".format(0, final_txn.id))

    if teardown:
        do_teardown(conn_list[0], db_name)

    return History(history)
