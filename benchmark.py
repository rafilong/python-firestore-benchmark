import asyncio
import uuid
import os
from timeit import timeit
from multiprocessing.pool import ThreadPool

from google.cloud import firestore


SYNC_COLLECTION = "benchmark"
ASYNC_COLLECTION = "benchmark-async"
THREAD_COLLECTION = "benchmark-threaded"


def test(test_data):
    db = firestore.Client()

    for name, data in test_data:
        write_result = db.collection(SYNC_COLLECTION).document(name).set(data)
        _ = write_result.update_time

    for name, _ in test_data:
        timestamp = db.collection(SYNC_COLLECTION).document(name).delete()
        _ = timestamp.second


def test_threaded(test_data):
    db = firestore.Client()

    with ThreadPool(processes=10) as pool:
        for write_result in pool.starmap(
            add_data, [(db, name, data) for name, data in test_data]
        ):
            _ = write_result.update_time

        for timestamp in pool.starmap(
            delete_doc, [(db, name) for name, _ in test_data]
        ):
            _ = timestamp.second

def add_data(db, doc, data):
    return db.collection(SYNC_COLLECTION).document(doc).set(data)

def delete_doc(db, doc):
    return db.collection(SYNC_COLLECTION).document(doc).delete()


def test_async(test_data):
    asyncio.run(test_async_helper(test_data))

async def test_async_helper(test_data):
    db = firestore.AsyncClient()

    for write_result in asyncio.as_completed(
        [
            db.collection(ASYNC_COLLECTION).document(name).set(data)
            for name, data in test_data
        ]
    ):
        _ = (await write_result).update_time

    for timestamp in asyncio.as_completed(
        [
            db.collection(ASYNC_COLLECTION).document(name).delete()
            for name, _ in test_data
        ]
    ):
        _ = (await timestamp).second


def run_tests(size, num_docs, num_trials):
    data = {"data": os.urandom(size)}
    test_data = [(str(uuid.uuid1()), data) for i in range(num_docs)]

    print(f"sync, {size}, {num_docs}, {num_trials}, {timeit(lambda: test(test_data), number=num_trials)}")
    print(f"pool, {size}, {num_docs}, {num_trials}, {timeit(lambda: test_threaded(test_data), number=num_trials)}")
    print(f"async, {size}, {num_docs}, {num_trials}, {timeit(lambda: test_async(test_data), number=num_trials)}")


DOC_SIZE = 500000
NUM_DOCS = 50
NUM_TRIALS = 5

for size in [1024, 10*1024, 20*1024, 100*1024, 500*1024]:
    run_tests(size, NUM_DOCS, NUM_TRIALS)

for num in [1, 5, 10, 20, 50]:
    run_tests(DOC_SIZE, num, NUM_TRIALS)