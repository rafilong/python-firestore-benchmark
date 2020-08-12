import asyncio
import uuid
from timeit import timeit
from multiprocessing.pool import ThreadPool

from google.cloud import firestore


SYNC_COLLECTION = "benchmark"
ASYNC_COLLECTION = "benchmark-async"
THREAD_COLLECTION = "benchmark-threaded"

NUMBER_DOCS = 50
NUMBER_TRIALS = 5


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


# Testing

with open("upload", "rb") as f:
    data = {"file": f.read()}
    test_data = [(str(uuid.uuid1()), data) for i in range(NUMBER_DOCS)]

print(f"Sync time {timeit(lambda: test(test_data), number=NUMBER_TRIALS)}")
print(f"Threaded time {timeit(lambda: test_threaded(test_data), number=NUMBER_TRIALS)}")
print(f"Async time {timeit(lambda: test_async(test_data), number=NUMBER_TRIALS)}")
