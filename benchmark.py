import asyncio
import uuid
import threading
from timeit import timeit

from google.cloud import firestore


SYNC_COLLECTION = "benchmark"
ASYNC_COLLECTION = "benchmark-async"
THREAD_COLLECTION = "benchmark-threaded"

NUMBER_DOCS = 100
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

    threads = {}
    results = {}
    for name, data in test_data:
        threads[name] = threading.Thread(target=add_data, args=(db,name,data,results))
        threads[name].start()

    for name, _ in test_data:
        threads[name].join()
        _ = results[name].update_time

    threads = {}
    timestamps = {}
    for name, _ in test_data:
        threads[name] = threading.Thread(target=delete_doc, args=(db,name,timestamps))
        threads[name].start()

    for name, _ in test_data:
        threads[name].join()
        _ = timestamps[name].second

def add_data(db, doc, data, results):
    results[doc] = db.collection(SYNC_COLLECTION).document(doc).set(data)

def delete_doc(db, doc, timestamps):
    timestamps[doc] = db.collection(SYNC_COLLECTION).document(doc).delete()
    


def test_async(test_data):
    asyncio.run(test_async_helper(test_data))

async def test_async_helper(test_data):
    db = firestore.AsyncClient()

    for write_result in asyncio.as_completed([
        db.collection(ASYNC_COLLECTION).document(name).set(data)
        for name, data in test_data
    ]):
        _ = (await write_result).update_time

    for timestamp in asyncio.as_completed([
        db.collection(ASYNC_COLLECTION).document(name).delete()
        for name, _ in test_data
    ]):
        _ = (await timestamp).second


# Testing

with open("upload", "rb") as f:
    data = {"file": f.read()}
    test_data = [(str(uuid.uuid1()), data) for i in range(NUMBER_DOCS)]

print(f"Sync time {timeit(lambda: test(test_data), number=NUMBER_TRIALS)}")
print(f"Threaded time {timeit(lambda: test_threaded(test_data), number=NUMBER_TRIALS)}")
print(f"Async time {timeit(lambda: test_async(test_data), number=NUMBER_TRIALS)}")
