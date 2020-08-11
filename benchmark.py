import asyncio
import uuid
from timeit import timeit

from google.cloud import firestore


SYNC_COLLECTION = "benchmark"
ASYNC_COLLECTION = "benchmark-async"

NUMBER_DOCS = 100
NUMBER_TRIALS = 10

# Sync Helpers

def add_data(db, doc, data):
    doc_ref = db.collection(SYNC_COLLECTION).document(doc)
    return doc_ref.set(data)

def delete_doc(db, doc):
    return db.collection(SYNC_COLLECTION).document(doc).delete()


# Async Helpers

async def async_add_data(db, doc, data):
    doc_ref = db.collection(ASYNC_COLLECTION).document(doc)
    return await doc_ref.set(data)

async def async_delete_doc(db, doc):
    return await db.collection(ASYNC_COLLECTION).document(doc).delete()


# Testing Helpers

def test(test_data):
    db = firestore.Client()
    write_results = []

    for name, data in test_data:
        write_results.append(add_data(db, name, data))

    for result in write_results:
        _ = result.update_time

    delete_timestamps = []
    for name, _ in test_data:
        delete_timestamps.append(delete_doc(db, name))

    for timestamp in delete_timestamps:
        _ = timestamp.second

async def test_async(test_data):
    db = firestore.AsyncClient()
    write_results = []

    for name, data in test_data:
        write_results.append(await async_add_data(db, name, data))

    for result in write_results:
        _ = result.update_time
    
    delete_timestamps = []
    for name, _ in test_data:
        delete_timestamps.append(await async_delete_doc(db, name))

    for timestamp in delete_timestamps:
        _ = timestamp.second


# Testing

with open("upload", "rb") as f:
    data = {"file": f.read()}
    test_data = [(str(uuid.uuid1()), data) for i in range(NUMBER_DOCS)]

print(f"Sync time {timeit(lambda: test(test_data), number=NUMBER_TRIALS)}")
print(f"Async time {timeit(lambda: asyncio.run(test_async(test_data)), number=NUMBER_TRIALS)}")