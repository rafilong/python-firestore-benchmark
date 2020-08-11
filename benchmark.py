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
    doc_ref.set(data)

def check_data(db, doc, data):
    snapshot = db.collection(SYNC_COLLECTION).document(doc).get()
    assert data == snapshot.get()

def delete_doc(db, doc):
    db.collection(SYNC_COLLECTION).document(doc).delete()


# Async Helpers

async def async_add_data(db, doc, data):
    doc_ref = db.collection(ASYNC_COLLECTION).document(doc)
    await doc_ref.set(data)

async def async_check_data(db, doc, data):
    snapshot = await db.collection(ASYNC_COLLECTION).document(doc).get()
    assert data == snapshot.get()

async def async_delete_doc(db, doc):
    await db.collection(ASYNC_COLLECTION).document(doc).delete()


# Testing Helpers

def test(test_data):
    db = firestore.Client()

    for name, data in test_data:
        add_data(db, name, data)

    for name, _ in test_data:
        delete_doc(db, name)

async def test_async(test_data):
    db = firestore.AsyncClient()

    for name, data in test_data:
        await async_add_data(db, name, data)
    
    for name, _ in test_data:
        await async_delete_doc(db, name)


# Testing

data = {"first": "Alan", "middle": "Mathison", "last": "Turing", "born": 1912}
test_data = [(str(uuid.uuid1()), data) for i in range(NUMBER_DOCS)]

print(f"Sync time {timeit(lambda: test(test_data), number=NUMBER_TRIALS)}")
print(f"Async time {timeit(lambda: asyncio.run(test_async(test_data)), number=NUMBER_TRIALS)}")