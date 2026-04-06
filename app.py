import os
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

app = FastAPI(title="EV Population API")

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "ev_db"
COLLECTION_NAME = "vehicles"

if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is not set")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
vehicles = db[COLLECTION_NAME]


@app.get("/")
def root():
    return {"message": "EV API is running"}



@app.post("/insert-fast")
def insert_fast(record: dict):
    try:
        fast_collection = vehicles.with_options(
            write_concern=WriteConcern(w=1)
        )
        result = fast_collection.insert_one(record)
        return {"inserted_id": str(result.inserted_id)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))




@app.post("/insert-safe")
def insert_safe(record: dict):
    try:
        safe_collection = vehicles.with_options(
            write_concern=WriteConcern(w="majority")
        )
        result = safe_collection.insert_one(record)
        return {"inserted_id": str(result.inserted_id)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))



@app.get("/count-tesla-primary")
def count_tesla_primary():
    try:
        primary_collection = vehicles.with_options(
            read_preference=ReadPreference.PRIMARY
        )
        count = primary_collection.count_documents({"Make": "TESLA"})
        return {"count": count}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))



@app.get("/count-bmw-secondary")
def count_bmw_secondary():
    try:
        secondary_pref_collection = vehicles.with_options(
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )
        count = secondary_pref_collection.count_documents({"Make": "BMW"})
        return {"count": count}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
                                                                                            
