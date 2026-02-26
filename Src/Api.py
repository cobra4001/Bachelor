from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from neo4j import AsyncGraphDatabase
from pydantic import BaseModel
from typing import List

# -------------------------
# CONFIG
# -------------------------
URI = "bolt://10.196.241.255:7687"
USER = "neo4j"
PASSWORD = "mRTzP1@h"

driver = None  # settes ved startup

# -------------------------
# LIFESPAN
# -------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global driver
    print("🚀 Starter API og kobler til Neo4j...")

    driver = AsyncGraphDatabase.driver(
        URI,
        auth=(USER, PASSWORD)
    )

    # Test connection
    try:
        await driver.verify_connectivity()
        print("✅ Koblet til Neo4j!")
    except Exception as e:
        print("❌ Klarte ikke koble til Neo4j:", e)

    yield

    print("🛑 Lukker Neo4j-tilkobling...")
    await driver.close()

app = FastAPI(lifespan=lifespan)

# -------------------------
# RESPONSE MODELS
# -------------------------
class KnownPersonResponse(BaseModel):
    ident: str
    is_known_person: bool

class PersonDetailResponse(BaseModel):
    ident: str
    fullName: str
    birthDate: str
    familyName: str
    givenName: str
    personType: str

class RegisteredEntityResponse(BaseModel):
    ident: str
    is_registered_entity: bool    
# -------------------------
# DB SESSION
# -------------------------
async def get_session():
    async with driver.session() as session:
        yield session

# -------------------------
# CYPHER / DB FUNCTIONS
# -------------------------
async def check_known_person(session, ident: str):
    query = """
    MATCH (p:Person {ident: $ident})
    RETURN p.ident AS ident,
           p IS NOT NULL AS is_known_person
    """
    result = await session.run(query, ident=ident)
    record = await result.single()

    if not record:
        return None

    return {
        "ident": record["ident"],
        "is_known_person": record["is_known_person"]
    }


async def get_person_details(session, ident: str):
    query = """
    MATCH (p:Person {ident: $ident})
    RETURN p.ident AS ident,
           p.fullName AS fullName,
           p.birthDate AS birthDate,
           p.familyName AS familyName,
           p.givenName AS givenName,
           p.personType AS personType
    """
    result = await session.run(query, ident=ident)
    record = await result.single()
    if not record:
        return None
    return dict(record)

async def check_registered_entity(session, ident: str):
    query = """
    MATCH (e:Entity {ident: $ident})
    RETURN e.ident AS ident,
           e IS NOT NULL AS is_registered_entity
    """
    result = await session.run(query, ident=ident)
    record = await result.single()

    if not record:
        return None

    return {
        "ident": record["ident"],
        "is_registered_entity": record["is_registered_entity"]
    }


# -------------------------
# ROUTES / ENDPOINTS
# -------------------------

# 🔍 Check if statement is known
@app.get("/Person/{ident}/known", response_model=KnownPersonResponse)
async def is_known_person(ident: str, session=Depends(get_session)):
    data = await check_known_person(session, ident)
    if not data:
        raise HTTPException(status_code=404, detail="Person not found")
    return data


# 👤 Get person details using ident
@app.get("/Person/{ident}/details", response_model=PersonDetailResponse)
async def person_details_endpoint(ident: str, session=Depends(get_session)):
    data = await get_person_details(session, ident)
    if not data:
        raise HTTPException(status_code=404, detail="Person not found")
    return data

# check if Entity is registered
@app.get("/Entity/{ident}/registered", response_model=RegisteredEntityResponse)
async def is_registered_entity(ident: str, session=Depends(get_session)):
    data = await check_registered_entity(session, ident)
    if not data:
        raise HTTPException(status_code=404, detail="Entity not found")
    return data



# ❤️ Health check
@app.get("/Health")
async def health_check():
    try:
        await driver.verify_connectivity()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}