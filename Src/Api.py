from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from neo4j import AsyncGraphDatabase
from pydantic import BaseModel

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
# RESPONSE MODEL
# -------------------------
class KnownPersonResponse(BaseModel):
    statement_id: str
    is_known_person: bool

# -------------------------
# DB SESSION
# -------------------------
async def get_session():
    async with driver.session() as session:
        yield session

# -------------------------
# CYPHER
# -------------------------
async def check_known_person(session, statement_id: str):
    query = """
    MATCH (ps:PersonStatement {statementID: $statement_id})
    RETURN ps.statementID AS statement_id,
    EXISTS {
        MATCH (o:OOCStatement {
            interestedParty_describedByPersonStatement: ps.statementID
        })
    } AS is_known_person
    """

    result = await session.run(query, statement_id=statement_id)
    record = await result.single()

    if not record:
        return None

    return {
        "statement_id": record["statement_id"],
        "is_known_person": record["is_known_person"]
    }

# -------------------------
# ROUTES
# -------------------------

# 🔍 Main endpoint
@app.get("/person-statement/{statement_id}/known", response_model=KnownPersonResponse)
async def is_known_person(statement_id: str, session=Depends(get_session)):
    data = await check_known_person(session, statement_id)

    if not data:
        raise HTTPException(status_code=404, detail="PersonStatement not found")

    return data


# ❤️ Health check (viktig!)
@app.get("/health")
async def health_check():
    try:
        await driver.verify_connectivity()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}