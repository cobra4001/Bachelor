from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from neo4j import AsyncGraphDatabase
from pydantic import BaseModel
from typing import Optional
from jose import JWTError, jwt
from datetime import datetime, timedelta
import logging

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# =========================================================
# AUTH CONFIG
# =========================================================

SECRET_KEY = "change-this-super-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Enkel brukerliste i minnet
fake_users_db = {
    "admin": {
        "username": "admin",
        "password": "password123"
    }
}


class UserCreate(BaseModel):
    username: str
    password: str


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing authentication",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")

        if username is None:
            raise credentials_exception

        user = fake_users_db.get(username)

        if user is None:
            raise credentials_exception

        return user

    except JWTError:
        raise credentials_exception


# =========================================================
# NEO4J CONFIG
# =========================================================

URI = "bolt://10.196.241.255:7687"
USER = "neo4j"
PASSWORD = "mRTzP1@h"

driver = None

# =========================================================
# LIFESPAN (Startup / Shutdown)
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global driver

    print("🚀 Starter API og kobler til Neo4j...")

    driver = AsyncGraphDatabase.driver(
        URI,
        auth=(USER, PASSWORD)
    )

    try:
        await driver.verify_connectivity()
        print("✅ Koblet til Neo4j!")
    except Exception as e:
        print("❌ Klarte ikke koble til Neo4j:", e)

    yield

    print("🛑 Lukker Neo4j-tilkobling...")
    await driver.close()


app = FastAPI(lifespan=lifespan)

# CORS for React
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# AUTH ENDPOINTS
# =========================================================

@app.post("/register")
async def register(user: UserCreate):
    logger.info(f"Registreringsforsøk: {user.username}")

    if user.username in fake_users_db:
        logger.warning(f"Brukernavn finnes allerede: {user.username}")
        raise HTTPException(
            status_code=400,
            detail="Username already exists"
        )

    fake_users_db[user.username] = {
        "username": user.username,
        "password": user.password
    }

    logger.info(f"Ny bruker opprettet: {user.username}")

    return {
        "message": f"User '{user.username}' created successfully"
    }


@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    logger.info(f"Loginforsøk: {form_data.username}")

    user = fake_users_db.get(form_data.username)

    if not user or user["password"] != form_data.password:
        logger.warning(f"Mislykket login: {form_data.username}")
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password"
        )

    access_token = create_access_token(
        data={"sub": form_data.username}
    )

    logger.info(f"Vellykket login: {form_data.username}")

    return {
        "access_token": access_token,
        "token_type": "bearer"
    }


@app.get("/me")
async def read_me(current_user: dict = Depends(get_current_user)):
    return {
        "username": current_user["username"]
    }


# =========================================================
# RESPONSE MODELS
# =========================================================

class KnownPersonResponse(BaseModel):
    personID: str
    is_known_person: bool


class PersonDetailResponse(BaseModel):
    personID: str
    fullName: Optional[str] = None
    birthDate: Optional[str] = None
    familyName: Optional[str] = None
    givenName: Optional[str] = None
    type: Optional[str] = None
    personType: Optional[str] = None


class RegisteredEntityResponse(BaseModel):
    entityID: str
    is_registered_entity: bool


class EntityDetailResponse(BaseModel):
    entityID: str
    name: Optional[str] = None
    alternateNames: Optional[str] = None
    entityType: Optional[str] = None
    foundingDate: Optional[str] = None
    incorporatedInJurisdiction_name: Optional[str] = None
    incorporatedInJurisdiction_code: Optional[str] = None
    dissolutionDate: Optional[str] = None


# =========================================================
# DB SESSION
# =========================================================

async def get_session():
    async with driver.session() as session:
        yield session


# =========================================================
# CYPHER FUNCTIONS
# =========================================================

async def check_known_person(session, personID: str):
    query = """
    MATCH (p:Person {personID: $personID})
    RETURN p.personID AS personID,
           p IS NOT NULL AS is_known_person
    """
    result = await session.run(query, personID=personID)
    record = await result.single()
    if not record:
        return None
    return dict(record)


async def get_person_details(session, personID: str):
    query = """
    MATCH (p:Person {personID: $personID})
    RETURN p.personID AS personID,
           p.fullName AS fullName,
           p.birthDate AS birthDate,
           p.familyName AS familyName,
           p.givenName AS givenName,
           p.type AS type,
           p.personType AS personType
    """
    result = await session.run(query, personID=personID)
    record = await result.single()
    if not record:
        return None
    return dict(record)


async def check_registered_entity(session, entityID: str):
    query = """
    MATCH (e:Entity {entityID: $entityID})
    RETURN e.entityID AS entityID,
           e IS NOT NULL AS is_registered_entity
    """
    result = await session.run(query, entityID=entityID)
    record = await result.single()
    if not record:
        return None
    return dict(record)


async def get_entity_details(session, entityID: str):
    query = """
    MATCH (e:Entity {entityID: $entityID})
    RETURN e.entityID AS entityID,
           e.name AS name,
           e.alternateNames AS alternateNames,
           e.entityType AS entityType,
           e.foundingDate AS foundingDate,
           e.incorporatedInJurisdiction_name AS incorporatedInJurisdiction_name,
           e.incorporatedInJurisdiction_code AS incorporatedInJurisdiction_code,
           e.dissolutionDate AS dissolutionDate
    """
    result = await session.run(query, entityID=entityID)
    record = await result.single()
    if not record:
        return None
    return dict(record)


# =========================================================
# PROTECTED ENDPOINTS
# =========================================================

@app.get("/Person/{personID}/known", response_model=KnownPersonResponse)
async def is_known_person(
    personID: str,
    user: dict = Depends(get_current_user),
    session=Depends(get_session)
):
    data = await check_known_person(session, personID)
    if not data:
        raise HTTPException(status_code=404, detail="Person not found")
    return data


@app.get("/Person/{personID}/details", response_model=PersonDetailResponse)
async def person_details_endpoint(
    personID: str,
    user: dict = Depends(get_current_user),
    session=Depends(get_session)
):
    data = await get_person_details(session, personID)
    if not data:
        raise HTTPException(status_code=404, detail="Person not found")
    return data


@app.get("/Entity/{entityID}/registered", response_model=RegisteredEntityResponse)
async def is_registered_entity(
    entityID: str,
    user: dict = Depends(get_current_user),
    session=Depends(get_session)
):
    data = await check_registered_entity(session, entityID)
    if not data:
        raise HTTPException(status_code=404, detail="Entity not found")
    return data


@app.get("/Entity/{entityID}/details", response_model=EntityDetailResponse)
async def entity_details_endpoint(
    entityID: str,
    user: dict = Depends(get_current_user),
    session=Depends(get_session)
):
    data = await get_entity_details(session, entityID)
    if not data:
        raise HTTPException(status_code=404, detail="Entity not found")
    return data


# =========================================================
# HEALTH CHECK (OPEN)
# =========================================================

@app.get("/health")
async def health_check():
    try:
        await driver.verify_connectivity()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}