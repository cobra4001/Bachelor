from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from neo4j import AsyncGraphDatabase
from pydantic import BaseModel
from typing import Optional
from jose import JWTError, jwt
from datetime import datetime, timedelta
import logging
from pathlib import Path
import sqlite3
from passlib.context import CryptContext

# =========================================================
# 🔐 AUTH CONFIG
# =========================================================

SECRET_KEY = "change-this-super-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

DB_FILE = "users.db"
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
MAX_BCRYPT_LENGTH = 72

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# =========================================================
# 📜 SQLITE USER DB
# =========================================================

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Opprett tabell hvis den ikke finnes
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)

    conn.commit()

    # Seed admin-bruker hvis den ikke finnes
    import os
    admin_password = os.getenv("ADMIN_PASSWORD", "Admin1234!")
    admin_password = admin_password[:MAX_BCRYPT_LENGTH]
    admin_hash = pwd_context.hash(admin_password)

    cursor.execute("SELECT username FROM users WHERE username = 'Admin'")
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?)",
            ("Admin", admin_hash, 1, datetime.utcnow().isoformat())
        )
        conn.commit()
        print("✅ Admin-bruker opprettet. Sett passord via ADMIN_PASSWORD (standard: Admin1234!)")

    conn.close()

init_db()

def create_user(username: str, password: str):
    password = password[:MAX_BCRYPT_LENGTH]
    password_hash = pwd_context.hash(password)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, 0, ?)",
        (username, password_hash, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()

def verify_user(username: str, password: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return False
    return pwd_context.verify(password, row[0])

def is_admin_user(username: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT is_admin FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    conn.close()
    return bool(row and row[0] == 1)

# =========================================================
# 📜 LOGGING SETUP
# =========================================================

def setup_user_logger(username: str):
    base_dir = Path("Logging")
    user_dir = base_dir / username
    user_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    logfile = user_dir / f"session_{timestamp}.log"

    logger = logging.getLogger(username)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.FileHandler(logfile, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info("=== New session started ===")

    return logger

# =========================================================
# 🗄️ NEO4J CONFIG
# =========================================================

URI = "bolt://10.196.241.255:7687"
USER = "neo4j"
PASSWORD = "mRTzP1@h"
driver = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global driver
    driver = AsyncGraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        await driver.verify_connectivity()
        print("✅ Koblet til Neo4j!")
    except Exception as e:
        print("❌ Klarte ikke koble til Neo4j:", e)
    yield
    await driver.close()

app = FastAPI(lifespan=lifespan)

# =========================================================
# 📜 MIDDLEWARE — LOGGER ALLE REQUESTS
# =========================================================

@app.middleware("http")
async def log_requests(request: Request, call_next):
    username = "anonymous"
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username = payload.get("sub", "anonymous")
        except JWTError:
            pass

    logger = logging.getLogger(username)
    client_ip = request.client.host
    logger.info(f"➡️ {request.method} {request.url.path} | IP: {client_ip}")

    response = await call_next(request)
    logger.info(f"⬅️ Status: {response.status_code}")
    return response

# =========================================================
# 🔐 AUTH HELPERS
# =========================================================

class UserRegister(BaseModel):
    username: str
    password: str

class UserUpdate(BaseModel):
    new_password: str

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing authentication",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT username FROM users WHERE username = ?", (username,))
        if not cursor.fetchone():
            conn.close()
            raise credentials_exception
        conn.close()
        return username
    except JWTError:
        raise credentials_exception

async def get_current_admin(user: str = Depends(get_current_user)) -> str:
    if not is_admin_user(user):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Du har ikke tilgang – kun admin-brukere kan gjøre dette"
        )
    return user

# =========================================================
# 🔐 AUTH ENDPOINTS
# =========================================================

@app.post("/register", tags =["Register"])
def register(user: UserRegister):
    try:
        create_user(user.username, user.password)
        setup_user_logger(user.username).info("User registered")
        return {"msg": f"User {user.username} registered successfully"}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Username already exists")

@app.post("/token", tags=["Login"])
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if not verify_user(form_data.username, form_data.password):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    setup_user_logger(form_data.username).info("User logged in")
    access_token = create_access_token(data={"sub": form_data.username})
    return {"access_token": access_token, "token_type": "bearer"}

# =========================================================
# 👑 ADMIN — USER MANAGEMENT ENDPOINTS
# =========================================================

@app.get("/users", tags=["Admin"])
def get_users(
    token: str = Depends(oauth2_scheme),
    admin: str = Depends(get_current_admin)
):
    """List all registered users. Admin only."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT username, is_admin, created_at FROM users")
    rows = cursor.fetchall()
    conn.close()
    return [
        {"username": row[0], "is_admin": bool(row[1]), "created_at": row[2]}
        for row in rows
    ]

@app.delete("/users/{username}", tags=["Admin"])
def delete_user(
    username: str,
    token: str = Depends(oauth2_scheme),
    admin: str = Depends(get_current_admin)
):
    """Delete a user by username. Admin only."""
    if username == "Admin":
        raise HTTPException(status_code=400, detail="Cannot delete the admin account")

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT username FROM users WHERE username = ?", (username,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="User not found")

    cursor.execute("DELETE FROM users WHERE username = ?", (username,))
    conn.commit()
    conn.close()

    logging.getLogger("admin").info(f"Admin deleted user: {username}")
    return {"msg": f"User '{username}' deleted successfully"}

@app.put("/users/{username}", tags=["Admin"])
def update_user(
    username: str,
    body: UserUpdate,
    token: str = Depends(oauth2_scheme),
    admin: str = Depends(get_current_admin)
):
    """Update a user's password. Admin only."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT username FROM users WHERE username = ?", (username,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="User not found")

    new_hash = pwd_context.hash(body.new_password[:MAX_BCRYPT_LENGTH])
    cursor.execute(
        "UPDATE users SET password_hash = ? WHERE username = ?",
        (new_hash, username)
    )
    conn.commit()
    conn.close()

    logging.getLogger("admin").info(f"Admin updated password for user: {username}")
    return {"msg": f"Password for '{username}' updated successfully"}

# =========================================================
# 📦 RESPONSE MODELS
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
# 🧠 DB SESSION
# =========================================================

async def get_session():
    async with driver.session() as session:
        yield session

# =========================================================
# 🧾 CYPHER FUNCTIONS
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
# 🔒 PROTECTED ENDPOINTS
# =========================================================

@app.get("/Person/{personID}/known", tags=["Person"], response_model=KnownPersonResponse)
async def is_known_person(
    personID: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session)
):
    data = await check_known_person(session, personID)
    if not data:
        raise HTTPException(status_code=404, detail="Person not found")
    return data

@app.get("/Person/{personID}/details", tags=["Person"], response_model=PersonDetailResponse)
async def person_details_endpoint(
    personID: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session)
):
    data = await get_person_details(session, personID)
    if not data:
        raise HTTPException(status_code=404, detail="Person not found")
    return data

@app.get("/Entity/{entityID}/registered", tags=["Entity"], response_model=RegisteredEntityResponse)
async def is_registered_entity(
    entityID: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session)
):
    data = await check_registered_entity(session, entityID)
    if not data:
        raise HTTPException(status_code=404, detail="Entity not found")
    return data

@app.get("/Entity/{entityID}/details", tags=["Entity"], response_model=EntityDetailResponse)
async def entity_details_endpoint(
    entityID: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session)
):
    data = await get_entity_details(session, entityID)
    if not data:
        raise HTTPException(status_code=404, detail="Entity not found")
    return data

# =========================================================
# ❤️ HEALTH CHECK
# =========================================================

@app.get("/health", tags=["System"])
async def health_check():
    try:
        await driver.verify_connectivity()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}