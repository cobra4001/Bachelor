from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from neo4j import AsyncGraphDatabase
from pydantic import BaseModel
from typing import Optional
from jose import JWTError, jwt
from datetime import datetime, timedelta
from itertools import combinations
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)

    conn.commit()

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
        async with driver.session() as session:
            await session.run("""
                CREATE INDEX inv_link_entity IF NOT EXISTS
                FOR ()-[r:INVESTIGATION_LINK]-()
                ON (r.sharedEntityID)
            """)
    except Exception as e:
        print("❌ Klarte ikke koble til Neo4j:", e)
    yield
    await driver.close()

app = FastAPI(
    lifespan=lifespan,
    title="OpenOwnership API",
    description="""
## API for søk i eierstruktur og etterforskningsrelasjoner

### Graf-logikk for eierskap/kontroll
```
Person/Entity -[IS_INTERESTED_PARTY]-> OOCStatement -[DESCRIBES_OWNERSHIP]-> Entity
Person/Entity -[IS_INTERESTED_PARTY]-> OOCStatement -[DESCRIBES_CONTROL]-> Entity
```

### Graf-logikk for etterforskningsrelasjoner
```
Person -[INVESTIGATION_LINK {tag, note, created_by, expires_at}]-> Person
```
Koblingen krever at begge personene er knyttet til samme firma via OOC-grafen.
    """,
)

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

@app.post("/register", tags=["Register"])
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
# 📦 RESPONSE MODELS — BASIS
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
# 📦 RESPONSE MODELS — OOC TILKOBLINGER
# =========================================================

class OwnershipEntry(BaseModel):
    entityID: str
    name: Optional[str] = None
    entityType: Optional[str] = None
    statementID: Optional[str] = None
    statementDate: Optional[str] = None
    interest_type: Optional[str] = None
    share_exact: Optional[float] = None
    share_minimum: Optional[float] = None
    share_maximum: Optional[float] = None
    share_exclusiveMinimum: Optional[bool] = None
    share_exclusiveMaximum: Optional[bool] = None
    details: Optional[str] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None

class ControlEntry(BaseModel):
    entityID: str
    name: Optional[str] = None
    entityType: Optional[str] = None
    statementID: Optional[str] = None
    statementDate: Optional[str] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None

class PersonConnectionsResponse(BaseModel):
    personID: str
    fullName: Optional[str] = None
    owns: list[OwnershipEntry] = []
    controls: list[ControlEntry] = []

class OwnerEntry(BaseModel):
    personID: Optional[str] = None
    entityID: Optional[str] = None
    fullName: Optional[str] = None
    name: Optional[str] = None
    statementID: Optional[str] = None
    statementDate: Optional[str] = None
    interest_type: Optional[str] = None
    share_exact: Optional[float] = None
    share_minimum: Optional[float] = None
    share_maximum: Optional[float] = None
    share_exclusiveMinimum: Optional[bool] = None
    share_exclusiveMaximum: Optional[bool] = None
    details: Optional[str] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None

class ControllerEntry(BaseModel):
    personID: Optional[str] = None
    entityID: Optional[str] = None
    fullName: Optional[str] = None
    name: Optional[str] = None
    statementID: Optional[str] = None
    statementDate: Optional[str] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None

class EntityConnectionsResponse(BaseModel):
    entityID: str
    name: Optional[str] = None
    ownership: list[OwnerEntry] = []
    control: list[ControllerEntry] = []

# =========================================================
# 📦 RESPONSE MODELS — ETTERFORSKNINGSRELASJONER
# =========================================================

class InvestigationLinkCreate(BaseModel):
    """
    Body for å opprette en etterforskningsrelasjon mellom to personer.

    - **fromPersonID**: personID koblingen går FRA
    - **toPersonID**: personID koblingen går TIL
    - **sharedEntityID**: entityID på firmaet begge er koblet til (brukes til validering)
    - **tag**: hashtag-etikett, f.eks. '#hvitvasking' eller '#korrupsjon'
    - **note**: valgfri fritekst
    - **expires_at**: ISO-dato for utløp (None = permanent kobling)
    """
    fromPersonID: str
    toPersonID: str
    sharedEntityID: str
    tag: str
    note: Optional[str] = None
    expires_at: Optional[str] = None  # ISO 8601, f.eks. "2025-12-31"

class InvestigationLinkResponse(BaseModel):
    linkID: str                          # Neo4j elementId på relasjonen
    fromPersonID: str
    fromPersonName: Optional[str] = None
    toPersonID: str
    toPersonName: Optional[str] = None
    sharedEntityID: str
    sharedEntityName: Optional[str] = None
    tag: str
    note: Optional[str] = None
    created_by: str
    created_at: str
    expires_at: Optional[str] = None
    is_expired: bool = False

class InvestigationLinkUpdate(BaseModel):
    """Felt som kan oppdateres etter opprettelse. Utelatte felt endres ikke."""
    tag: Optional[str] = None
    note: Optional[str] = None
    expires_at: Optional[str] = None

# =========================================================
# 📦 RESPONSE MODELS — BULK KOBLING
# =========================================================

class BulkLinkCreate(BaseModel):
    """
    Body for å masseknytte alle personer tilknyttet et firma.

    - **tag**: hashtag-etikett for saken, f.eks. '#sak-2024-42' eller '#korrupsjon'
    - **note**: valgfri fritekst som settes på alle koblingene
    - **expires_at**: ISO-dato for utløp (None = permanent)
    - **skip_existing**: hopp over par som allerede har en kobling med samme tag
                         og sharedEntityID (default: true)
    """
    tag: str
    note: Optional[str] = None
    expires_at: Optional[str] = None
    skip_existing: bool = True

class BulkLinkResponse(BaseModel):
    entityID: str
    entityName: Optional[str] = None
    tag: str
    created_count: int
    skipped_count: int
    persons_involved: list[str]
    links: list[InvestigationLinkResponse]

# =========================================================
# 🧠 DB SESSION
# =========================================================

async def get_session():
    async with driver.session() as session:
        yield session

# =========================================================
# 🧾 CYPHER FUNKSJONER — BASIS
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
# 🧾 CYPHER FUNKSJONER — OOC TILKOBLINGER
# =========================================================

async def get_person_connections(session, personID: str):
    person_query = """
        MATCH (p:Person {personID: $personID})
        RETURN p.personID AS personID, p.fullName AS fullName
    """
    owns_query = """
        MATCH (p:Person {personID: $personID})
              -[:IS_INTERESTED_PARTY]->(os:OOCStatement)
              -[r:DESCRIBES_OWNERSHIP]->(e:Entity)
        RETURN
            e.entityID               AS entityID,
            e.name                   AS name,
            e.entityType             AS entityType,
            os.statementID           AS statementID,
            os.statementDate         AS statementDate,
            r.type                   AS interest_type,
            r.share_exact            AS share_exact,
            r.share_minimum          AS share_minimum,
            r.share_maximum          AS share_maximum,
            r.share_exclusiveMinimum AS share_exclusiveMinimum,
            r.share_exclusiveMaximum AS share_exclusiveMaximum,
            r.details                AS details,
            r.startDate              AS startDate,
            r.endDate                AS endDate
        ORDER BY r.share_exact DESC
    """
    controls_query = """
        MATCH (p:Person {personID: $personID})
              -[:IS_INTERESTED_PARTY]->(os:OOCStatement)
              -[r:DESCRIBES_CONTROL]->(e:Entity)
        RETURN
            e.entityID       AS entityID,
            e.name           AS name,
            e.entityType     AS entityType,
            os.statementID   AS statementID,
            os.statementDate AS statementDate,
            r.startDate      AS startDate,
            r.endDate        AS endDate
    """
    person_result = await session.run(person_query, personID=personID)
    person_record = await person_result.single()
    if not person_record:
        return None
    owns_result  = await session.run(owns_query, personID=personID)
    owns_records = await owns_result.data()
    ctrl_result  = await session.run(controls_query, personID=personID)
    ctrl_records = await ctrl_result.data()
    return {
        "personID": person_record["personID"],
        "fullName": person_record["fullName"],
        "owns":     owns_records,
        "controls": ctrl_records,
    }


async def get_entity_connections(session, entityID: str):
    entity_query = """
        MATCH (e:Entity {entityID: $entityID})
        RETURN e.entityID AS entityID, e.name AS name
    """
    ownership_person_query = """
        MATCH (p:Person)
              -[:IS_INTERESTED_PARTY]->(os:OOCStatement)
              -[r:DESCRIBES_OWNERSHIP]->(e:Entity {entityID: $entityID})
        RETURN
            p.personID AS personID, null AS entityID,
            p.fullName AS fullName, null AS name,
            os.statementID AS statementID, os.statementDate AS statementDate,
            r.type AS interest_type, r.share_exact AS share_exact,
            r.share_minimum AS share_minimum, r.share_maximum AS share_maximum,
            r.share_exclusiveMinimum AS share_exclusiveMinimum,
            r.share_exclusiveMaximum AS share_exclusiveMaximum,
            r.details AS details, r.startDate AS startDate, r.endDate AS endDate
        ORDER BY r.share_exact DESC
    """
    ownership_entity_query = """
        MATCH (owner:Entity)
              -[:IS_INTERESTED_PARTY]->(os:OOCStatement)
              -[r:DESCRIBES_OWNERSHIP]->(e:Entity {entityID: $entityID})
        RETURN
            null AS personID, owner.entityID AS entityID,
            null AS fullName, owner.name AS name,
            os.statementID AS statementID, os.statementDate AS statementDate,
            r.type AS interest_type, r.share_exact AS share_exact,
            r.share_minimum AS share_minimum, r.share_maximum AS share_maximum,
            r.share_exclusiveMinimum AS share_exclusiveMinimum,
            r.share_exclusiveMaximum AS share_exclusiveMaximum,
            r.details AS details, r.startDate AS startDate, r.endDate AS endDate
        ORDER BY r.share_exact DESC
    """
    control_person_query = """
        MATCH (p:Person)
              -[:IS_INTERESTED_PARTY]->(os:OOCStatement)
              -[r:DESCRIBES_CONTROL]->(e:Entity {entityID: $entityID})
        RETURN
            p.personID AS personID, null AS entityID,
            p.fullName AS fullName, null AS name,
            os.statementID AS statementID, os.statementDate AS statementDate,
            r.startDate AS startDate, r.endDate AS endDate
    """
    control_entity_query = """
        MATCH (owner:Entity)
              -[:IS_INTERESTED_PARTY]->(os:OOCStatement)
              -[r:DESCRIBES_CONTROL]->(e:Entity {entityID: $entityID})
        RETURN
            null AS personID, owner.entityID AS entityID,
            null AS fullName, owner.name AS name,
            os.statementID AS statementID, os.statementDate AS statementDate,
            r.startDate AS startDate, r.endDate AS endDate
    """
    entity_result = await session.run(entity_query, entityID=entityID)
    entity_record = await entity_result.single()
    if not entity_record:
        return None
    own_p  = await (await session.run(ownership_person_query, entityID=entityID)).data()
    own_e  = await (await session.run(ownership_entity_query, entityID=entityID)).data()
    ctrl_p = await (await session.run(control_person_query,   entityID=entityID)).data()
    ctrl_e = await (await session.run(control_entity_query,   entityID=entityID)).data()
    return {
        "entityID":  entity_record["entityID"],
        "name":      entity_record["name"],
        "ownership": own_p + own_e,
        "control":   ctrl_p + ctrl_e,
    }

# =========================================================
# 🧾 CYPHER FUNKSJONER — ETTERFORSKNINGSRELASJONER
# =========================================================

def _format_link(record: dict) -> dict:
    """Mapper en Neo4j-rad til InvestigationLinkResponse-dict."""
    now = datetime.utcnow().isoformat()
    expires = record.get("expires_at")
    return {
        "linkID":           record["linkID"],
        "fromPersonID":     record["fromPersonID"],
        "fromPersonName":   record.get("fromPersonName"),
        "toPersonID":       record["toPersonID"],
        "toPersonName":     record.get("toPersonName"),
        "sharedEntityID":   record["sharedEntityID"],
        "sharedEntityName": record.get("sharedEntityName"),
        "tag":              record["tag"],
        "note":             record.get("note"),
        "created_by":       record["created_by"],
        "created_at":       record["created_at"],
        "expires_at":       expires,
        "is_expired":       bool(expires and expires < now),
    }


async def db_create_investigation_link(
    session, body: InvestigationLinkCreate, created_by: str
) -> dict:
    """
    Steg 1: Valider at begge personene er koblet til sharedEntityID via OOC-grafen.
    Steg 2: Opprett INVESTIGATION_LINK med alle metadata.
    """
    validate_query = """
        MATCH (fromP:Person {personID: $fromPersonID})
              -[:IS_INTERESTED_PARTY]->(:OOCStatement)
              -[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]->(e:Entity {entityID: $sharedEntityID})
        WITH fromP, e
        MATCH (toP:Person {personID: $toPersonID})
              -[:IS_INTERESTED_PARTY]->(:OOCStatement)
              -[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]->(e)
        RETURN
            fromP.personID AS fromPersonID,
            fromP.fullName AS fromPersonName,
            toP.personID   AS toPersonID,
            toP.fullName   AS toPersonName,
            e.entityID     AS entityID,
            e.name         AS entityName
        LIMIT 1
    """
    val_result = await session.run(
        validate_query,
        fromPersonID=body.fromPersonID,
        toPersonID=body.toPersonID,
        sharedEntityID=body.sharedEntityID,
    )
    val_record = await val_result.single()

    if not val_record:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Validering feilet: Begge personene ({body.fromPersonID}, {body.toPersonID}) "
                f"må være koblet til firma {body.sharedEntityID} via OOC-grafen."
            ),
        )

    now = datetime.utcnow().isoformat()
    tag = body.tag if body.tag.startswith("#") else f"#{body.tag}"

    create_query = """
        MATCH (fromP:Person {personID: $fromPersonID})
        MATCH (toP:Person   {personID: $toPersonID})
        CREATE (fromP)-[r:INVESTIGATION_LINK {
            sharedEntityID: $sharedEntityID,
            tag:            $tag,
            note:           $note,
            created_by:     $created_by,
            created_at:     $created_at,
            expires_at:     $expires_at
        }]->(toP)
        RETURN elementId(r) AS linkID
    """
    create_result = await session.run(
        create_query,
        fromPersonID=body.fromPersonID,
        toPersonID=body.toPersonID,
        sharedEntityID=body.sharedEntityID,
        tag=tag,
        note=body.note,
        created_by=created_by,
        created_at=now,
        expires_at=body.expires_at,
    )
    create_record = await create_result.single()

    return _format_link({
        "linkID":           create_record["linkID"],
        "fromPersonID":     val_record["fromPersonID"],
        "fromPersonName":   val_record["fromPersonName"],
        "toPersonID":       val_record["toPersonID"],
        "toPersonName":     val_record["toPersonName"],
        "sharedEntityID":   val_record["entityID"],
        "sharedEntityName": val_record["entityName"],
        "tag":              tag,
        "note":             body.note,
        "created_by":       created_by,
        "created_at":       now,
        "expires_at":       body.expires_at,
    })


async def db_get_links_for_person(session, personID: str) -> list[dict]:
    """Alle INVESTIGATION_LINK der personen er kilde eller mål."""
    query = """
        MATCH (fromP:Person)-[r:INVESTIGATION_LINK]->(toP:Person)
        WHERE fromP.personID = $personID OR toP.personID = $personID
        OPTIONAL MATCH (e:Entity {entityID: r.sharedEntityID})
        RETURN
            elementId(r)     AS linkID,
            fromP.personID   AS fromPersonID,
            fromP.fullName   AS fromPersonName,
            toP.personID     AS toPersonID,
            toP.fullName     AS toPersonName,
            r.sharedEntityID AS sharedEntityID,
            e.name           AS sharedEntityName,
            r.tag            AS tag,
            r.note           AS note,
            r.created_by     AS created_by,
            r.created_at     AS created_at,
            r.expires_at     AS expires_at
        ORDER BY r.created_at DESC
    """
    result = await session.run(query, personID=personID)
    records = await result.data()
    return [_format_link(r) for r in records]


async def db_get_links_for_entity(session, entityID: str) -> list[dict]:
    """Alle INVESTIGATION_LINK mellom personer koblet til et bestemt firma."""
    query = """
        MATCH (fromP:Person)-[r:INVESTIGATION_LINK]->(toP:Person)
        WHERE r.sharedEntityID = $entityID
        OPTIONAL MATCH (e:Entity {entityID: $entityID})
        RETURN
            elementId(r)     AS linkID,
            fromP.personID   AS fromPersonID,
            fromP.fullName   AS fromPersonName,
            toP.personID     AS toPersonID,
            toP.fullName     AS toPersonName,
            r.sharedEntityID AS sharedEntityID,
            e.name           AS sharedEntityName,
            r.tag            AS tag,
            r.note           AS note,
            r.created_by     AS created_by,
            r.created_at     AS created_at,
            r.expires_at     AS expires_at
        ORDER BY r.created_at DESC
    """
    result = await session.run(query, entityID=entityID)
    records = await result.data()
    return [_format_link(r) for r in records]


async def db_update_investigation_link(
    session, link_id: str, body: InvestigationLinkUpdate, updated_by: str
) -> dict:
    """Oppdaterer bare de feltene som er sendt inn (COALESCE-logikk)."""
    tag = body.tag
    if tag and not tag.startswith("#"):
        tag = f"#{tag}"

    query = """
        MATCH ()-[r:INVESTIGATION_LINK]->()
        WHERE elementId(r) = $link_id
        SET
            r.tag        = COALESCE($tag,        r.tag),
            r.note       = COALESCE($note,       r.note),
            r.expires_at = COALESCE($expires_at, r.expires_at),
            r.updated_by = $updated_by,
            r.updated_at = $updated_at
        WITH r
        MATCH (fromP:Person)-[r]->(toP:Person)
        OPTIONAL MATCH (e:Entity {entityID: r.sharedEntityID})
        RETURN
            elementId(r)     AS linkID,
            fromP.personID   AS fromPersonID,
            fromP.fullName   AS fromPersonName,
            toP.personID     AS toPersonID,
            toP.fullName     AS toPersonName,
            r.sharedEntityID AS sharedEntityID,
            e.name           AS sharedEntityName,
            r.tag            AS tag,
            r.note           AS note,
            r.created_by     AS created_by,
            r.created_at     AS created_at,
            r.expires_at     AS expires_at
    """
    result = await session.run(
        query,
        link_id=link_id,
        tag=tag,
        note=body.note,
        expires_at=body.expires_at,
        updated_by=updated_by,
        updated_at=datetime.utcnow().isoformat(),
    )
    record = await result.single()
    if not record:
        raise HTTPException(status_code=404, detail=f"Link {link_id} ikke funnet")
    return _format_link(dict(record))


async def db_delete_investigation_link(session, link_id: str) -> bool:
    query = """
        MATCH ()-[r:INVESTIGATION_LINK]->()
        WHERE elementId(r) = $link_id
        DELETE r
        RETURN count(r) AS deleted
    """
    result = await session.run(query, link_id=link_id)
    record = await result.single()
    return bool(record and record["deleted"] > 0)


async def db_get_co_persons(session, personID: str) -> list[dict]:
    """
    Finner alle andre personer som er koblet til de samme firmaene som personID.
    Viser også antall eksisterende investigation links mellom dem.
    Nyttig for å se hvem man kan koble til.
    """
    query = """
        MATCH (p:Person {personID: $personID})
              -[:IS_INTERESTED_PARTY]->(:OOCStatement)
              -[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]->(e:Entity)
              <-[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]-(:OOCStatement)
              <-[:IS_INTERESTED_PARTY]-(other:Person)
        WHERE other.personID <> $personID
        WITH DISTINCT other, e, p

        OPTIONAL MATCH (p)-[r:INVESTIGATION_LINK]->(other)
        OPTIONAL MATCH (other)-[r2:INVESTIGATION_LINK]->(p)

        RETURN
            other.personID                   AS personID,
            other.fullName                   AS fullName,
            collect(DISTINCT e.entityID)     AS sharedEntityIDs,
            collect(DISTINCT e.name)         AS sharedEntityNames,
            count(DISTINCT r) + count(DISTINCT r2) AS existingLinkCount
        ORDER BY existingLinkCount DESC, other.fullName
    """
    result = await session.run(query, personID=personID)
    return await result.data()


# =========================================================
# 🧾 CYPHER FUNKSJON — BULK KOBLING
# =========================================================

async def db_bulk_link_entity_persons(
    session,
    entityID: str,
    body: BulkLinkCreate,
    created_by: str,
) -> dict:
    """
    Finner alle personer koblet til entityID og oppretter
    INVESTIGATION_LINK mellom alle unike par (A→B, ikke B→A i tillegg).
    """
    # Hent firmainfo + alle tilknyttede personer
    persons_query = """
        MATCH (e:Entity {entityID: $entityID})
        OPTIONAL MATCH (p:Person)
              -[:IS_INTERESTED_PARTY]->(:OOCStatement)
              -[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]->(e)
        RETURN
            e.entityID AS entityID,
            e.name     AS entityName,
            collect(DISTINCT {personID: p.personID, fullName: p.fullName}) AS persons
    """
    result = await session.run(persons_query, entityID=entityID)
    record = await result.single()

    if not record or not record["entityID"]:
        raise HTTPException(status_code=404, detail=f"Entity {entityID} ikke funnet")

    # Filtrer ut None-entries (entity uten tilknyttede personer)
    persons = [p for p in record["persons"] if p["personID"] is not None]

    if len(persons) < 2:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Færre enn 2 personer er koblet til {entityID} — ingen par å koble. "
                f"Fant {len(persons)} person(er)."
            )
        )

    tag = body.tag if body.tag.startswith("#") else f"#{body.tag}"
    now = datetime.utcnow().isoformat()
    created_links = []
    skipped = 0

    # Generer alle unike retningsløse par
    pairs = list(combinations(persons, 2))

    for fromP, toP in pairs:
        # Sjekk om kobling med samme tag allerede finnes (begge retninger)
        if body.skip_existing:
            check_query = """
                MATCH (a:Person {personID: $fromID})-[r:INVESTIGATION_LINK]->(b:Person {personID: $toID})
                WHERE r.tag = $tag AND r.sharedEntityID = $entityID
                RETURN count(r) AS existing
                UNION
                MATCH (a:Person {personID: $toID})-[r:INVESTIGATION_LINK]->(b:Person {personID: $fromID})
                WHERE r.tag = $tag AND r.sharedEntityID = $entityID
                RETURN count(r) AS existing
            """
            check_result = await session.run(
                check_query,
                fromID=fromP["personID"],
                toID=toP["personID"],
                tag=tag,
                entityID=entityID,
            )
            check_records = await check_result.data()
            total_existing = sum(r["existing"] for r in check_records)
            if total_existing > 0:
                skipped += 1
                continue

        create_query = """
            MATCH (fromP:Person {personID: $fromPersonID})
            MATCH (toP:Person   {personID: $toPersonID})
            CREATE (fromP)-[r:INVESTIGATION_LINK {
                sharedEntityID: $sharedEntityID,
                tag:            $tag,
                note:           $note,
                created_by:     $created_by,
                created_at:     $created_at,
                expires_at:     $expires_at
            }]->(toP)
            RETURN elementId(r) AS linkID
        """
        create_result = await session.run(
            create_query,
            fromPersonID=fromP["personID"],
            toPersonID=toP["personID"],
            sharedEntityID=entityID,
            tag=tag,
            note=body.note,
            created_by=created_by,
            created_at=now,
            expires_at=body.expires_at,
        )
        create_record = await create_result.single()

        created_links.append(_format_link({
            "linkID":           create_record["linkID"],
            "fromPersonID":     fromP["personID"],
            "fromPersonName":   fromP["fullName"],
            "toPersonID":       toP["personID"],
            "toPersonName":     toP["fullName"],
            "sharedEntityID":   entityID,
            "sharedEntityName": record["entityName"],
            "tag":              tag,
            "note":             body.note,
            "created_by":       created_by,
            "created_at":       now,
            "expires_at":       body.expires_at,
        }))

    return {
        "entityID":         entityID,
        "entityName":       record["entityName"],
        "tag":              tag,
        "created_count":    len(created_links),
        "skipped_count":    skipped,
        "persons_involved": [p["personID"] for p in persons],
        "links":            created_links,
    }


# =========================================================
# 🔒 PROTECTED ENDPOINTS — BASIS
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
# 🔒 PROTECTED ENDPOINTS — OOC TILKOBLINGER
# =========================================================

@app.get(
    "/Person/{personID}/connections",
    tags=["Person"],
    response_model=PersonConnectionsResponse,
    summary="Hva personen eier og kontrollerer",
)
async def person_connections(
    personID: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """
    - **owns**: selskaper personen eier, med eierandeler
    - **controls**: selskaper personen kontrollerer
    """
    data = await get_person_connections(session, personID)
    if not data:
        raise HTTPException(status_code=404, detail="Person not found")
    logging.getLogger(user).info(f"Person connections queried: {personID}")
    return data


@app.get(
    "/Entity/{entityID}/connections",
    tags=["Entity"],
    response_model=EntityConnectionsResponse,
    summary="Hvem eier og kontrollerer selskapet",
)
async def entity_connections(
    entityID: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """
    - **ownership**: alle som eier selskapet (person eller annet selskap)
    - **control**: alle som kontrollerer selskapet
    """
    data = await get_entity_connections(session, entityID)
    if not data:
        raise HTTPException(status_code=404, detail="Entity not found")
    logging.getLogger(user).info(f"Entity connections queried: {entityID}")
    return data

# =========================================================
# 🔒 PROTECTED ENDPOINTS — ETTERFORSKNINGSRELASJONER
#
# Relasjonstype i Neo4j:
#   (Person)-[:INVESTIGATION_LINK {
#       sharedEntityID, tag, note,
#       created_by, created_at,
#       expires_at,
#       updated_by?, updated_at?
#   }]->(Person)
#
# Validering ved opprettelse:
#   Begge Person-noder må nå firma via OOC-grafen:
#   Person -[IS_INTERESTED_PARTY]-> OOCStatement
#          -[DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]-> Entity
# =========================================================

@app.post(
    "/investigation/links",
    tags=["Investigation"],
    response_model=InvestigationLinkResponse,
    status_code=201,
    summary="Opprett etterforskningsrelasjon mellom to personer",
)
async def create_investigation_link(
    body: InvestigationLinkCreate,
    user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """
    Oppretter en `INVESTIGATION_LINK` direkte mellom to Person-noder.

    **Validering**: Begge personene må allerede være koblet til `sharedEntityID`
    via OOC-grafen (eierskap eller kontroll).

    **Tag**: `#` legges til automatisk om det mangler (`hvitvasking` → `#hvitvasking`).

    **Midlertidig kobling**: Sett `expires_at` for å markere relasjonen som
    tidsbegrenset. Feltet `is_expired` settes automatisk til `true` i responsen
    når datoen er passert.
    """
    result = await db_create_investigation_link(session, body, created_by=user)
    logging.getLogger(user).info(
        f"Investigation link created: {body.fromPersonID} → {body.toPersonID} "
        f"via {body.sharedEntityID} tag={result['tag']}"
    )
    return result


@app.post(
    "/investigation/links/entity/{entityID}/bulk",
    tags=["Investigation"],
    response_model=BulkLinkResponse,
    status_code=201,
    summary="Koble alle personer tilknyttet et firma med én felles tag",
)
async def bulk_link_entity_persons(
    entityID: str,
    body: BulkLinkCreate,
    user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """
    Finner **alle personer** koblet til `entityID` via OOC-grafen og oppretter
    `INVESTIGATION_LINK` mellom samtlige unike par — merket med samme `tag`.

    Nyttig for å samle alle aktører rundt ett selskap under én saksreferanse,
    f.eks. `#sak-2024-42` eller `#korrupsjon`.

    - **tag**: `#` legges til automatisk om det mangler
    - **skip_existing** (default `true`): hopper over par som allerede har en
      kobling med nøyaktig samme `tag` og `sharedEntityID`
    - **created_count**: antall nye koblinger opprettet
    - **skipped_count**: antall par som ble hoppet over
    - **persons_involved**: liste over alle personID-er som inngår
    """
    result = await db_bulk_link_entity_persons(session, entityID, body, created_by=user)
    logging.getLogger(user).info(
        f"Bulk investigation links created for entity: {entityID} "
        f"tag={result['tag']} created={result['created_count']} skipped={result['skipped_count']}"
    )
    return result


@app.get(
    "/investigation/links/person/{personID}",
    tags=["Investigation"],
    response_model=list[InvestigationLinkResponse],
    summary="Hent etterforskningsrelasjoner for en person",
)
async def get_links_for_person(
    personID: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """
    Returnerer alle `INVESTIGATION_LINK`-relasjoner der personen er kilde **eller** mål.
    Utløpte koblinger inkluderes med `is_expired: true`.
    """
    data = await db_get_links_for_person(session, personID)
    logging.getLogger(user).info(
        f"Investigation links queried for person: {personID} → {len(data)} treff"
    )
    return data


@app.get(
    "/investigation/links/entity/{entityID}",
    tags=["Investigation"],
    response_model=list[InvestigationLinkResponse],
    summary="Hent alle etterforskningskoblinger rundt et firma",
)
async def get_links_for_entity(
    entityID: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """
    Returnerer alle `INVESTIGATION_LINK`-relasjoner der `sharedEntityID` matcher
    firmaet. Gir et samlet bilde av alle koblede personpar rundt ett selskap.
    """
    data = await db_get_links_for_entity(session, entityID)
    logging.getLogger(user).info(
        f"Investigation links queried for entity: {entityID} → {len(data)} treff"
    )
    return data


@app.get(
    "/investigation/links/person/{personID}/co-persons",
    tags=["Investigation"],
    summary="Finn andre personer koblet til samme firma(er)",
)
async def get_co_persons(
    personID: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """
    Finner alle andre Person-noder koblet til de samme firmaene som `personID`
    via OOC-grafen. Nyttig for å velge hvem man vil opprette en kobling mot.

    Returnerer per person:
    - `personID`, `fullName`
    - `sharedEntityIDs` / `sharedEntityNames` — felles firma
    - `existingLinkCount` — eksisterende investigation links mellom dem
    """
    data = await db_get_co_persons(session, personID)
    logging.getLogger(user).info(
        f"Co-persons queried for: {personID} → {len(data)} treff"
    )
    return data


@app.patch(
    "/investigation/links/{link_id}",
    tags=["Investigation"],
    response_model=InvestigationLinkResponse,
    summary="Oppdater tag, notat eller utløpsdato",
)
async def update_investigation_link(
    link_id: str,
    body: InvestigationLinkUpdate,
    user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """
    Oppdaterer ett eller flere felt på en eksisterende `INVESTIGATION_LINK`.
    Felt som **ikke** er med i body forblir uendret.

    `link_id` er Neo4j `elementId` på relasjonen — returnert ved opprettelse.
    """
    result = await db_update_investigation_link(session, link_id, body, updated_by=user)
    logging.getLogger(user).info(f"Investigation link updated: {link_id}")
    return result


@app.delete(
    "/investigation/links/{link_id}",
    tags=["Investigation"],
    status_code=200,
    summary="Slett en etterforskningskobling",
)
async def delete_investigation_link(
    link_id: str,
    user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """
    Sletter `INVESTIGATION_LINK`-relasjonen permanent fra Neo4j.
    Person-nodene og OOC-grafen er uberørt.
    """
    deleted = await db_delete_investigation_link(session, link_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Link {link_id} ikke funnet")
    logging.getLogger(user).info(f"Investigation link deleted: {link_id}")
    return {"msg": f"Link {link_id} slettet"}

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