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
# 🔐 AUTENTISERINGSKONFIGURASJON
# =========================================================

SECRET_KEY = "change-this-super-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

DB_FILE = "users.db"
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
MAX_BCRYPT_LENGTH = 72

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# =========================================================
# 📜 SQLITE BRUKERDATABASE
# =========================================================

def init_db():
    """Oppretter brukertabellen og admin-bruker dersom de ikke finnes."""
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
    admin_passord = os.getenv("ADMIN_PASSWORD", "Admin1234!")
    admin_passord = admin_passord[:MAX_BCRYPT_LENGTH]
    admin_hash = pwd_context.hash(admin_passord)

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


def opprett_bruker(username: str, password: str):
    """Oppretter en ny bruker i databasen med hashet passord."""
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


def bekreft_bruker(username: str, password: str) -> bool:
    """Sjekker om brukernavn og passord stemmer overens."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
    rad = cursor.fetchone()
    conn.close()
    if not rad:
        return False
    return pwd_context.verify(password, rad[0])


def er_admin(username: str) -> bool:
    """Returnerer True dersom brukeren har admin-rettigheter."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT is_admin FROM users WHERE username = ?", (username,))
    rad = cursor.fetchone()
    conn.close()
    return bool(rad and rad[0] == 1)


# =========================================================
# 📜 LOGGING-OPPSETT
# =========================================================

def opprett_brukerlogger(username: str):
    """Oppretter en dedikert loggfil per bruker og økt."""
    base_dir = Path("Logging")
    bruker_dir = base_dir / username
    bruker_dir.mkdir(parents=True, exist_ok=True)
    tidsstempel = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    loggfil = bruker_dir / f"okt_{tidsstempel}.log"

    logger = logging.getLogger(username)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    behandler = logging.FileHandler(loggfil, encoding="utf-8")
    formatering = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    behandler.setFormatter(formatering)
    logger.addHandler(behandler)
    logger.info("=== Ny okt startet ===")

    return logger


# =========================================================
# 🗄️ NEO4J-KONFIGURASJON
# =========================================================

URI = "bolt://10.196.241.255:7687"
USER = "neo4j"
PASSWORD = "mRTzP1@h"
driver = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Starter og avslutter Neo4j-driveren sammen med applikasjonen."""
    global driver
    driver = AsyncGraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        await driver.verify_connectivity()
        print("✅ Koblet til Neo4j!")
        async with driver.session() as session:
            # Oppretter indeks på sharedEntityID for raskere oppslag
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

### Graf-logikk for eierskap og kontroll
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
# 📜 MELLOMVARE — LOGGER ALLE FORESPØRSLER
# =========================================================

@app.middleware("http")
async def logg_forespørsler(request: Request, call_next):
    """Logger alle innkommende forespørsler med bruker og IP-adresse."""
    brukernavn = "anonym"
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        try:
            nyttelast = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            brukernavn = nyttelast.get("sub", "anonym")
        except JWTError:
            pass

    logger = logging.getLogger(brukernavn)
    klient_ip = request.client.host
    logger.info(f"Innkommende: {request.method} {request.url.path} | IP: {klient_ip}")

    respons = await call_next(request)
    logger.info(f"Svar: Statuskode {respons.status_code}")
    return respons


# =========================================================
# 🔐 AUTENTISERINGSHJELPERE
# =========================================================

class BrukerRegistrering(BaseModel):
    username: str
    password: str


class BrukerOppdatering(BaseModel):
    new_password: str


def opprett_tilgangstoken(data: dict):
    """Genererer et signert JWT-token med utløpstid."""
    kopi = data.copy()
    utloper = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    kopi.update({"exp": utloper})
    return jwt.encode(kopi, SECRET_KEY, algorithm=ALGORITHM)


async def hent_innlogget_bruker(token: str = Depends(oauth2_scheme)) -> str:
    """Dekoder JWT-token og returnerer brukernavn. Kaster 401 ved ugyldig token."""
    autentiseringsfeil = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Ugyldig eller manglende autentisering",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        nyttelast = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        brukernavn = nyttelast.get("sub")
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT username FROM users WHERE username = ?", (brukernavn,))
        if not cursor.fetchone():
            conn.close()
            raise autentiseringsfeil
        conn.close()
        return brukernavn
    except JWTError:
        raise autentiseringsfeil


async def hent_innlogget_admin(bruker: str = Depends(hent_innlogget_bruker)) -> str:
    """Verifiserer at innlogget bruker har admin-rettigheter. Kaster 403 ellers."""
    if not er_admin(bruker):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Du har ikke tilgang – kun admin-brukere kan gjøre dette"
        )
    return bruker


# =========================================================
# 🔐 AUTENTISERINGSENDEPUNKTER
# =========================================================

@app.post("/register", tags=["Registrering"])
def registrer(bruker: BrukerRegistrering):
    """Registrerer en ny bruker i systemet."""
    try:
        opprett_bruker(bruker.username, bruker.password)
        opprett_brukerlogger(bruker.username).info("Bruker registrert")
        return {"melding": f"Bruker {bruker.username} ble registrert"}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Brukernavnet er allerede i bruk")


@app.post("/token", tags=["Innlogging"])
def logg_inn(skjemadata: OAuth2PasswordRequestForm = Depends()):
    """Logger inn en bruker og returnerer et JWT-tilgangstoken."""
    if not bekreft_bruker(skjemadata.username, skjemadata.password):
        raise HTTPException(status_code=400, detail="Feil brukernavn eller passord")
    opprett_brukerlogger(skjemadata.username).info("Bruker logget inn")
    tilgangstoken = opprett_tilgangstoken(data={"sub": skjemadata.username})
    return {"access_token": tilgangstoken, "token_type": "bearer"}


# =========================================================
# 👑 ADMIN — BRUKERADMINISTRASJON
# =========================================================

@app.get("/users", tags=["Admin"])
def hent_brukere(
    token: str = Depends(oauth2_scheme),
    admin: str = Depends(hent_innlogget_admin)
):
    """Lister alle registrerte brukere. Kun admin."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT username, is_admin, created_at FROM users")
    rader = cursor.fetchall()
    conn.close()
    return [
        {"brukernavn": rad[0], "er_admin": bool(rad[1]), "opprettet": rad[2]}
        for rad in rader
    ]


@app.delete("/users/{username}", tags=["Admin"])
def slett_bruker(
    username: str,
    token: str = Depends(oauth2_scheme),
    admin: str = Depends(hent_innlogget_admin)
):
    """Sletter en bruker basert på brukernavn. Kun admin."""
    if username == "Admin":
        raise HTTPException(status_code=400, detail="Admin-kontoen kan ikke slettes")

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT username FROM users WHERE username = ?", (username,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Brukeren ble ikke funnet")

    cursor.execute("DELETE FROM users WHERE username = ?", (username,))
    conn.commit()
    conn.close()

    logging.getLogger("admin").info(f"Admin slettet bruker: {username}")
    return {"melding": f"Bruker '{username}' ble slettet"}


@app.put("/users/{username}", tags=["Admin"])
def oppdater_bruker(
    username: str,
    body: BrukerOppdatering,
    token: str = Depends(oauth2_scheme),
    admin: str = Depends(hent_innlogget_admin)
):
    """Oppdaterer passordet til en bruker. Kun admin."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT username FROM users WHERE username = ?", (username,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Brukeren ble ikke funnet")

    nytt_hash = pwd_context.hash(body.new_password[:MAX_BCRYPT_LENGTH])
    cursor.execute(
        "UPDATE users SET password_hash = ? WHERE username = ?",
        (nytt_hash, username)
    )
    conn.commit()
    conn.close()

    logging.getLogger("admin").info(f"Admin oppdaterte passord for bruker: {username}")
    return {"melding": f"Passord for '{username}' ble oppdatert"}


# =========================================================
# 📦 RESPONSMODELLER — BASIS
# =========================================================

class KjentPersonSvar(BaseModel):
    personID: str
    is_known_person: bool


class PersonDetaljerSvar(BaseModel):
    personID: str
    fullName: Optional[str] = None
    birthDate: Optional[str] = None
    familyName: Optional[str] = None
    givenName: Optional[str] = None
    type: Optional[str] = None
    personType: Optional[str] = None


class RegistrertEntitySvar(BaseModel):
    entityID: str
    is_registered_entity: bool


class EntityDetaljerSvar(BaseModel):
    entityID: str
    name: Optional[str] = None
    alternateNames: Optional[str] = None
    entityType: Optional[str] = None
    foundingDate: Optional[str] = None
    incorporatedInJurisdiction_name: Optional[str] = None
    incorporatedInJurisdiction_code: Optional[str] = None
    dissolutionDate: Optional[str] = None


# =========================================================
# 📦 RESPONSMODELLER — OOC-TILKOBLINGER
# =========================================================

class EierskapsoppføringModell(BaseModel):
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


class KontrolloppføringModell(BaseModel):
    entityID: str
    name: Optional[str] = None
    entityType: Optional[str] = None
    statementID: Optional[str] = None
    statementDate: Optional[str] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None


class PersonTilkoblingerSvar(BaseModel):
    personID: str
    fullName: Optional[str] = None
    owns: list[EierskapsoppføringModell] = []
    controls: list[KontrolloppføringModell] = []


class EierEntityModell(BaseModel):
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


class KontrollørEntityModell(BaseModel):
    personID: Optional[str] = None
    entityID: Optional[str] = None
    fullName: Optional[str] = None
    name: Optional[str] = None
    statementID: Optional[str] = None
    statementDate: Optional[str] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None


class EntityTilkoblingerSvar(BaseModel):
    entityID: str
    name: Optional[str] = None
    ownership: list[EierEntityModell] = []
    control: list[KontrollørEntityModell] = []


# =========================================================
# 📦 RESPONSMODELLER — ETTERFORSKNINGSRELASJONER
# =========================================================

class EtterforskningskoblingOpprett(BaseModel):
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


class EtterforskningskoblingSvar(BaseModel):
    linkID: str                           # Neo4j elementId på relasjonen
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


class EtterforskningskoblingOppdater(BaseModel):
    """Felt som kan oppdateres etter opprettelse. Utelatte felt endres ikke."""
    tag: Optional[str] = None
    note: Optional[str] = None
    expires_at: Optional[str] = None


# =========================================================
# 📦 RESPONSMODELLER — MASSEKOBLING
# =========================================================

class MassekoblingOpprett(BaseModel):
    """
    Body for å masseknytte alle personer tilknyttet et firma.

    - **tag**: hashtag-etikett for saken, f.eks. '#sak-2024-42' eller '#korrupsjon'
    - **note**: valgfri fritekst som settes på alle koblingene
    - **expires_at**: ISO-dato for utløp (None = permanent)
    - **hopp_over_eksisterende**: hopp over par som allerede har en kobling
                                  med samme tag og sharedEntityID (standard: true)
    """
    tag: str
    note: Optional[str] = None
    expires_at: Optional[str] = None
    hopp_over_eksisterende: bool = True


class MassekoblingSvar(BaseModel):
    entityID: str
    entityName: Optional[str] = None
    tag: str
    antall_opprettet: int
    antall_hoppet_over: int
    involverte_personer: list[str]
    koblinger: list[EtterforskningskoblingSvar]


# =========================================================
# 🧠 DATABASEØKT
# =========================================================

async def hent_okt():
    """Gir en asynkron Neo4j-økt som avhengighet."""
    async with driver.session() as session:
        yield session


# =========================================================
# 🧾 CYPHER-FUNKSJONER — BASIS
# =========================================================

async def sjekk_kjent_person(session, personID: str):
    """Sjekker om en Person-node med gitt personID finnes i grafen."""
    sporring = """
    MATCH (p:Person {personID: $personID})
    RETURN p.personID AS personID,
           p IS NOT NULL AS is_known_person
    """
    resultat = await session.run(sporring, personID=personID)
    rad = await resultat.single()
    if not rad:
        return None
    return dict(rad)


async def hent_person_detaljer(session, personID: str):
    """Henter alle detaljfelt for en Person-node."""
    sporring = """
    MATCH (p:Person {personID: $personID})
    RETURN p.personID AS personID,
           p.fullName AS fullName,
           p.birthDate AS birthDate,
           p.familyName AS familyName,
           p.givenName AS givenName,
           p.type AS type,
           p.personType AS personType
    """
    resultat = await session.run(sporring, personID=personID)
    rad = await resultat.single()
    if not rad:
        return None
    return dict(rad)


async def sjekk_registrert_entity(session, entityID: str):
    """Sjekker om en Entity-node med gitt entityID finnes i grafen."""
    sporring = """
    MATCH (e:Entity {entityID: $entityID})
    RETURN e.entityID AS entityID,
           e IS NOT NULL AS is_registered_entity
    """
    resultat = await session.run(sporring, entityID=entityID)
    rad = await resultat.single()
    if not rad:
        return None
    return dict(rad)


async def hent_entity_detaljer(session, entityID: str):
    """Henter alle detaljfelt for en Entity-node."""
    sporring = """
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
    resultat = await session.run(sporring, entityID=entityID)
    rad = await resultat.single()
    if not rad:
        return None
    return dict(rad)


# =========================================================
# 🧾 CYPHER-FUNKSJONER — OOC-TILKOBLINGER
# =========================================================

async def hent_person_tilkoblinger(session, personID: str):
    """Henter alle eierskap- og kontrollrelasjoner for en person via OOC-grafen."""
    person_sporring = """
        MATCH (p:Person {personID: $personID})
        RETURN p.personID AS personID, p.fullName AS fullName
    """
    eierskap_sporring = """
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
    kontroll_sporring = """
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
    person_resultat = await session.run(person_sporring, personID=personID)
    person_rad = await person_resultat.single()
    if not person_rad:
        return None
    eierskap_resultat = await session.run(eierskap_sporring, personID=personID)
    eierskap_rader    = await eierskap_resultat.data()
    kontroll_resultat = await session.run(kontroll_sporring, personID=personID)
    kontroll_rader    = await kontroll_resultat.data()
    return {
        "personID": person_rad["personID"],
        "fullName": person_rad["fullName"],
        "owns":     eierskap_rader,
        "controls": kontroll_rader,
    }


async def hent_entity_tilkoblinger(session, entityID: str):
    """Henter alle eiere og kontrollører for en Entity via OOC-grafen."""
    entity_sporring = """
        MATCH (e:Entity {entityID: $entityID})
        RETURN e.entityID AS entityID, e.name AS name
    """
    eierskap_person_sporring = """
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
    eierskap_entity_sporring = """
        MATCH (eier:Entity)
              -[:IS_INTERESTED_PARTY]->(os:OOCStatement)
              -[r:DESCRIBES_OWNERSHIP]->(e:Entity {entityID: $entityID})
        RETURN
            null AS personID, eier.entityID AS entityID,
            null AS fullName, eier.name AS name,
            os.statementID AS statementID, os.statementDate AS statementDate,
            r.type AS interest_type, r.share_exact AS share_exact,
            r.share_minimum AS share_minimum, r.share_maximum AS share_maximum,
            r.share_exclusiveMinimum AS share_exclusiveMinimum,
            r.share_exclusiveMaximum AS share_exclusiveMaximum,
            r.details AS details, r.startDate AS startDate, r.endDate AS endDate
        ORDER BY r.share_exact DESC
    """
    kontroll_person_sporring = """
        MATCH (p:Person)
              -[:IS_INTERESTED_PARTY]->(os:OOCStatement)
              -[r:DESCRIBES_CONTROL]->(e:Entity {entityID: $entityID})
        RETURN
            p.personID AS personID, null AS entityID,
            p.fullName AS fullName, null AS name,
            os.statementID AS statementID, os.statementDate AS statementDate,
            r.startDate AS startDate, r.endDate AS endDate
    """
    kontroll_entity_sporring = """
        MATCH (eier:Entity)
              -[:IS_INTERESTED_PARTY]->(os:OOCStatement)
              -[r:DESCRIBES_CONTROL]->(e:Entity {entityID: $entityID})
        RETURN
            null AS personID, eier.entityID AS entityID,
            null AS fullName, eier.name AS name,
            os.statementID AS statementID, os.statementDate AS statementDate,
            r.startDate AS startDate, r.endDate AS endDate
    """
    entity_resultat = await session.run(entity_sporring, entityID=entityID)
    entity_rad = await entity_resultat.single()
    if not entity_rad:
        return None
    eier_p  = await (await session.run(eierskap_person_sporring, entityID=entityID)).data()
    eier_e  = await (await session.run(eierskap_entity_sporring,  entityID=entityID)).data()
    ctrl_p  = await (await session.run(kontroll_person_sporring, entityID=entityID)).data()
    ctrl_e  = await (await session.run(kontroll_entity_sporring,  entityID=entityID)).data()
    return {
        "entityID":  entity_rad["entityID"],
        "name":      entity_rad["name"],
        "ownership": eier_p + eier_e,
        "control":   ctrl_p + ctrl_e,
    }


# =========================================================
# 🧾 CYPHER-FUNKSJONER — ETTERFORSKNINGSRELASJONER
# =========================================================

def _formater_kobling(rad: dict) -> dict:
    """Mapper en Neo4j-rad til EtterforskningskoblingSvar-dict."""
    na = datetime.utcnow().isoformat()
    utloper = rad.get("expires_at")
    return {
        "linkID":           rad["linkID"],
        "fromPersonID":     rad["fromPersonID"],
        "fromPersonName":   rad.get("fromPersonName"),
        "toPersonID":       rad["toPersonID"],
        "toPersonName":     rad.get("toPersonName"),
        "sharedEntityID":   rad["sharedEntityID"],
        "sharedEntityName": rad.get("sharedEntityName"),
        "tag":              rad["tag"],
        "note":             rad.get("note"),
        "created_by":       rad["created_by"],
        "created_at":       rad["created_at"],
        "expires_at":       utloper,
        "is_expired":       bool(utloper and utloper < na),
    }


async def db_opprett_etterforskningskobling(
    session, body: EtterforskningskoblingOpprett, opprettet_av: str
) -> dict:
    """
    Steg 1: Validerer at begge personene er koblet til sharedEntityID via OOC-grafen.
    Steg 2: Oppretter INVESTIGATION_LINK med alle metadata.
    """
    valider_sporring = """
        MATCH (fraP:Person {personID: $fromPersonID})
              -[:IS_INTERESTED_PARTY]->(:OOCStatement)
              -[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]->(e:Entity {entityID: $sharedEntityID})
        WITH fraP, e
        MATCH (tilP:Person {personID: $toPersonID})
              -[:IS_INTERESTED_PARTY]->(:OOCStatement)
              -[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]->(e)
        RETURN
            fraP.personID AS fromPersonID,
            fraP.fullName AS fromPersonName,
            tilP.personID AS toPersonID,
            tilP.fullName AS toPersonName,
            e.entityID    AS entityID,
            e.name        AS entityName
        LIMIT 1
    """
    val_resultat = await session.run(
        valider_sporring,
        fromPersonID=body.fromPersonID,
        toPersonID=body.toPersonID,
        sharedEntityID=body.sharedEntityID,
    )
    val_rad = await val_resultat.single()

    if not val_rad:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Validering feilet: Begge personene ({body.fromPersonID}, {body.toPersonID}) "
                f"må være koblet til firma {body.sharedEntityID} via OOC-grafen."
            ),
        )

    na = datetime.utcnow().isoformat()
    tag = body.tag if body.tag.startswith("#") else f"#{body.tag}"

    opprett_sporring = """
        MATCH (fraP:Person {personID: $fromPersonID})
        MATCH (tilP:Person {personID: $toPersonID})
        CREATE (fraP)-[r:INVESTIGATION_LINK {
            sharedEntityID: $sharedEntityID,
            tag:            $tag,
            note:           $note,
            created_by:     $created_by,
            created_at:     $created_at,
            expires_at:     $expires_at
        }]->(tilP)
        RETURN elementId(r) AS linkID
    """
    opprett_resultat = await session.run(
        opprett_sporring,
        fromPersonID=body.fromPersonID,
        toPersonID=body.toPersonID,
        sharedEntityID=body.sharedEntityID,
        tag=tag,
        note=body.note,
        created_by=opprettet_av,
        created_at=na,
        expires_at=body.expires_at,
    )
    opprett_rad = await opprett_resultat.single()

    return _formater_kobling({
        "linkID":           opprett_rad["linkID"],
        "fromPersonID":     val_rad["fromPersonID"],
        "fromPersonName":   val_rad["fromPersonName"],
        "toPersonID":       val_rad["toPersonID"],
        "toPersonName":     val_rad["toPersonName"],
        "sharedEntityID":   val_rad["entityID"],
        "sharedEntityName": val_rad["entityName"],
        "tag":              tag,
        "note":             body.note,
        "created_by":       opprettet_av,
        "created_at":       na,
        "expires_at":       body.expires_at,
    })


async def db_hent_koblinger_for_person(session, personID: str) -> list[dict]:
    """Henter alle INVESTIGATION_LINK der personen er kilde eller mål."""
    sporring = """
        MATCH (fraP:Person)-[r:INVESTIGATION_LINK]->(tilP:Person)
        WHERE fraP.personID = $personID OR tilP.personID = $personID
        OPTIONAL MATCH (e:Entity {entityID: r.sharedEntityID})
        RETURN
            elementId(r)     AS linkID,
            fraP.personID    AS fromPersonID,
            fraP.fullName    AS fromPersonName,
            tilP.personID    AS toPersonID,
            tilP.fullName    AS toPersonName,
            r.sharedEntityID AS sharedEntityID,
            e.name           AS sharedEntityName,
            r.tag            AS tag,
            r.note           AS note,
            r.created_by     AS created_by,
            r.created_at     AS created_at,
            r.expires_at     AS expires_at
        ORDER BY r.created_at DESC
    """
    resultat = await session.run(sporring, personID=personID)
    rader = await resultat.data()
    return [_formater_kobling(r) for r in rader]


async def db_hent_koblinger_for_entity(session, entityID: str) -> list[dict]:
    """Henter alle INVESTIGATION_LINK mellom personer koblet til et bestemt Entity."""
    sporring = """
        MATCH (fraP:Person)-[r:INVESTIGATION_LINK]->(tilP:Person)
        WHERE r.sharedEntityID = $entityID
        OPTIONAL MATCH (e:Entity {entityID: $entityID})
        RETURN
            elementId(r)     AS linkID,
            fraP.personID    AS fromPersonID,
            fraP.fullName    AS fromPersonName,
            tilP.personID    AS toPersonID,
            tilP.fullName    AS toPersonName,
            r.sharedEntityID AS sharedEntityID,
            e.name           AS sharedEntityName,
            r.tag            AS tag,
            r.note           AS note,
            r.created_by     AS created_by,
            r.created_at     AS created_at,
            r.expires_at     AS expires_at
        ORDER BY r.created_at DESC
    """
    resultat = await session.run(sporring, entityID=entityID)
    rader = await resultat.data()
    return [_formater_kobling(r) for r in rader]


async def db_oppdater_etterforskningskobling(
    session, link_id: str, body: EtterforskningskoblingOppdater, oppdatert_av: str
) -> dict:
    """Oppdaterer kun de feltene som er sendt inn (COALESCE-logikk)."""
    tag = body.tag
    if tag and not tag.startswith("#"):
        tag = f"#{tag}"

    sporring = """
        MATCH ()-[r:INVESTIGATION_LINK]->()
        WHERE elementId(r) = $link_id
        SET
            r.tag        = COALESCE($tag,        r.tag),
            r.note       = COALESCE($note,       r.note),
            r.expires_at = COALESCE($expires_at, r.expires_at),
            r.updated_by = $updated_by,
            r.updated_at = $updated_at
        WITH r
        MATCH (fraP:Person)-[r]->(tilP:Person)
        OPTIONAL MATCH (e:Entity {entityID: r.sharedEntityID})
        RETURN
            elementId(r)     AS linkID,
            fraP.personID    AS fromPersonID,
            fraP.fullName    AS fromPersonName,
            tilP.personID    AS toPersonID,
            tilP.fullName    AS toPersonName,
            r.sharedEntityID AS sharedEntityID,
            e.name           AS sharedEntityName,
            r.tag            AS tag,
            r.note           AS note,
            r.created_by     AS created_by,
            r.created_at     AS created_at,
            r.expires_at     AS expires_at
    """
    resultat = await session.run(
        sporring,
        link_id=link_id,
        tag=tag,
        note=body.note,
        expires_at=body.expires_at,
        updated_by=oppdatert_av,
        updated_at=datetime.utcnow().isoformat(),
    )
    rad = await resultat.single()
    if not rad:
        raise HTTPException(status_code=404, detail=f"Kobling {link_id} ble ikke funnet")
    return _formater_kobling(dict(rad))


async def db_slett_etterforskningskobling(session, link_id: str) -> bool:
    """Sletter en INVESTIGATION_LINK fra Neo4j basert på elementId."""
    sporring = """
        MATCH ()-[r:INVESTIGATION_LINK]->()
        WHERE elementId(r) = $link_id
        DELETE r
        RETURN count(r) AS slettet
    """
    resultat = await session.run(sporring, link_id=link_id)
    rad = await resultat.single()
    return bool(rad and rad["slettet"] > 0)


async def db_hent_mede_personer(session, personID: str) -> list[dict]:
    """
    Finner alle andre personer koblet til de samme firmaene som personID.
    Returnerer også antall eksisterende etterforskningskoblinger mellom dem.
    Nyttig for å se hvem man kan koble til.
    """
    sporring = """
        MATCH (p:Person {personID: $personID})
              -[:IS_INTERESTED_PARTY]->(:OOCStatement)
              -[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]->(e:Entity)
              <-[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]-(:OOCStatement)
              <-[:IS_INTERESTED_PARTY]-(annen:Person)
        WHERE annen.personID <> $personID
        WITH DISTINCT annen, e, p

        OPTIONAL MATCH (p)-[r:INVESTIGATION_LINK]->(annen)
        OPTIONAL MATCH (annen)-[r2:INVESTIGATION_LINK]->(p)

        RETURN
            annen.personID                         AS personID,
            annen.fullName                         AS fullName,
            collect(DISTINCT e.entityID)           AS sharedEntityIDs,
            collect(DISTINCT e.name)               AS sharedEntityNames,
            count(DISTINCT r) + count(DISTINCT r2) AS eksisterendeKoblinger
        ORDER BY eksisterendeKoblinger DESC, annen.fullName
    """
    resultat = await session.run(sporring, personID=personID)
    return await resultat.data()


# =========================================================
# 🧾 CYPHER-FUNKSJON — MASSEKOBLING
# =========================================================

async def db_masskoblinger_for_entity(
    session,
    entityID: str,
    body: MassekoblingOpprett,
    opprettet_av: str,
) -> dict:
    """
    Finner alle personer koblet til entityID og oppretter
    INVESTIGATION_LINK mellom alle unike par (A->B, ikke B->A i tillegg).
    """
    # Henter firmainformasjon og alle tilknyttede personer
    personer_sporring = """
        MATCH (e:Entity {entityID: $entityID})
        OPTIONAL MATCH (p:Person)
              -[:IS_INTERESTED_PARTY]->(:OOCStatement)
              -[:DESCRIBES_OWNERSHIP|DESCRIBES_CONTROL]->(e)
        RETURN
            e.entityID AS entityID,
            e.name     AS entityName,
            collect(DISTINCT {personID: p.personID, fullName: p.fullName}) AS personer
    """
    resultat = await session.run(personer_sporring, entityID=entityID)
    rad = await resultat.single()

    if not rad or not rad["entityID"]:
        raise HTTPException(status_code=404, detail=f"Entity {entityID} ble ikke funnet")

    # Filtrerer bort None-oppføringer (entity uten tilknyttede personer)
    personer = [p for p in rad["personer"] if p["personID"] is not None]

    if len(personer) < 2:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Færre enn 2 personer er koblet til {entityID} — ingen par å koble. "
                f"Fant {len(personer)} person(er)."
            )
        )

    tag = body.tag if body.tag.startswith("#") else f"#{body.tag}"
    na = datetime.utcnow().isoformat()
    opprettede_koblinger = []
    hoppet_over = 0

    # Genererer alle unike retningsløse par
    par = list(combinations(personer, 2))

    for fraP, tilP in par:
        # Sjekker om kobling med samme tag allerede finnes (begge retninger)
        if body.hopp_over_eksisterende:
            sjekk_sporring = """
                MATCH (a:Person {personID: $fraID})-[r:INVESTIGATION_LINK]->(b:Person {personID: $tilID})
                WHERE r.tag = $tag AND r.sharedEntityID = $entityID
                RETURN count(r) AS eksisterende
                UNION
                MATCH (a:Person {personID: $tilID})-[r:INVESTIGATION_LINK]->(b:Person {personID: $fraID})
                WHERE r.tag = $tag AND r.sharedEntityID = $entityID
                RETURN count(r) AS eksisterende
            """
            sjekk_resultat = await session.run(
                sjekk_sporring,
                fraID=fraP["personID"],
                tilID=tilP["personID"],
                tag=tag,
                entityID=entityID,
            )
            sjekk_rader = await sjekk_resultat.data()
            totalt_eksisterende = sum(r["eksisterende"] for r in sjekk_rader)
            if totalt_eksisterende > 0:
                hoppet_over += 1
                continue

        opprett_sporring = """
            MATCH (fraP:Person {personID: $fromPersonID})
            MATCH (tilP:Person {personID: $toPersonID})
            CREATE (fraP)-[r:INVESTIGATION_LINK {
                sharedEntityID: $sharedEntityID,
                tag:            $tag,
                note:           $note,
                created_by:     $created_by,
                created_at:     $created_at,
                expires_at:     $expires_at
            }]->(tilP)
            RETURN elementId(r) AS linkID
        """
        opprett_resultat = await session.run(
            opprett_sporring,
            fromPersonID=fraP["personID"],
            toPersonID=tilP["personID"],
            sharedEntityID=entityID,
            tag=tag,
            note=body.note,
            created_by=opprettet_av,
            created_at=na,
            expires_at=body.expires_at,
        )
        opprett_rad = await opprett_resultat.single()

        opprettede_koblinger.append(_formater_kobling({
            "linkID":           opprett_rad["linkID"],
            "fromPersonID":     fraP["personID"],
            "fromPersonName":   fraP["fullName"],
            "toPersonID":       tilP["personID"],
            "toPersonName":     tilP["fullName"],
            "sharedEntityID":   entityID,
            "sharedEntityName": rad["entityName"],
            "tag":              tag,
            "note":             body.note,
            "created_by":       opprettet_av,
            "created_at":       na,
            "expires_at":       body.expires_at,
        }))

    return {
        "entityID":           entityID,
        "entityName":         rad["entityName"],
        "tag":                tag,
        "antall_opprettet":   len(opprettede_koblinger),
        "antall_hoppet_over": hoppet_over,
        "involverte_personer": [p["personID"] for p in personer],
        "koblinger":          opprettede_koblinger,
    }


# =========================================================
# 🔒 BESKYTTEDE ENDEPUNKTER — BASIS
# =========================================================

@app.get("/Person/{personID}/kjent", tags=["Person"], response_model=KjentPersonSvar)
async def er_kjent_person(
    personID: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt)
):
    """Sjekker om en person med gitt personID finnes i grafen."""
    data = await sjekk_kjent_person(session, personID)
    if not data:
        raise HTTPException(status_code=404, detail="Person ble ikke funnet")
    return data


@app.get("/Person/{personID}/detaljer", tags=["Person"], response_model=PersonDetaljerSvar)
async def person_detaljer_endepunkt(
    personID: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt)
):
    """Returnerer alle detaljfelt for en person."""
    data = await hent_person_detaljer(session, personID)
    if not data:
        raise HTTPException(status_code=404, detail="Person ble ikke funnet")
    return data


@app.get("/Entity/{entityID}/registrert", tags=["Entity"], response_model=RegistrertEntitySvar)
async def er_registrert_entity(
    entityID: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt)
):
    """Sjekker om en Entity med gitt entityID finnes i grafen."""
    data = await sjekk_registrert_entity(session, entityID)
    if not data:
        raise HTTPException(status_code=404, detail="Entity ble ikke funnet")
    return data


@app.get("/Entity/{entityID}/detaljer", tags=["Entity"], response_model=EntityDetaljerSvar)
async def entity_detaljer_endepunkt(
    entityID: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt)
):
    """Returnerer alle detaljfelt for en Entity."""
    data = await hent_entity_detaljer(session, entityID)
    if not data:
        raise HTTPException(status_code=404, detail="Entity ble ikke funnet")
    return data


# =========================================================
# 🔒 BESKYTTEDE ENDEPUNKTER — OOC-TILKOBLINGER
# =========================================================

@app.get(
    "/Person/{personID}/tilkoblinger",
    tags=["Person"],
    response_model=PersonTilkoblingerSvar,
    summary="Hva personen eier og kontrollerer",
)
async def person_tilkoblinger(
    personID: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt),
):
    """
    - **owns**: selskaper personen eier, med eierandeler
    - **controls**: selskaper personen kontrollerer
    """
    data = await hent_person_tilkoblinger(session, personID)
    if not data:
        raise HTTPException(status_code=404, detail="Person ble ikke funnet")
    logging.getLogger(bruker).info(f"Persontilkoblinger hentet: {personID}")
    return data


@app.get(
    "/Entity/{entityID}/tilkoblinger",
    tags=["Entity"],
    response_model=EntityTilkoblingerSvar,
    summary="Hvem eier og kontrollerer selskapet",
)
async def entity_tilkoblinger(
    entityID: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt),
):
    """
    - **ownership**: alle som eier selskapet (person eller annet selskap)
    - **control**: alle som kontrollerer selskapet
    """
    data = await hent_entity_tilkoblinger(session, entityID)
    if not data:
        raise HTTPException(status_code=404, detail="Entity ble ikke funnet")
    logging.getLogger(bruker).info(f"Entity-tilkoblinger hentet: {entityID}")
    return data


# =========================================================
# 🔒 BESKYTTEDE ENDEPUNKTER — ETTERFORSKNINGSRELASJONER
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
    "/etterforskning/koblinger",
    tags=["Etterforskning"],
    response_model=EtterforskningskoblingSvar,
    status_code=201,
    summary="Opprett etterforskningsrelasjon mellom to personer",
)
async def opprett_etterforskningskobling(
    body: EtterforskningskoblingOpprett,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt),
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
    resultat = await db_opprett_etterforskningskobling(session, body, opprettet_av=bruker)
    logging.getLogger(bruker).info(
        f"Etterforskningskobling opprettet: {body.fromPersonID} -> {body.toPersonID} "
        f"via {body.sharedEntityID} tag={resultat['tag']}"
    )
    return resultat


@app.post(
    "/etterforskning/koblinger/entity/{entityID}/masse",
    tags=["Etterforskning"],
    response_model=MassekoblingSvar,
    status_code=201,
    summary="Koble alle personer tilknyttet et firma med én felles tag",
)
async def massekoblinger_for_entity(
    entityID: str,
    body: MassekoblingOpprett,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt),
):
    """
    Finner **alle personer** koblet til `entityID` via OOC-grafen og oppretter
    `INVESTIGATION_LINK` mellom samtlige unike par — merket med samme `tag`.

    Nyttig for å samle alle aktører rundt ett selskap under én saksreferanse,
    f.eks. `#sak-2024-42` eller `#korrupsjon`.

    - **tag**: `#` legges til automatisk om det mangler
    - **hopp_over_eksisterende** (standard `true`): hopper over par som allerede har en
      kobling med nøyaktig samme `tag` og `sharedEntityID`
    - **antall_opprettet**: antall nye koblinger opprettet
    - **antall_hoppet_over**: antall par som ble hoppet over
    - **involverte_personer**: liste over alle personID-er som inngår
    """
    resultat = await db_masskoblinger_for_entity(session, entityID, body, opprettet_av=bruker)
    logging.getLogger(bruker).info(
        f"Masskoblinger opprettet for entity: {entityID} "
        f"tag={resultat['tag']} opprettet={resultat['antall_opprettet']} "
        f"hoppet_over={resultat['antall_hoppet_over']}"
    )
    return resultat


@app.get(
    "/etterforskning/koblinger/person/{personID}",
    tags=["Etterforskning"],
    response_model=list[EtterforskningskoblingSvar],
    summary="Hent etterforskningsrelasjoner for en person",
)
async def hent_koblinger_for_person(
    personID: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt),
):
    """
    Returnerer alle `INVESTIGATION_LINK`-relasjoner der personen er kilde **eller** mål.
    Utløpte koblinger inkluderes med `is_expired: true`.
    """
    data = await db_hent_koblinger_for_person(session, personID)
    logging.getLogger(bruker).info(
        f"Etterforskningskoblinger hentet for person: {personID} -> {len(data)} treff"
    )
    return data


@app.get(
    "/etterforskning/koblinger/entity/{entityID}",
    tags=["Etterforskning"],
    response_model=list[EtterforskningskoblingSvar],
    summary="Hent alle etterforskningskoblinger rundt et firma",
)
async def hent_koblinger_for_entity(
    entityID: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt),
):
    """
    Returnerer alle `INVESTIGATION_LINK`-relasjoner der `sharedEntityID` matcher
    firmaet. Gir et samlet bilde av alle koblede personpar rundt ett selskap.
    """
    data = await db_hent_koblinger_for_entity(session, entityID)
    logging.getLogger(bruker).info(
        f"Etterforskningskoblinger hentet for entity: {entityID} -> {len(data)} treff"
    )
    return data


@app.get(
    "/etterforskning/koblinger/person/{personID}/medpersoner",
    tags=["Etterforskning"],
    summary="Finn andre personer koblet til samme firma(er)",
)
async def hent_mede_personer(
    personID: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt),
):
    """
    Finner alle andre Person-noder koblet til de samme firmaene som `personID`
    via OOC-grafen. Nyttig for å velge hvem man vil opprette en kobling mot.

    Returnerer per person:
    - `personID`, `fullName`
    - `sharedEntityIDs` / `sharedEntityNames` — felles firma
    - `eksisterendeKoblinger` — antall eksisterende etterforskningskoblinger mellom dem
    """
    data = await db_hent_mede_personer(session, personID)
    logging.getLogger(bruker).info(
        f"Medpersoner hentet for: {personID} -> {len(data)} treff"
    )
    return data


@app.patch(
    "/etterforskning/koblinger/{link_id}",
    tags=["Etterforskning"],
    response_model=EtterforskningskoblingSvar,
    summary="Oppdater tag, notat eller utløpsdato",
)
async def oppdater_etterforskningskobling(
    link_id: str,
    body: EtterforskningskoblingOppdater,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt),
):
    """
    Oppdaterer ett eller flere felt på en eksisterende `INVESTIGATION_LINK`.
    Felt som **ikke** er med i body forblir uendret.

    `link_id` er Neo4j `elementId` på relasjonen — returnert ved opprettelse.
    """
    resultat = await db_oppdater_etterforskningskobling(session, link_id, body, oppdatert_av=bruker)
    logging.getLogger(bruker).info(f"Etterforskningskobling oppdatert: {link_id}")
    return resultat


@app.delete(
    "/etterforskning/koblinger/{link_id}",
    tags=["Etterforskning"],
    status_code=200,
    summary="Slett en etterforskningskobling",
)
async def slett_etterforskningskobling(
    link_id: str,
    bruker: str = Depends(hent_innlogget_bruker),
    session=Depends(hent_okt),
):
    """
    Sletter `INVESTIGATION_LINK`-relasjonen permanent fra Neo4j.
    Person-nodene og OOC-grafen er uberørt.

    `link_id` er Neo4j elementId på relasjonen.
    """
    slettet = await db_slett_etterforskningskobling(session, link_id)
    if not slettet:
        raise HTTPException(status_code=404, detail=f"Kobling {link_id} ble ikke funnet")
    logging.getLogger(bruker).info(f"Etterforskningskobling slettet: {link_id}")
    return {"melding": f"Kobling {link_id} ble slettet"}


# =========================================================
# ❤️ HELSESJEKK
# =========================================================

@app.get("/helse", tags=["System"])
async def helsesjekk():
    """Sjekker at applikasjonen kjører og at Neo4j er tilgjengelig."""
    try:
        await driver.verify_connectivity()
        return {"status": "ok", "database": "tilkoblet"}
    except Exception as e:
        return {"status": "feil", "database": str(e)}