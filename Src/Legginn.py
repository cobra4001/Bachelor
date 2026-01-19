import os
from json import JSONDecoder
from datetime import datetime
from neo4j import GraphDatabase

# ========================
# HJELPEFUNKSJONER
# ========================

def parse_share(value):
    """
    Konverterer en string eller int til float.
    Fjerner % hvis det finnes.
    Returnerer None hvis verdien er ugyldig.
    """
    if isinstance(value, str):
        value = value.strip().replace("%", "")
        try:
            return float(value)
        except ValueError:
            return None
    elif isinstance(value, (int, float)):
        return float(value)
    else:
        return None

# ========================
# KONFIG
# ========================
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "test1234")
FILE_PATH = r"F:\OpenOwnership full fil\register.json"

BATCH_SIZE = 2000
LOG_EVERY = 10000
# ========================

# ========================
# KOBLE TIL NEO4J
# ========================
driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()
print("✅ Connection to Neo4j established.")

if not os.path.exists(FILE_PATH):
    raise FileNotFoundError(FILE_PATH)

# ========================
# NDJSON PARSING
# ========================
def json_parse_lines(fileobj):
    for line_num, line in enumerate(fileobj, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            yield JSONDecoder().decode(line)
        except Exception as e:
            print(f"❌ ERROR parsing line {line_num}: {e}")

# ========================
# CONSTRAINTS
# ========================
def create_constraints(session):
    session.run("""
        CREATE CONSTRAINT person_ident IF NOT EXISTS
        FOR (p:Person) REQUIRE p.ident IS UNIQUE
    """)
    session.run("""
        CREATE CONSTRAINT firma_ident IF NOT EXISTS
        FOR (f:Firma) REQUIRE f.ident IS UNIQUE
    """)
    print("✅ Constraints created")

# ========================
# NODE IMPORT
# ========================
def import_persons(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (p:Person {ident: obj.personID})
        SET
            p.fullName      = COALESCE(obj.fullName, p.fullName),
            p.givenName     = COALESCE(obj.givenName, p.givenName),
            p.familyName    = COALESCE(obj.familyName, p.familyName),
            p.birthDate     = COALESCE(obj.birthDate, p.birthDate),
            p.type          = COALESCE(obj.type, p.type),
            p.personType    = COALESCE(obj.personType, p.personType),
            p.statementDate = COALESCE(obj.statementDate, p.statementDate)
    """, batch=batch)

def import_firmas(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (f:Firma {ident: obj.firmaID})
        SET
            f.name    = COALESCE(obj.name, f.name),
            f.address = COALESCE(obj.adress, f.address),
            f.county  = COALESCE(obj.county, f.county)
    """, batch=batch)

# ========================
# RELASJONER 
# ========================
def import_person_to_firma(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (p:Person {ident: obj.personID})
        MERGE (f:Firma  {ident: obj.firmaID})
        MERGE (p)-[r:TILFIRMA {statementID: obj.statementID}]->(f)

        SET
           r.statementDate = COALESCE(obj.statementDate, r.statementDate),
           r.share_minimum = obj.share_minimum,
           r.share_maximum = obj.share_maximum,
           r.share_exact   = obj.share_exact,
           r.startDate     = obj.startDate,
           r.endDate       = obj.endDate
    """, batch=batch)

def import_firma_to_firma(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (f1:Firma {ident: obj.fromFirmaID})
        MERGE (f2:Firma {ident: obj.toFirmaID})
        MERGE (f1)-[r:FIRMA_TIL_FIRMA {statementID: obj.statementID}]->(f2)
        SET
           r.statementDate = COALESCE(obj.statementDate, r.statementDate),
           r.share_minimum = obj.share_minimum,
           r.share_maximum = obj.share_maximum,
           r.share_exact   = obj.share_exact,
           r.startDate     = obj.startDate,
           r.endDate       = obj.endDate
    """, batch=batch)

# ========================
# IMPORT
# ========================
def stream_import():
    start_time = datetime.now()

    person_batch = []
    firma_batch = []
    rel_person_batch = []
    rel_firma_batch = []

    teller_total = 0
    count_persons = 0
    count_firmas = 0
    count_rel_person = 0
    count_rel_firma = 0

    with driver.session() as session:
        create_constraints(session)

    with open(FILE_PATH, "r", encoding="utf-8") as f:
        for obj in json_parse_lines(f):
            stype = obj.get("statementType")
            if not stype:
                continue

            try:
                # ---------------- PERSON ----------------
                if stype == "personStatement":
                    sid = obj.get("statementID")
                    names = obj.get("names", [])
                    name = names[0] if names else {}

                    person_batch.append({
                        "personID": sid,
                        "fullName": name.get("fullName"),
                        "givenName": name.get("givenName"),
                        "familyName": name.get("familyName"),
                        "type": name.get("type"),
                        "birthDate": obj.get("birthDate"),
                        "personType": obj.get("personType"),
                        "statementDate": obj.get("statementDate")
                    })

                    if len(person_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_persons, person_batch)
                        count_persons += len(person_batch)
                        person_batch.clear()

                # ---------------- FIRMA ----------------
                elif stype == "entityStatement":
                    sid = obj.get("statementID")
                    addresses = obj.get("addresses", [])
                    addr = addresses[0] if addresses else {}

                    firma_batch.append({
                        "firmaID": sid,
                        "name": obj.get("name"),
                        "adress": addr.get("address"),
                        "county": addr.get("country")
                    })

                    if len(firma_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_firmas, firma_batch)
                        count_firmas += len(firma_batch)
                        firma_batch.clear()

                # ---------------- RELASJONER ----------------
                elif stype == "ownershipOrControlStatement":
                    sid = obj.get("statementID")
                    statementDate = obj.get("statementDate")

                    # Hovedperson → firma relasjon
                    person_id = obj.get("interestedParty", {}).get("describedByPersonStatement")
                    firma_id = obj.get("subject", {}).get("describedByEntityStatement")
                    from_firma_id = obj.get("interestedParty", {}).get("describedByEntityStatement")

                    # ---------------- Hovedrelasjoner ----------------
                    if person_id and firma_id:
                        rel_person_batch.append({
                            "statementID": sid,
                            "personID": person_id,
                            "firmaID": firma_id,
                            "share_minimum": 0,
                            "share_maximum": 0,
                            "share_exact": 0,
                            "statementDate": statementDate
                        })

                    if from_firma_id and firma_id:
                        rel_firma_batch.append({
                            "statementID": sid,
                            "fromFirmaID": from_firma_id,
                            "toFirmaID": firma_id,
                            "share_minimum": 0,
                            "share_maximum": 0,
                            "share_exact": 0,
                            "statementDate": statementDate
                        })

                    # ---------------- Interests med aksjer ----------------
                    for interest in obj.get("interests", []):
                        share = interest.get("share", {})
                        share_min = parse_share(share.get("minimum")) or 0
                        share_max = parse_share(share.get("maximum")) or 0
                        share_ex  = parse_share(share.get("exact"))   or 0
                        startDate = interest.get("startDate") or None
                        endDate = interest.get("endDate") or None 

                        interest_subject = interest.get("subject", {})
                        interest_firma_id = interest_subject.get("describedByEntityStatement") or firma_id

                        # Person → Firma fra interest
                        if person_id and interest_firma_id:
                            rel_person_batch.append({
                                "statementID": sid,
                                "personID": person_id,
                                "firmaID": interest_firma_id,
                                "startDate": startDate or None,
                                "share_minimum": share_min if share_min is not None else 0,
                                "share_maximum": share_max if share_max is not None else 0,
                                "share_exact": share_ex if share_ex is not None else 0,
                                "endDate": endDate or None,
                                "statementDate": statementDate
                            })

                        # Firma → Firma fra interest
                        if from_firma_id and interest_firma_id:
                            rel_firma_batch.append({
                                "statementID": sid,
                                "fromFirmaID": from_firma_id,
                                "toFirmaID": interest_firma_id,
                                "startDate": startDate,
                                "share_minimum": share_min if share_min is not None else 0,     
                                "share_maximum": share_max if share_max is not None else 0,
                                "share_exact": share_ex if share_ex is not None else 0,
                                "endDate": endDate,
                                "statementDate": statementDate
                            })

                    if len(rel_person_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_person_to_firma, rel_person_batch)
                        count_rel_person += len(rel_person_batch)
                        rel_person_batch.clear()

                    if len(rel_firma_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_firma_to_firma, rel_firma_batch)
                        count_rel_firma += len(rel_firma_batch)
                        rel_firma_batch.clear()

                teller_total += 1

                if teller_total % LOG_EVERY == 0:
                    print(
                        f"{teller_total} objects | "
                        f"Δt: {datetime.now() - start_time}"
                    )
                    start_time = datetime.now()

            except Exception as e:
                print(f"🚨 ERROR at object {teller_total}: {e}")

    # ---------------- FLUSH ----------------
    with driver.session() as s:
        if person_batch:
            s.execute_write(import_persons, person_batch)
            count_persons += len(person_batch)
        if firma_batch:
            s.execute_write(import_firmas, firma_batch)
            count_firmas += len(firma_batch)
        if rel_person_batch:
            s.execute_write(import_person_to_firma, rel_person_batch)
            count_rel_person += len(rel_person_batch)
        if rel_firma_batch:
            s.execute_write(import_firma_to_firma, rel_firma_batch)
            count_rel_firma += len(rel_firma_batch)

    # ---------------- TOTAL ----------------
    print("🎉 Import ferdig!")
    print(f"Totalt prosessert objekter: {teller_total}")
    print(f"Person nodes: {count_persons}")
    print(f"Firma nodes: {count_firmas}")
    print(f"Person → Firma relasjoner: {count_rel_person}")
    print(f"Firma → Firma relasjoner: {count_rel_firma}")

# ========================
# KJØR
# ========================
stream_import()
