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
URI = "bolt://10.196.241.255:7687"
AUTH = ("neo4j", "mRTzP1@h")
FILE_PATH = r"F:\10 tusen\10000.json"

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


# ---------------- PERSONSTATEMENT ----------------
def import_person_statements(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (ps:PersonStatement {statementID: obj.statementID})
        SET
            ps.personType    = COALESCE(obj.personType, ps.personType),
            ps.statementType = COALESCE(obj.statementType, ps.statementType),
            ps.isComponent   = COALESCE(obj.isComponent, ps.isComponent),
            ps.statementDate = COALESCE(obj.statementDate, ps.statementDate),
            ps.pub_bodsVersion = COALESCE(obj.publicationDetails_bodsVersion, ps.pub_bodsVersion),
            ps.pub_license    = COALESCE(obj.publicationDetails_license, ps.pub_license),
            ps.pub_date       = COALESCE(obj.publicationDetails_publicationDate, ps.pub_date),
            ps.pub_publisher_name = COALESCE(obj.publicationDetails_publisher_name, ps.pub_publisher_name),
            ps.pub_publisher_url  = COALESCE(obj.publicationDetails_publisher_url, ps.pub_publisher_url)
    """, batch=batch)


# ---------------- PERSON ----------------
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
            p.CountryCode   = COALESCE(obj.Countrycode, p.CountryCode),
            p.Country       = COALESCE(obj.Country, p.Country)
    """, batch=batch)

# ---------------- Person Adresse ----------------
def import_person_addresses(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (a:Adresse {address: obj.address, country: obj.country, type: obj.type})
    """, batch=batch)


# ---------------- FIRMASTATEMENT ----------------
def import_firma_statements(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (fs:FirmaStatement {statementID: obj.statementID})
        SET
            fs.replacesStatements = COALESCE(obj.replacesStatements, fs.replacesStatements),
            fs.statementType   = COALESCE(obj.statementType, fs.statementType),
            fs.isComponent = COALESCE(obj.isComponent, fs.isComponent),
            fs.statementDate = COALESCE(obj.statementDate, fs.statementDate),
            fs.pub_bodsVersion = COALESCE(obj.publicationDetails_bodsVersion, fs.pub_bodsVersion),
            fs.pub_license    = COALESCE(obj.publicationDetails_license, fs.pub_license),
            fs.pub_date       = COALESCE(obj.publicationDetails_publicationDate, fs.pub_date),
            fs.pub_publisher_name = COALESCE(obj.publicationDetails_publisher_name, fs.pub_publisher_name),
            fs.pub_publisher_url  = COALESCE(obj.publicationDetails_publisher_url, fs.pub_publisher_url)
    """, batch=batch)

# ---------------- Firma ----------------
def import_firmas(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (f:Firma {ident: obj.firmaID})
        SET
            f.name    = COALESCE(obj.name, f.name),
            f.alternateNames = COALESCE(obj.alternateNames, f.alternateNames),
            f.entityType = COALESCE(obj.entityType, f.entityType),  
            f.foundingDate = COALESCE(obj.foundingDate, f.foundingDate),
            f.incorporatedInJurisdiction_name = COALESCE(obj.incorporatedInJurisdiction_name, f.incorporatedInJurisdiction_name),
            f.incorporatedInJurisdiction_code = COALESCE(obj.incorporatedInJurisdiction_code, f.incorporatedInJurisdiction_code),
            f.dissolutionDate = COALESCE(obj.dissolutionDate, f.dissolutionDate)
    """, batch=batch)

# ---------------- OOCStatement ----------------
def import_OOC_statements(tx, batch): 
    tx.run("""
        UNWIND $batch AS obj
        MERGE (os:OOCStatement {statementID: obj.statementID})   
        SET
            os.statementType = COALESCE(obj.statementType, os.statementType),
            os.statementDate = COALESCE(obj.statementDate, os.statementDate),
            os.isComponent   = COALESCE(obj.isComponent, os.isComponent),
            os.interestedParty_describedByPersonStatement = COALESCE(obj.interestedParty_describedByPersonStatement, os.interestedParty_describedByPersonStatement),
            os.pub_bodsVersion = COALESCE(obj.publicationDetails_bodsVersion, os.pub_bodsVersion),
            os.pub_license    = COALESCE(obj.publicationDetails_license, os.pub_license),
            os.pub_date       = COALESCE(obj.publicationDetails_publicationDate, os.pub_date),
            os.pub_publisher_name = COALESCE(obj.publicationDetails_publisher_name, os.pub_publisher_name),
            os.pub_publisher_url  = COALESCE(obj.publicationDetails_publisher_url, os.pub_publisher_url)
    """, batch=batch)

# ---------------- Firma adresse ----------------
def import_firma_addresses(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (a:Adresse {address: obj.address, country: obj.country, type: obj.type})
    """, batch=batch)

# ========================
# RELASJONER 
# ========================

# --------------Person_interested_in_Firma ----------------
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
           r.endDate       = obj.endDate,
           r.type          = obj.type
    """, batch=batch)

# ----------------Firma_Interessert_i_Firma ----------------
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
           r.endDate       = obj.endDate,
           r.type          = obj.type
    """, batch=batch)

# ========================
# IMPORT
# ========================
def stream_import():
    start_time = datetime.now()
    personStatement_batch = []
    person_batch = []
    personaddress_batch = []
    firmaStatement_batch = []
    firma_batch = []
    firmaaddress_batch = []
    OOCStatement_batch = []
    rel_person_batch = []
    rel_firma_batch = []

    teller_total = 0

    with driver.session() as session:
        create_constraints(session)

    with open(FILE_PATH, "r", encoding="utf-8") as f:
        for obj in json_parse_lines(f):
            stype = obj.get("statementType")
            if not stype:
                continue

            try:
                # ---------------- PERSONSTATEMENT ----------------
                if stype == "personStatement":
                    sid = obj.get("statementID")

                    personStatement_batch.append({
                        "statementID": sid,
                        "personType": obj.get("personType"),
                        "statementType": obj.get("statementType"),
                        "isComponent": obj.get("isComponent"),
                        "replacesStatements": obj.get("replacesStatements"),
                        "statementDate": obj.get("statementDate"),
                        "publicationDetails_bodsVersion": obj.get("publicationDetails", {}).get("bodsVersion"),
                        "publicationDetails_license": obj.get("publicationDetails", {}).get("license"),
                        "publicationDetails_publicationDate": obj.get("publicationDetails", {}).get("publicationDate"),
                        "publicationDetails_publisher_name": obj.get("publicationDetails", {}).get("publisher", {}).get("name"),
                        "publicationDetails_publisher_url": obj.get("publicationDetails", {}).get("publisher", {}).get("url")
                    })

                # ---------------- PERSON ----------------
                    names = obj.get("names", [])
                    name = names[0] if names else {}
                    nationalites = obj.get("nationalities", [])
                    national = nationalites[0] if nationalites else {}

                    person_batch.append({
                        "personID": sid,
                        "fullName": name.get("fullName"),
                        "givenName": name.get("givenName"),
                        "familyName": name.get("familyName"),
                        "type": name.get("type"),
                        "birthDate": obj.get("birthDate"),
                        "personType": obj.get("personType"),
                        "Countrycode": national.get("code"),
                        "Country": national.get("name")
                    })

                    # ---------------- Person adresse ----------------
                    addresses = obj.get("addresses", [])
                    for addr in addresses:
                        if not addr.get("address"):
                            continue
                        personaddress_batch.append({
                            "personID": sid,
                            "address": addr.get("address"),
                            "country": addr.get("country"),
                            "type": addr.get("type"),
                        })

                    # Flush personstatement batch
                    if len(personStatement_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_person_statements, personStatement_batch)
                        personStatement_batch.clear()

                    # Flush person batch
                    if len(person_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_persons, person_batch)
                        person_batch.clear()

                    # Flush person addresses
                    if len(personaddress_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_person_addresses, personaddress_batch)
                        personaddress_batch.clear()

                # ---------------- FIRMA ----------------
                elif stype == "entityStatement":
                    sid = obj.get("statementID")
                
                    # ---------------- FIRMASTATEMENT ----------------
                    firmaStatement_batch.append({
                        "statementID": sid,
                        "replacesStatements": obj.get("replacesStatements"),
                        "statementType": obj.get("statementType"),
                        "isComponent": obj.get("isComponent"),
                        "statementDate": obj.get("statementDate"),
                        "publicationDetails_bodsVersion": obj.get("publicationDetails", {}).get("bodsVersion"),
                        "publicationDetails_license": obj.get("publicationDetails", {}).get("license"),
                        "publicationDetails_publicationDate": obj.get("publicationDetails", {}).get("publicationDate"),
                        "publicationDetails_publisher_name": obj.get("publicationDetails", {}).get("publisher", {}).get("name"),
                        "publicationDetails_publisher_url": obj.get("publicationDetails", {}).get("publisher", {}).get("url")
                    })


                      # ---------------- FIRMA ----------------
                    firma_batch.append({
                        "firmaID": sid,
                        "name": obj.get("name"),
                        "alternateNames": obj.get("alternateNames"),
                        "entityType": obj.get("entityType"),
                        "foundingDate": obj.get("foundingDate"),
                        "incorporatedInJurisdiction_name": obj.get("incorporatedInJurisdiction", {}).get("name"),
                        "incorporatedInJurisdiction_code": obj.get("incorporatedInJurisdiction", {}).get("code"),
                        "dissolutionDate": obj.get("dissolutionDate")
                    })

                    # ---------------- FIRMA adresse ----------------
                    addresses = obj.get("addresses", [])
                    for addr in addresses:
                        if not addr.get("address"):
                            continue
                        firmaaddress_batch.append({
                            "firmaID": sid,
                            "address": addr.get("address"),
                            "country": addr.get("country"),
                            "type": addr.get("type"),
                        })

                    # Flush firmastatement batch
                    if len(firmaStatement_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_firma_statements, firmaStatement_batch)
                        firmaStatement_batch.clear()

                    # Flush firma batch
                    if len(firma_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_firmas, firma_batch)
                        firma_batch.clear()

                    # Flush firma addresses
                    if len(firmaaddress_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_firma_addresses, firmaaddress_batch)
                        firmaaddress_batch.clear()


                # ---------------- OOCStatement ----------------
                elif stype == "ownershipOrControlStatement":
                    sid = obj.get("statementID")

                    OOCStatement_batch.append({
                        "statementID": sid,
                        "statementType": obj.get("statementType"),
                        "statementDate": obj.get("statementDate"),
                        "isComponent": obj.get("isComponent"),
                        "interestedParty_describedByPersonStatement": obj.get("interestedParty", {}).get("describedByPersonStatement"),
                        "publicationDetails_bodsVersion": obj.get("publicationDetails", {}).get("bodsVersion"),
                        "publicationDetails_license": obj.get("publicationDetails", {}).get("license"),
                        "publicationDetails_publicationDate": obj.get("publicationDetails", {}).get("publicationDate"),
                        "publicationDetails_publisher_name": obj.get("publicationDetails", {}).get("publisher", {}).get("name"),
                        "publicationDetails_publisher_url": obj.get("publicationDetails", {}).get("publisher", {}).get("url")
                    })

                    # Flush OOCStatement batch
                    if len(OOCStatement_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_OOC_statements, OOCStatement_batch)
                        OOCStatement_batch.clear()

                # ---------------- RELASJONER ----------------
                elif stype == "ownershipOrControlStatement":
                    sid = obj.get("statementID")
                    statementDate = obj.get("statementDate")
                    person_id = obj.get("interestedParty", {}).get("describedByPersonStatement")
                    firma_id = obj.get("subject", {}).get("describedByEntityStatement")
                    from_firma_id = obj.get("interestedParty", {}).get("describedByEntityStatement")

                    # Person → Firma relasjon
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

                    # Firma → Firma relasjon
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

                    # Interests
                    for interest in obj.get("interests", []):
                        share = interest.get("share", {})
                        share_min = parse_share(share.get("minimum")) or 0
                        share_max = parse_share(share.get("maximum")) or 0
                        share_ex  = parse_share(share.get("exact")) or 0
                        startDate = interest.get("startDate")
                        endDate = interest.get("endDate") 
                        type = interest.get("type") 
                        interest_subject = interest.get("subject", {})
                        interest_firma_id = interest_subject.get("describedByEntityStatement") or firma_id

                        if person_id and interest_firma_id:
                            rel_person_batch.append({
                                "statementID": sid,
                                "personID": person_id,
                                "firmaID": interest_firma_id,
                                "startDate": startDate,
                                "share_minimum": share_min,
                                "share_maximum": share_max,
                                "share_exact": share_ex,
                                "endDate": endDate,
                                "type": type,
                                "statementDate": statementDate
                            })
                        if from_firma_id and interest_firma_id:
                            rel_firma_batch.append({
                                "statementID": sid,
                                "fromFirmaID": from_firma_id,
                                "toFirmaID": interest_firma_id,
                                "startDate": startDate,
                                "share_minimum": share_min,     
                                "share_maximum": share_max,
                                "share_exact": share_ex,
                                "endDate": endDate,
                                "type": type,
                                "statementDate": statementDate
                            })

                    # Flush relasjoner
                    if len(rel_person_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_person_to_firma, rel_person_batch)
                        rel_person_batch.clear()
                    if len(rel_firma_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_firma_to_firma, rel_firma_batch)
                        rel_firma_batch.clear()

                teller_total += 1

                if teller_total % LOG_EVERY == 0:
                    print(f"{teller_total} objects | Δt: {datetime.now() - start_time}")
                    start_time = datetime.now()

            except Exception as e:
                print(f"🚨 ERROR at object {teller_total}: {e}")

    # ---------------- FLUSH REST ----------------
    with driver.session() as s:
        if person_batch:
            s.execute_write(import_persons, person_batch)
        if personStatement_batch:
            s.execute_write(import_person_statements, personStatement_batch)
        if personaddress_batch:
            s.execute_write(import_person_addresses, personaddress_batch)
        if firmaStatement_batch:
            s.execute_write(import_firma_statements, firmaStatement_batch)    
        if firma_batch:
            s.execute_write(import_firmas, firma_batch)
        if firmaaddress_batch:
            s.execute_write(import_firma_addresses, firmaaddress_batch)
        if OOCStatement_batch:
            s.execute_write(import_OOC_statements, OOCStatement_batch)    
        if rel_person_batch:
            s.execute_write(import_person_to_firma, rel_person_batch)
        if rel_firma_batch:
            s.execute_write(import_firma_to_firma, rel_firma_batch)

    # ---------------- RETURN TELLERE ----------------
    with driver.session() as s:
        personstatement_count = s.run("MATCH (ps:PersonStatement) RETURN count(ps) AS c").single()["c"]
        person_count = s.run("MATCH (p:Person) RETURN count(p) AS c").single()["c"]
        firmastatement_count = s.run("MATCH (fs:FirmaStatement) RETURN count(fs) AS c").single()["c"]
        firma_count = s.run("MATCH (f:Firma) RETURN count(f) AS c").single()["c"]
        address_count = s.run("MATCH (a:Adresse) RETURN count(a) AS c").single()["c"]
        ownershiporcontrolstatement_count = s.run("MATCH (o:OOCStatement) RETURN count(o) AS c").single()["c"]

    return teller_total,personstatement_count, person_count, firmastatement_count, firma_count, address_count, ownershiporcontrolstatement_count


# ========================
# KJØR
# ========================
teller_total, ps, fs, ooc, p, f, a, ooc = stream_import()

print("🎉 Import ferdig!")
print(f"Totalt prosessert objekter: {teller_total}")
print(f"PersonStatement-noder: {ps}")
print(f"FirmaStatement-noder: {fs}")
print(f"OOCStatement-noder: {ooc}")
print(f"Person-noder: {p}")
print(f"Firma-noder: {f}")
print(f"Adresse-noder: {a}")
