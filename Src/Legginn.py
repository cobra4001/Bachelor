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
           
        // Lag relasjon til Person
        WITH ps, obj WHERE obj.personID IS NOT NULL   
        MERGE (p:Person {ident: obj.personID})
        MERGE (ps)-[:DESCRIBES_PERSON]->(p) 
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
        MERGE (a:Address {address: obj.address, country: obj.country, type: obj.type})
           
        // Lag relasjon til Person hvis personID finnes
        WITH a, obj WHERE obj.personID IS NOT NULL
        MERGE (p:Person {ident: obj.personID})
        MERGE (p)-[:HAS_ADDRESS]->(a)  
    """, batch=batch)

# ---------------- Person Kilde ----------------

def import_person_sources(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        // Bare merge hvis url finnes
        WITH obj WHERE obj.url IS NOT NULL
        MERGE (so:Source {url: obj.url})
        SET
            so.assertedBy  = COALESCE(obj.assertedBy, so.assertedBy),
            so.description = COALESCE(obj.description, so.description),
            so.retrievedAt = COALESCE(obj.retrievedAt, so.retrievedAt),
            so.type        = COALESCE(obj.type, so.type),
            so.url         = COALESCE(obj.url, so.url)
        // Relasjon til statement
        MERGE (ps:PersonStatement {statementID: obj.statementID})
        MERGE (ps)-[:HAS_SOURCE]->(so)
    """, batch=batch)


# ---------------- Person identifier ----------------
def import_person_identifiers(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (i:Identifier {uri: obj.uri})
        SET
            i.id = COALESCE(obj.id, i.id),
            i.schemeName = COALESCE(obj.schemeName, i.schemeName),
            i.uri = COALESCE(obj.uri, i.uri)
        // Relasjon til statement
        MERGE (ps:PersonStatement {statementID: obj.statementID})
        MERGE (ps)-[:HAS_IDENTIFIER]->(i)
    """, batch=batch)


# ---------------- FIRMASTATEMENT ----------------
def import_entity_statements(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (fs:EntityStatement {statementID: obj.statementID})
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
           
        // Lag relasjon til Entity
        WITH fs, obj WHERE obj.EntityID IS NOT NULL
        MERGE (f:Entity {ident: obj.EntityID})
        MERGE (fs)-[:DESCRIBES_ENTITY]->(f)
    """, batch=batch)

# ---------------- Firma ----------------
def import_entitys(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (f:Entity {ident: obj.EntityID})
        SET
            f.name    = COALESCE(obj.name, f.name),
            f.alternateNames = COALESCE(obj.alternateNames, f.alternateNames),
            f.entityType = COALESCE(obj.entityType, f.entityType),  
            f.foundingDate = COALESCE(obj.foundingDate, f.foundingDate),
            f.incorporatedInJurisdiction_name = COALESCE(obj.incorporatedInJurisdiction_name, f.incorporatedInJurisdiction_name),
            f.incorporatedInJurisdiction_code = COALESCE(obj.incorporatedInJurisdiction_code, f.incorporatedInJurisdiction_code),
            f.dissolutionDate = COALESCE(obj.dissolutionDate, f.dissolutionDate)
    """, batch=batch)

# ---------------- Firma adresse ----------------
def import_entity_addresses(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (a:Address {address: obj.address, country: obj.country, type: obj.type})

        // Lag relasjon til Firma hvis EntityID finnes
        WITH a, obj WHERE obj.EntityID IS NOT NULL
        MERGE (f:Entity {ident: obj.EntityID})
        MERGE (f)-[:HAS_ADDRESS]->(a)    
    """, batch=batch)

# ---------------- Firma Kilde ----------------
def import_entity_sources(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        // Bare merge hvis url finnes
        WITH obj WHERE obj.url IS NOT NULL
        MERGE (so:Source {url: obj.url})
        SET
            so.assertedBy  = COALESCE(obj.assertedBy, so.assertedBy),
            so.description = COALESCE(obj.description, so.description),
            so.retrievedAt = COALESCE(obj.retrievedAt, so.retrievedAt),
            so.type        = COALESCE(obj.type, so.type),
            so.url         = COALESCE(obj.url, so.url)
        // Relasjon til statement
        MERGE (fs:EntityStatement {statementID: obj.statementID})
        MERGE (fs)-[:HAS_SOURCE]->(so)
    """, batch=batch)

# ---------------- Firma Identifier ----------------
def import_entity_identifiers(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (i:Identifier {uri: obj.uri})
        SET
            i.id = COALESCE(obj.id, i.id),
            i.scheme = COALESCE(obj.scheme, i.scheme),
            i.schemeName = COALESCE(obj.schemeName, i.schemeName),
            i.uri = COALESCE(obj.uri, i.uri)
        // Relasjon til statement
        MERGE (fs:EntityStatement {statementID: obj.statementID})
        MERGE (fs)-[:HAS_IDENTIFIER]->(i)  

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

# ---------------- OOCStatement kilde ----------------
def import_OOC_sources(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        // Bare merge hvis url finnes
        WITH obj WHERE obj.url IS NOT NULL
        MERGE (so:Source {url: obj.url})
        SET
            so.assertedBy  = COALESCE(obj.assertedBy, so.assertedBy),
            so.description = COALESCE(obj.description, so.description),
            so.retrievedAt = COALESCE(obj.retrievedAt, so.retrievedAt),
            so.type        = COALESCE(obj.type, so.type),
            so.url         = COALESCE(obj.url, so.url)
        // Relasjon til statement
        MERGE (os:OOCStatement {statementID: obj.statementID})
        MERGE (os)-[:HAS_SOURCE]->(so)
    """, batch=batch)

# ---------------- OOCStatement Identifier ----------------
def import_OOC_identifiers(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (i:Identifier {uri: obj.uri})
        SET
            i.id = COALESCE(obj.id, i.id),
            i.schemeName = COALESCE(obj.schemeName, i.schemeName),
            i.scheme = COALESCE(obj.scheme, i.scheme),
            i.uri = COALESCE(obj.uri, i.uri)
        WITH i, obj
        MATCH (os:OOCStatement {statementID: obj.statementID})
        MERGE (os)-[:HAS_IDENTIFIER]->(i)
    """, batch=batch)

# ---------------- Person → OOCStatement ----------------
def import_person_to_OOC(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MATCH (p:Person {ident: obj.personID})
        MATCH (os:OOCStatement {statementID: obj.oocID})
        MERGE (p)-[:IS_INTERESTED_PARTY]->(os)
    """, batch=batch)

# ---------------- Firma → OOCStatement ----------------
def import_firma_to_OOC(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MATCH (f:Entity {ident: obj.firmaID})
        MATCH (os:OOCStatement {statementID: obj.oocID})
        MERGE (f)-[:IS_INTERESTED_PARTY]->(os)
    """, batch=batch)
    # Person → OOCStatement → Firma
def import_person_to_OOC_interest(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MATCH (p:Person {ident: obj.personID})
        MATCH (os:OOCStatement {statementID: obj.oocID})
        MERGE (p)-[:IS_INTERESTED_PARTY]->(os)
        WITH os, obj
        MATCH (f:Entity {ident: obj.firmaID})
        MERGE (os)-[r:INTEREST_IN]->(f)
        SET r.share_minimum = obj.share_minimum,
            r.share_maximum = obj.share_maximum,
            r.share_exact = obj.share_exact,
            r.startDate = obj.startDate,
            r.endDate = obj.endDate,
            r.type = obj.type,
            r.statementDate = obj.statementDate
    """, batch=batch)

# Firma → OOCStatement → Firma
def import_firma_to_OOC_interest(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MATCH (f1:Entity {ident: obj.firmaID})
        MATCH (os:OOCStatement {statementID: obj.oocID})
        MERGE (f1)-[:IS_INTERESTED_PARTY]->(os)
        WITH os, obj
        MATCH (f2:Entity {ident: obj.toFirmaID})
        MERGE (os)-[r:INTEREST_IN]->(f2)
        SET r.share_minimum = obj.share_minimum,
            r.share_maximum = obj.share_maximum,
            r.share_exact = obj.share_exact,
            r.startDate = obj.startDate,
            r.endDate = obj.endDate,
            r.type = obj.type,
            r.statementDate = obj.statementDate
    """, batch=batch)

    # Firma → OOCStatement → Firma
def import_firma_to_OOC_interest(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MATCH (f1:Entity {ident: obj.firmaID})
        MATCH (os:OOCStatement {statementID: obj.oocID})
        MERGE (f1)-[:IS_INTERESTED_PARTY]->(os)
        WITH os, obj
        MATCH (f2:Entity {ident: obj.toFirmaID})
        MERGE (os)-[r:INTEREST_IN]->(f2)
        SET r.share_minimum = obj.share_minimum,
            r.share_maximum = obj.share_maximum,
            r.share_exact = obj.share_exact,
            r.startDate = obj.startDate,
            r.endDate = obj.endDate,
            r.type = obj.type,
            r.statementDate = obj.statementDate
    """, batch=batch)



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
    personsource_batch = []
    personidentifier_batch = []
    entityStatement_batch = []
    entity_batch = []
    entityaddress_batch = []
    entitysource_batch = []
    entityidentifier_batch = []
    OOCStatement_batch = []
    OOCStatementsource_batch = []
    OOCidentifier_batch = []
    person_to_OOC_batch = []
    firma_to_OOC_batch = []

    
    
   

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
                        "publicationDetails_publisher_url": obj.get("publicationDetails", {}).get("publisher", {}).get("url"),
                        "personID": sid # kobling til personen
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
                            "personID": sid,   # <- koble adressen til personen
                            "address": addr.get("address"),
                            "country": addr.get("country"),
                            "type": addr.get("type"),
                        })

                    # ---------------- PersonStatement kilde ----------------
                    source = obj.get("source")
                    if source and source.get("url"):
                        personsource_batch.append({
                            "statementID": sid,  # kobling til statementen
                            "assertedBy": source.get("assertedBy"),
                            "description": source.get("description"),
                            "retrievedAt": source.get("retrievedAt"),
                            "type": source.get("type"),
                            "url": source.get("url"),
                        })

                    # ---------------- PersonStatement identifier ----------------
                    identifiers = obj.get("identifiers", [])
                    for ident in identifiers:
                        if ident.get("uri"):  # bare ta med de som har URI
                            personidentifier_batch.append({
                                "statementID": sid, # kobling til statementen
                                "id": ident.get("id"),
                                "schemeName": ident.get("schemeName"),
                                "uri": ident.get("uri"),
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

                    # Flush person sources
                    if len(personsource_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_person_sources, personsource_batch)
                        personsource_batch.clear()    

               
                    # ---------------- FIRMASTATEMENT ----------------

                elif stype == "entityStatement":
                    sid = obj.get("statementID")

                    entityStatement_batch.append({
                        "statementID": sid,
                        "replacesStatements": obj.get("replacesStatements"),
                        "statementType": obj.get("statementType"),
                        "isComponent": obj.get("isComponent"),
                        "statementDate": obj.get("statementDate"),
                        "publicationDetails_bodsVersion": obj.get("publicationDetails", {}).get("bodsVersion"),
                        "publicationDetails_license": obj.get("publicationDetails", {}).get("license"),
                        "publicationDetails_publicationDate": obj.get("publicationDetails", {}).get("publicationDate"),
                        "publicationDetails_publisher_name": obj.get("publicationDetails", {}).get("publisher", {}).get("name"),
                        "publicationDetails_publisher_url": obj.get("publicationDetails", {}).get("publisher", {}).get("url"),
                        "EntityID": sid # kobling til firma
                    })


                      # ---------------- FIRMA ----------------
                    entity_batch.append({
                        
                        "EntityID": sid,
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
                        entityaddress_batch.append({
                            "EntityID": sid,  # <- koble adressen til firmaet
                            "address": addr.get("address"),
                            "country": addr.get("country"),
                            "type": addr.get("type"),
                        })

                    # ---------------- FirmaStatement kilde ----------------

                    source = obj.get("source")

                    if source and source.get("url"):
                        entitysource_batch.append({
                            "statementID": sid,
                            "assertedBy": source.get("assertedBy"),
                            "description": source.get("description"),
                            "retrievedAt": source.get("retrievedAt"),
                            "type": source.get("type"),
                            "url": source.get("url"),
                        })



                    # ---------------- FirmaStatement identifier ----------------

                    identifiers = obj.get("identifiers", [])
                    for ident in identifiers:
                        if ident.get("uri"):  # bare ta med de som har URI
                            entityidentifier_batch.append({
                                "statementID": sid,
                                "id": ident.get("id"),
                                "scheme": ident.get("scheme"),
                                "schemeName": ident.get("schemeName"),
                                "uri": ident.get("uri"),
                            })

                    # Flush firmastatement batch
                    if len(entityStatement_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_entity_statements, entityStatement_batch)
                        entityStatement_batch.clear()

                    # Flush firma batch
                    if len(entity_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_entitys, entity_batch)
                        entity_batch.clear()

                    # Flush firma addresses
                    if len(entityaddress_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_entity_addresses, entityaddress_batch)
                        entityaddress_batch.clear()

                    # Flush firma sources
                    if len(entitysource_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_entity_sources, entitysource_batch)
                        entitysource_batch.clear()

                # ---------------- OOCStatement ----------------
                elif stype == "ownershipOrControlStatement":
                    sid = obj.get("statementID")
                    statementDate = obj.get("statementDate")

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
                    # Person → OOC
                    person_node_id = obj.get("interestedParty", {}).get("describedByPersonStatement")
                    if person_node_id:
                        person_to_OOC_batch.append({
                            "personID": person_node_id,  # dette er Person.ident (statementID)
                            "oocID": sid                  # OOCStatement.statementID
                        })

                    # Firma → OOC
                    firma_node_id = obj.get("subject", {}).get("describedByEntityStatement")
                    if firma_node_id:
                        firma_to_OOC_batch.append({
                            "firmaID": firma_node_id,     # dette er Entity.ident (statementID)
                            "oocID": sid
                        })
                    # ---------------- OOCStatement → Firma med aksjer ----------------
                    rel_person_to_OOC_batch = []
                    rel_firma_to_OOC_batch = []
 
                    # Hvem er interessert i dette OOCStatement?
                    person_node_id = obj.get("interestedParty", {}).get("describedByPersonStatement")
                    from_firma_id = obj.get("interestedParty", {}).get("describedByEntityStatement")
                    
                    # Hvem er subject i dette OOCStatement?
                    firma_id = obj.get("subject", {}).get("describedByEntityStatement")
                    
                    # Lag relasjon fra person/firma til OOCStatement
                    if person_node_id:
                        person_to_OOC_batch.append({
                            "personID": person_node_id,
                            "oocID": sid
                        })
                    if from_firma_id:
                        firma_to_OOC_batch.append({
                            "firmaID": from_firma_id,
                            "oocID": sid
                        })
                    
                    # Interests → OOCStatement → Firma
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
                        
                        # Person → OOCStatement → Firma
                        if person_node_id and interest_firma_id:
                            rel_person_to_OOC_batch.append({
                                "personID": person_node_id,   # Person.ident
                                "oocID": sid,                 # OOCStatement.statementID
                                "firmaID": interest_firma_id, # Firma.ident
                                "startDate": startDate,
                                "share_minimum": share_min,
                                "share_maximum": share_max,
                                "share_exact": share_ex,
                                "endDate": endDate,
                                "type": type,
                                "statementDate": statementDate
                            })
                        
                        # Firma → OOCStatement → Firma
                        if from_firma_id and interest_firma_id:
                            rel_firma_to_OOC_batch.append({
                                "firmaID": from_firma_id,      # Firma.ident
                                "oocID": sid,                  # OOCStatement.statementID
                                "toFirmaID": interest_firma_id,# Firma.ident
                                "startDate": startDate,
                                "share_minimum": share_min,
                                "share_maximum": share_max,
                                "share_exact": share_ex,
                                "endDate": endDate,
                                "type": type,
                                "statementDate": statementDate
                            })
                    # ---------------- OOCStatement kilde ----------------

                    source = obj.get("source")

                    if source and source.get("url"):
                        OOCStatementsource_batch.append({
                            "statementID": sid,
                            "assertedBy": source.get("assertedBy"),
                            "description": source.get("description"),
                            "retrievedAt": source.get("retrievedAt"),
                            "type": source.get("type"),
                            "url": source.get("url"),
                        })

                    # ---------------- OOCStatement identifier ----------------
                    identifiers = obj.get("identifiers", [])
                    for ident in identifiers:
                        if ident.get("uri"):  # bare ta med de som har URI
                            OOCidentifier_batch.append({
                                "statementID": sid,
                                "id": ident.get("id"),
                                "scheme": ident.get("scheme"),
                                "schemeName": ident.get("schemeName"),
                                "uri": ident.get("uri"),
                                })
                            
                    # Flush OOCStatement batch
                    if len(OOCStatement_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_OOC_statements, OOCStatement_batch)
                        OOCStatement_batch.clear()

                    # Flush OOCStatement kilde
                    if len(OOCStatementsource_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_OOC_sources, OOCStatementsource_batch)
                        OOCStatementsource_batch.clear()

                    # Flush OOCStatement identifier batch
                    if len(OOCidentifier_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_OOC_identifiers, OOCidentifier_batch)
                        OOCidentifier_batch.clear()

                    # Flush Person → OOC relasjon
                    if len(person_to_OOC_batch) >= BATCH_SIZE:
                           with driver.session() as s:
                               s.execute_write(import_person_to_OOC, person_to_OOC_batch)
                           person_to_OOC_batch.clear()

                    # Flush Firma → OOC relasjon    
                    if len(firma_to_OOC_batch) >= BATCH_SIZE:
                           with driver.session() as s:
                               s.execute_write(import_firma_to_OOC, firma_to_OOC_batch)
                           firma_to_OOC_batch.clear()
                        
                    # Flush relasjoner når batch-size nås
                    if len(rel_person_to_OOC_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_person_to_OOC_interest, rel_person_to_OOC_batch)
                        rel_person_to_OOC_batch.clear()
                        
                    if len(rel_firma_to_OOC_batch) >= BATCH_SIZE:
                        with driver.session() as s:
                            s.execute_write(import_firma_to_OOC_interest, rel_firma_to_OOC_batch)
                        rel_firma_to_OOC_batch.clear()


                teller_total += 1

                if teller_total % LOG_EVERY == 0:
                    print(f"{teller_total} objects | Δt: {datetime.now() - start_time}")
                    start_time = datetime.now()

            except Exception as e:
                print(f"🚨 ERROR at object {teller_total}: {e}")

    # ---------------- FLUSH REST ----------------
    with driver.session() as s:
        if personStatement_batch:
            s.execute_write(import_person_statements, personStatement_batch)
        if person_batch:
            s.execute_write(import_persons, person_batch)
        if personaddress_batch:
            s.execute_write(import_person_addresses, personaddress_batch)
        if entityStatement_batch:
            s.execute_write(import_entity_statements, entityStatement_batch)
        if personsource_batch:
            s.execute_write(import_person_sources, personsource_batch)
        if personidentifier_batch:
            s.execute_write(import_person_identifiers, personidentifier_batch)      
        if entity_batch:
            s.execute_write(import_entitys, entity_batch)
        if entityaddress_batch:
            s.execute_write(import_entity_addresses, entityaddress_batch)
        if entitysource_batch:
            s.execute_write(import_entity_sources, entitysource_batch)    
        if entityidentifier_batch:
            s.execute_write(import_entity_identifiers, entityidentifier_batch)   
        if OOCStatement_batch:
            s.execute_write(import_OOC_statements, OOCStatement_batch)
        if OOCStatementsource_batch:
            s.execute_write(import_OOC_sources, OOCStatementsource_batch)
        if OOCidentifier_batch:
            s.execute_write(import_OOC_identifiers, OOCidentifier_batch)
        if person_to_OOC_batch:
            s.execute_write(import_person_to_OOC, person_to_OOC_batch)
        if firma_to_OOC_batch:
            s.execute_write(import_firma_to_OOC, firma_to_OOC_batch)
        if rel_person_to_OOC_batch:
            s.execute_write(import_person_to_OOC_interest, rel_person_to_OOC_batch)  
        if rel_firma_to_OOC_batch:
            s.execute_write(import_firma_to_OOC_interest, rel_firma_to_OOC_batch)                                
        

    # ---------------- RETURN TELLERE ----------------
    with driver.session() as s:
        personstatement_count = s.run("MATCH (ps:PersonStatement) RETURN count(ps) AS c").single()["c"]
        entitystatement_count = s.run("MATCH (fs:EntityStatement) RETURN count(fs) AS c").single()["c"]
        ownershiporcontrolstatement_count = s.run("MATCH (os:OOCStatement) RETURN count(os) AS c").single()["c"]
        person_count = s.run("MATCH (p:Person) RETURN count(p) AS c").single()["c"]
        entity_count = s.run("MATCH (f:Entity) RETURN count(f) AS c").single()["c"]
        address_count = s.run("MATCH (a:Address) RETURN count(a) AS c").single()["c"]
        identifier_count = s.run("MATCH (i:Identifier) RETURN count(i) AS c").single()["c"]
        source_count = s.run("MATCH (so:Source) RETURN count(so) AS c").single()["c"]
        
        

    

# ---------------- RETURN RELASJONER ----------------
        Has_Identifier_count = s.run("""MATCH ()-[r:HAS_IDENTIFIER]->() RETURN count(r) AS c""").single()["c"]
        Has_Source_count = s.run("""MATCH ()-[r:HAS_SOURCE]->() RETURN count(r) AS c""").single()["c"]
        Describes_Person_count = s.run("""MATCH ()-[r:DESCRIBES_PERSON]->() RETURN count(r) AS c""").single()["c"]
        Describes_Entity_count = s.run("""MATCH ()-[r:DESCRIBES_ENTITY]->() RETURN count(r) AS c""").single()["c"]
        hasadress_count = s.run("MATCH ()-[r:HAS_ADDRESS]->() RETURN count(r) AS c").single()["c"]
        is_interested_party_count = s.run("MATCH ()-[r:IS_INTERESTED_PARTY]->() RETURN count(r) AS c").single()["c"]
        interested_in_count = s.run("MATCH ()-[r:INTEREST_IN]->() RETURN count(r) AS c").single()["c"]



    return (
        teller_total,
        personstatement_count,
        entitystatement_count,
        ownershiporcontrolstatement_count,
        person_count,
        entity_count,
        address_count,
        identifier_count,
        source_count,
        Has_Identifier_count,
        Has_Source_count,
        Describes_Person_count,
        Describes_Entity_count,
        hasadress_count,
        is_interested_party_count,
        interested_in_count
        
    )
# ========================
# KJØR
# ========================
teller_total, ps, fs, osc, p, f, a, i, so, ir, hs, dp, de, ha, ip, ii = stream_import()

print("🎉 Import ferdig!")
print(f"Totalt prosessert objekter: {teller_total}")
print(f"PersonStatement-noder: {ps}")
print(f"EntityStatement-noder: {fs}")
print(f"OOCStatement-noder: {osc}")
print(f"Person-noder: {p}")
print(f"Entity-noder: {f}")
print(f"Adresse-noder: {a}")
print(f"identifier-noder: {i}")
print(f"Source-noder: {so}")
print(f"HAS_IDENTIFIER relasjoner: {ir}")
print(f"HAS_SOURCE relasjoner: {hs}")
print(f"DESCRIBES_PERSON relasjoner: {dp}")
print(f"DESCRIBES_ENTITY relasjoner: {de}")
print(f"HAS_ADDRESS relasjoner: {ha}")
print(f"IS_INTERESTED_PARTY relasjoner: {ip}")
print(f"INTERESTED_IN relasjoner: {ii}")
