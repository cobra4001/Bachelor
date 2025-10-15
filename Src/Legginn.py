from neo4j import GraphDatabase
import json
import os

# ------------------------------
# KONFIGURASJON
# ------------------------------
# 🔹 Sett riktig filbane til din lokale JSON-fil:
BODS_FILE = os.path.expanduser("~/Downloads/test.json")

# 🔹 Neo4j-databasekobling (oppdater passord om nødvendig)
NEO4J_URI = "bolt://192.168.50.23:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

# ------------------------------
# SETT OPP DATABASEKOBLING
# ------------------------------
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# ------------------------------
# FUNKSJONER
# ------------------------------
def create_indexes(tx):
    """Lag indekser for rask matching."""
    tx.run("CREATE INDEX person_id IF NOT EXISTS FOR (p:Person) ON (p.statementID)")
    tx.run("CREATE INDEX entity_id IF NOT EXISTS FOR (e:Entity) ON (e.statementID)")
    tx.run("CREATE INDEX stmt_id IF NOT EXISTS FOR (s:OwnershipOrControlStatement) ON (s.statementID)")


def load_bods_statement(tx, stmt):
    stype = stmt.get("statementType")
    sid = stmt.get("statementID")

    # ------------------ PERSON ------------------
    if stype == "personStatement":
        person_name = stmt.get("names", [{}])[0].get("fullName", "Unknown")
        nationality = stmt.get("nationalities", [""])[0]
        tx.run("""
            MERGE (p:Person {statementID: $sid})
            SET p.name = $name, p.nationality = $nat
        """, sid=sid, name=person_name, nat=nationality)

        # Identifiers
        for ident in stmt.get("identifiers", []):
            scheme = ident.get("schemeName", "")
            val = ident.get("id", "")
            tx.run("""
                MERGE (i:Identifier {id: $val, scheme: $scheme})
                MERGE (p:Person {statementID: $sid})-[:HAS_IDENTIFIER]->(i)
            """, sid=sid, val=val, scheme=scheme)

        # Addresses
        for addr in stmt.get("addresses", []):
            full_addr = addr.get("address", "")
            country = addr.get("country", "")
            tx.run("""
                MERGE (a:Address {address: $addr, country: $country})
                MERGE (p:Person {statementID: $sid})-[:HAS_ADDRESS]->(a)
            """, sid=sid, addr=full_addr, country=country)

    # ------------------ ENTITY ------------------
    elif stype == "entityStatement":
        entity_name = stmt.get("name", "Unknown")
        country = stmt.get("incorporatedInJurisdiction", {}).get("code", "")
        tx.run("""
            MERGE (e:Entity {statementID: $sid})
            SET e.name = $name, e.country = $country
        """, sid=sid, name=entity_name, country=country)

        # Identifiers
        for ident in stmt.get("identifiers", []):
            scheme = ident.get("schemeName", "")
            val = ident.get("id", "")
            tx.run("""
                MERGE (i:Identifier {id: $val, scheme: $scheme})
                MERGE (e:Entity {statementID: $sid})-[:HAS_IDENTIFIER]->(i)
            """, sid=sid, val=val, scheme=scheme)

        # Addresses
        for addr in stmt.get("addresses", []):
            full_addr = addr.get("address", "")
            country = addr.get("country", "")
            tx.run("""
                MERGE (a:Address {address: $addr, country: $country})
                MERGE (e:Entity {statementID: $sid})-[:HAS_ADDRESS]->(a)
            """, sid=sid, addr=full_addr, country=country)

    # ------------------ OWNERSHIP OR CONTROL ------------------
    elif stype == "ownershipOrControlStatement":
        tx.run("""
            MERGE (o:OwnershipOrControlStatement {statementID: $sid})
            SET o.type = $stype, o.statementDate = $date
        """, sid=sid, stype=stype, date=stmt.get("statementDate"))

        # Link interestedParty → statement
        party = stmt.get("interestedParty", {}).get("describedByStatement")
        if party:
            tx.run("""
                MATCH (p {statementID: $pid}), (o:OwnershipOrControlStatement {statementID: $sid})
                MERGE (p)-[:IS_SUBJECT_OF]->(o)
            """, pid=party, sid=sid)

        # Link subject (the company)
        subject = stmt.get("subject", {}).get("describedByStatement")
        if subject:
            tx.run("""
                MATCH (e {statementID: $eid}), (o:OwnershipOrControlStatement {statementID: $sid})
                MERGE (o)-[:HOLDS_INTEREST_IN]->(e)
            """, eid=subject, sid=sid)

        # Interests
        for intr in stmt.get("interests", []):
            itype = intr.get("type", "")
            share = intr.get("shareExact") or intr.get("shareMinimum")
            tx.run("""
                MERGE (i:Interest {type: $itype, share: $share})
                MERGE (o:OwnershipOrControlStatement {statementID: $sid})-[:HAS_INTEREST]->(i)
            """, sid=sid, itype=itype, share=share)

        # Source
        for src in stmt.get("source", []):
            sname = src.get("publisher", "")
            surl = src.get("url", "")
            tx.run("""
                MERGE (s:Source {url: $url})
                SET s.publisher = $publisher
                MERGE (o:OwnershipOrControlStatement {statementID: $sid})-[:REPORTED_BY]->(s)
            """, sid=sid, publisher=sname, url=surl)


# ------------------------------
# LAST INN DATA FRA LOKAL FIL
# ------------------------------
if not os.path.exists(BODS_FILE):
    raise FileNotFoundError(f"Fant ikke filen: {BODS_FILE}")

with driver.session() as session:
    session.execute_write(create_indexes)

    with open(BODS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    statements = data.get("statements", [])
    print(f"🔹 Importerer {len(statements)} statements fra {BODS_FILE}...")

    for stmt in statements:
        session.execute_write(load_bods_statement, stmt)

driver.close()
print("✅ Ferdig! Alle BODS-data er importert fra lokal JSON-fil til Neo4j.")
