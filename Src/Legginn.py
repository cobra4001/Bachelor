import json
import os
from neo4j import GraphDatabase

# =================================================================
# 1. KONFIGURASJON
# =================================================================
BODS_FILE = r"C:\Users\Admin2\Downloads\test.json"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "test1234"  # Endre ved behov
BATCH_SIZE = 1000

# =================================================================
# 2. KOBLING TIL NEO4J
# =================================================================
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# =================================================================
# 3. INNLASTING AV JSON
# =================================================================
data = []
if not os.path.exists(BODS_FILE):
    print(f"Feil: Fant ikke filen '{BODS_FILE}'")
    exit()

with open(BODS_FILE, "r", encoding="utf-8") as f:
    for line_number, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue
        try:
            data.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"Advarsel: Hoppet over feilaktig JSON på linje {line_number}: {e}")

if not data:
    print("Feil: Ingen gyldige JSON-objekter ble lastet inn.")
    exit()

print(f"Innlest {len(data)} JSON-objekter fra filen.")

# =================================================================
# 4. FUNKSJON FOR Å KJØRE CYPHER I BATCHER
# =================================================================
def execute_batch(session, commands):
    for cmd in commands:
        session.run(cmd)

# =================================================================
# 5. GENERERING OG INNSKRIVING I NEO4J
# =================================================================
cypher_commands = []
statement_ids = set()

for item in data:
    s_id = item.get("statementID")
    if s_id in statement_ids:
        continue
    statement_ids.add(s_id)

    # --- ENTITY ---
    if item.get("statementType") == "entityStatement":
        name = item.get("name", "Ukjent Entitet")
        cvr_id = item.get("identifiers")[0].get("id", "N/A") if item.get("identifiers") else "N/A"
        name_escaped = name.replace("'", "\\'")
        cypher = (
            f"""MERGE (e:Entity {{statementID: '{s_id}'}})
ON CREATE SET e.name = '{name_escaped}',
e.cvrId = '{cvr_id}',
e.type = '{item.get('entityType', 'ukjent')}'"""
        )
        cypher_commands.append(cypher)

    # --- PERSON ---
    elif item.get("statementType") == "personStatement":
        name = item.get("names", [{}])[0].get("fullName", "Ukjent Person")
        name_escaped = name.replace("'", "\\'")
        cypher = (
            f"""MERGE (p:Person {{statementID: '{s_id}'}})
ON CREATE SET p.fullName = '{name_escaped}',
p.birthDate = '{item.get('birthDate', 'N/A')}'"""
        )
        cypher_commands.append(cypher)

    # --- RELASJON ---
    elif item.get("statementType") == "ownershipOrControlStatement":
        subject_id = item.get("subject", {}).get("describedByEntityStatement")
        party_id = item.get("interestedParty", {}).get("describedByPersonStatement") or \
                   item.get("interestedParty", {}).get("describedByEntityStatement")

        if not subject_id or not party_id:
            continue

        has_shareholding = "false"
        has_voting = "false"
        for interest in item.get("interests", []):
            if interest.get("type") == "shareholding":
                has_shareholding = "true"
            if interest.get("type") == "voting-rights":
                has_voting = "true"

        source_label = "Person" if item.get("interestedParty", {}).get("describedByPersonStatement") else "Entity"

        cypher = (
            f"""MATCH (source:{source_label} {{statementID: '{party_id}'}}),
(target:Entity {{statementID: '{subject_id}'}})
MERGE (source)-[r:CONTROLS]->(target)
ON CREATE SET r.hasShareholding = {has_shareholding},
r.hasVotingRights = {has_voting},
r.statementID = '{s_id}'"""
        )
        cypher_commands.append(cypher)

# =================================================================
# 6. UTFØR INNSKRIVING
# =================================================================
print(f"Starter import til Neo4j ({len(cypher_commands)} kommandoer)...")

with driver.session() as session:
    for i in range(0, len(cypher_commands), BATCH_SIZE):
        batch = cypher_commands[i:i + BATCH_SIZE]
        execute_batch(session, batch)
        print(f"   → Ferdig med batch {i//BATCH_SIZE + 1}/{(len(cypher_commands)//BATCH_SIZE) + 1}")

driver.close()

print("✅ Ferdig! Alle noder og relasjoner er lagt inn i Neo4j.")
