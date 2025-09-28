from neo4j import GraphDatabase

# ---------- Konfigurasjon ----------
NEO4J_URI = ""  # Neo4j URL
NEO4J_USER = "neo4j"                  # brukernavn
NEO4J_PASSWORD = ""           # passord

# ---------- Koble til Neo4j ----------
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ---------- Funksjon for å opprette noder ----------
def create_nodes(tx):
    tx.run("CREATE (:Person {name: 'Alice'})")
    tx.run("CREATE (:Person {name: 'Bob'})")

# ---------- Kjør transaksjon ----------
with driver.session() as session:
    session.write_transaction(create_nodes)

driver.close()

print("To noder opprettet i Neo4j!")
