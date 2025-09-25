from neo4j import GraphDatabase

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "Test12345")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Connection established.")


#Kode fra ChatGPT - for å sjekke om koblingen ble laget.
driver = GraphDatabase.driver(URI, auth=AUTH)

with driver.session() as session:
    print("Connection established.")

    # Test-spørring
    result = session.run("MATCH (p:Person) RETURN p.name AS name")

    for record in result:
        print("Fant person:", record["name"])

driver.close()