from neo4j import GraphDatabase
from datetime import datetime

# ========================
# KONFIG
# ========================
URI = "bolt://10.196.241.255:7687"
AUTH = ("neo4j", "mRTzP1@h")
BATCH_SIZE = 2000   # antall noder per batch
# ========================

driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()
print("Connection to Neo4j established.")

def delete_all_nodes_batch(tx, batch_size):
    result = tx.run(f"""
        MATCH (n)
        WITH n LIMIT $batch_size
        DETACH DELETE n
        RETURN count(n) as deleted
    """, batch_size=batch_size)
    return result.single()["deleted"]

def delete_all_nodes():
    total_deleted = 0
    start_time = datetime.now()
    
    while True:
        with driver.session() as session:
            deleted = session.execute_write(delete_all_nodes_batch, BATCH_SIZE)
        
        if deleted == 0:
            break
        
        total_deleted += deleted
        print(f"{total_deleted} nodes deleted so far, batch deleted: {deleted}, time: {datetime.now() - start_time}")
    
    print(f"🎉 Finished deleting all nodes. Total deleted: {total_deleted}")

# ========================
# Kjør sletting
# ========================
delete_all_nodes()