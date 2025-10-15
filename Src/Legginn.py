import os
import json
from neo4j import GraphDatabase

# 🔹 Filbane til JSON
BODS_FILE = r"C:\Users\Admin2\Downloads\test.json"

# 🔹 Neo4j-tilkobling
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "test1234"

# 🔹 Batch-størrelse for store datasett
BATCH_SIZE = 1000

# 🔹 Les JSON-fil (liste eller linje-per-linje)
def read_json_objects(file_path):
    objects = []
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            if isinstance(data, dict):
                objects.append(data)
            elif isinstance(data, list):
                objects.extend(data)
            else:
                print("Ukjent JSON-format")
        except json.JSONDecodeError:
            f.seek(0)
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    objects.append(obj)
                except json.JSONDecodeError as e:
                    print(f"Ugyldig JSON på linje {line_number}: {line}\n  {e}")
    return objects

# 🔹 Rekursiv flattening: konverterer alle dicts til JSON-strenger
def flatten_object_recursive(obj):
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            if isinstance(v, dict) or isinstance(v, list):
                new_obj[k] = json.dumps(v)
            else:
                new_obj[k] = v
        return new_obj
    else:
        return obj

# 🔹 Sett inn noder batchvis
def insert_nodes(driver, objects, label="Person"):
    with driver.session() as session:
        total = len(objects)
        for start in range(0, total, BATCH_SIZE):
            batch = objects[start:start+BATCH_SIZE]
            tx = session.begin_transaction()
            for obj in batch:
                obj_flat = flatten_object_recursive(obj)
                keys = obj_flat.keys()
                props = ", ".join([f"{k}: ${k}" for k in keys])
                query = f"CREATE (n:{label} {{ {props} }})"
                tx.run(query, **obj_flat)
            tx.commit()
            print(f"Batch {start}–{start+len(batch)-1} lagt inn")

# 🔹 Hovedprogram
if __name__ == "__main__":
    if not os.path.exists(BODS_FILE):
        print(f"Filen finnes ikke: {BODS_FILE}")
        exit(1)

    print(f"Filen ble funnet: {BODS_FILE}")

    json_objects = read_json_objects(BODS_FILE)
    print(f"Totalt objekter å legge inn: {len(json_objects)}")

    if not json_objects:
        print("Ingen objekter funnet i filen.")
    else:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        insert_nodes(driver, json_objects, label="Person")
        driver.close()
        print("Alle objekter lagt inn i Neo4j.")
