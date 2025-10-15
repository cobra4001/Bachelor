import json
from neo4j import GraphDatabase

# ===== Konfigurasjon =====
BODS_FILE = r"C:\Users\Admin2\Downloads\test.json"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "test1234"  # Endre til ditt passord
BATCH_SIZE = 1000  # Sett batch størrelse

# ===== Funksjoner =====
def read_json_file(file_path):
    """Les JSON-fil som en liste av objekter (støtter flere JSON-objekter i fila)."""
    objects = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:  # hopp over tomme linjer
                obj = json.loads(line)
                objects.append(obj)
    return objects

def flatten_dict(d, parent_key='', sep='_'):
    """Flatten nested dictionaries slik at alle values blir primitive eller arrays."""
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        elif isinstance(v, list):
            # konverter liste av dicts til liste av str
            new_list = []
            for item in v:
                if isinstance(item, dict):
                    new_list.append(json.dumps(item))
                else:
                    new_list.append(item)
            items[new_key] = new_list
        else:
            items[new_key] = v
    return items

def create_nodes(tx, obj):
    """Oppretter én node per objekt, bruker label fra 'entityType' eller 'type'."""
    label = obj.get("entityType") or obj.get("type") or "Unknown"
    node_props = flatten_dict(obj)
    props_str = ", ".join(f"{k}: ${k}" for k in node_props.keys())
    query = f"CREATE (n:{label} {{ {props_str} }})"
    tx.run(query, **node_props)

def insert_nodes(driver, objects):
    total = len(objects)
    print(f"Totalt objekter å legge inn: {total}")
    for i in range(0, total, BATCH_SIZE):
        batch = objects[i:i+BATCH_SIZE]
        with driver.session() as session:
            session.write_transaction(lambda tx: [create_nodes(tx, obj) for obj in batch])
        print(f"Lagt inn {i + len(batch)} av {total} objekter")

# ===== Main =====
if __name__ == "__main__":
    try:
        json_objects = read_json_file(BODS_FILE)
    except FileNotFoundError:
        print(f"Filen finnes ikke: {BODS_FILE}")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Feil ved parsing av JSON: {e}")
        exit(1)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        insert_nodes(driver, json_objects)
    finally:
        driver.close()

    print("Ferdig!")
