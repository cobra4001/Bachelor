import orjson
import time

count_with_source = 0
count_without_source = 0
total_entity_statements = 0
start_time = time.time()

print("Scanning fil...")

with open(r"F:\1mil\1000000.json", "rb") as f:
    for i, line in enumerate(f):
        try:
            obj = orjson.loads(line)
            
            if obj.get("statementType") == "entityStatement":
                total_entity_statements += 1
                
                if obj.get("source") and obj.get("source").get("url"):
                    count_with_source += 1
                else:
                    count_without_source += 1
            
            # Progress update hver 100k linje
            if (i + 1) % 100000 == 0:
                elapsed = time.time() - start_time
                print(f"Processed {i+1:,} lines in {elapsed:.1f}s | EntityStatements: {total_entity_statements:,} | Med source: {count_with_source:,}")
        
        except:
            continue

elapsed = time.time() - start_time
print(f"\n=== RESULTAT ===")
print(f"Total linjer scannet: {i+1:,}")
print(f"Total EntityStatements: {total_entity_statements:,}")
print(f"EntityStatements MED source: {count_with_source:,} ({count_with_source/total_entity_statements*100:.1f}%)")
print(f"EntityStatements UTEN source: {count_without_source:,} ({count_without_source/total_entity_statements*100:.1f}%)")
print(f"Total tid: {elapsed:.1f}s")