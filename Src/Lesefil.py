import orjson
from collections import defaultdict

FILE_PATH = r"F:\OpenOwnership full fil\register.json"

# Finn statements med duplikater
duplicates_info = []

with open(FILE_PATH, "rb") as f:
    for line in f:
        try:
            obj = orjson.loads(line)
            
            if obj.get("statementType") == "ownershipOrControlStatement":
                subject_id = obj.get("subject", {}).get("describedByEntityStatement")
                statement_id = obj.get("statementID")
                
                interests = obj.get("interests", [])
                
                # Finn duplikater innenfor samme statement
                seen = {}
                for interest in interests:
                    interest_type = interest.get("type")
                    key = (statement_id, subject_id, interest_type)
                    
                    if key in seen:
                        # Dette er en duplikat!
                        duplicates_info.append({
                            "statementID": statement_id,
                            "type": interest_type,
                            "interest1": seen[key],
                            "interest2": interest
                        })
                    else:
                        seen[key] = interest
                
                if len(duplicates_info) >= 100:  # Sample de første 100
                    break
        except:
            continue

# Print eksempler
print(f"Fant {len(duplicates_info)} duplikat-eksempler:\n")

for i, dup in enumerate(duplicates_info[:10]):  # Vis første 10
    print(f"\n--- Duplikat {i+1} ---")
    print(f"StatementID: {dup['statementID']}")
    print(f"Type: {dup['type']}")
    print(f"\nInterest 1:")
    for key, val in dup['interest1'].items():
        print(f"  {key}: {val}")
    print(f"\nInterest 2:")
    for key, val in dup['interest2'].items():
        print(f"  {key}: {val}")
    print("-" * 50)