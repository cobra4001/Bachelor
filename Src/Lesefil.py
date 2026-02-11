import orjson

FILE_PATH = r"F:\OpenOwnership full fil\register.json"

duplicates_info = []
missing_id_info = []

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
                    # Sjekk om interest mangler type
                    if not interest_type:
                        missing_id_info.append({
                            "statementID": statement_id,
                            "subjectID": subject_id,
                            "interest": interest
                        })
                        continue  # hopper over denne interest for duplikat-sjekk
                    
                    key = (statement_id, subject_id, interest_type)
                    
                    if key in seen:
                        duplicates_info.append({
                            "statementID": statement_id,
                            "type": interest_type,
                            "interest1": seen[key],
                            "interest2": interest
                        })
                    else:
                        seen[key] = interest
                
                # Sample første 100 duplikater/manglende
                if len(duplicates_info) >= 100 and len(missing_id_info) >= 100:
                    break
        except Exception as e:
            print("Feil parsing linje:", e)
            continue

# Print eksempler
print(f"Fant {len(duplicates_info)} duplikat-eksempler:\n")
for i, dup in enumerate(duplicates_info[:10]):
    print(f"\n--- Duplikat {i+1} ---")
    print(f"StatementID: {dup['statementID']}")
    print(f"Type: {dup['type']}")
    print("Interest 1:", dup['interest1'])
    print("Interest 2:", dup['interest2'])
    print("-" * 50)

print(f"\nFant {len(missing_id_info)} interests uten type eller id:\n")
for i, miss in enumerate(missing_id_info[:10]):
    print(f"\n--- Manglende {i+1} ---")
    print(f"StatementID: {miss['statementID']}")
    print(f"SubjectID: {miss['subjectID']}")
    print("Interest:", miss['interest'])
    print("-" * 50)
