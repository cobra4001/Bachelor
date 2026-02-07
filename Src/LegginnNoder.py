import os
import orjson
import time
from multiprocessing import Process, Value
from datetime import datetime
from neo4j import GraphDatabase
from neo4j.exceptions import TransientError

# ========================
# KONFIGURASJON
# ========================
URI = "bolt://10.196.241.255:7687"
AUTH = ("neo4j", "mRTzP1@h")
FILE_PATH = r"F:\OpenOwnership full fil\register.json"

BATCH_SIZE = 7000
CPU_CORES = 8       # Bruker alle 8 kjernene
LOG_INTERVAL = 2.0  # Hvor ofte (sekunder) status skal printes til terminalen


def run_with_retry(session, func, batch, max_retries=5):
    """
    Kjør en skrivetransaksjon med enkel retry ved TransientError (inkl. deadlocks).
    Dette hindrer at prosesser feiler permanent ved midlertidige låsekonflikter.
    """
    attempt = 0
    while True:
        try:
            return session.execute_write(func, batch)
        except TransientError as e:
            attempt += 1
            if attempt > max_retries:
                print(f"🚨 Gitt opp etter {max_retries} forsøk for batch: {e}")
                raise
            sleep_time = 0.5 * attempt
            print(f"⚠️ TransientError (forsøk {attempt}/{max_retries}), prøver igjen om {sleep_time:.1f}s: {e.code}")
            time.sleep(sleep_time)

# ========================
# HJELPEFUNKSJONER (Globalt scope for workers)
# ========================
def parse_share(value):
    if isinstance(value, str):
        value = value.strip().replace("%", "")
        try:
            return float(value)
        except ValueError:
            return None
    elif isinstance(value, (int, float)):
        return float(value)
    else:
        return None

# ========================
# CYPHER QUERIES
# ========================

def import_person_statements(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (ps:PersonStatement {statementID: obj.statementID})
        SET
            ps.personType    = COALESCE(obj.personType, ps.personType),
            ps.statementType = COALESCE(obj.statementType, ps.statementType),
            ps.isComponent   = COALESCE(obj.isComponent, ps.isComponent),
            ps.statementDate = COALESCE(obj.statementDate, ps.statementDate),
            ps.pub_bodsVersion = COALESCE(obj.publicationDetails_bodsVersion, ps.pub_bodsVersion),
            ps.pub_license    = COALESCE(obj.publicationDetails_license, ps.pub_license),
            ps.pub_date       = COALESCE(obj.publicationDetails_publicationDate, ps.pub_date),
            ps.pub_publisher_name = COALESCE(obj.publicationDetails_publisher_name, ps.pub_publisher_name),
            ps.pub_publisher_url  = COALESCE(obj.publicationDetails_publisher_url, ps.pub_publisher_url)
    """, batch=batch)

def import_persons(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (p:Person {ident: obj.personID})
        SET
            p.fullName      = COALESCE(obj.fullName, p.fullName),
            p.givenName     = COALESCE(obj.givenName, p.givenName),
            p.familyName    = COALESCE(obj.familyName, p.familyName),
            p.birthDate     = COALESCE(obj.birthDate, p.birthDate),
            p.type          = COALESCE(obj.type, p.type),
            p.personType    = COALESCE(obj.personType, p.personType),
            p.CountryCode   = COALESCE(obj.Countrycode, p.CountryCode),
            p.Country       = COALESCE(obj.Country, p.Country)
    """, batch=batch)

def import_nationalities(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        WITH obj WHERE obj.code IS NOT NULL
        MERGE (n:Nationality {code: obj.code})
        SET n.name = COALESCE(obj.name, n.name)
    """, batch=batch)

def import_person_addresses(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        WITH DISTINCT obj.address AS addr, obj.country AS ctry, obj.type AS t
        WHERE addr IS NOT NULL AND ctry IS NOT NULL AND t IS NOT NULL
        // Sortering gjør at alle tråder låser i samme rekkefølge -> Mindre deadlocks
        ORDER BY addr
        MERGE (a:Address {address: addr, country: ctry, type: t})
    """, batch=batch)

def import_person_sources(tx, batch):
    # Constraint på Source.url håndterer unikhet
    tx.run("""
        UNWIND $batch AS obj
        WITH obj WHERE obj.url IS NOT NULL
        MERGE (so:Source {url: obj.url})
        SET
            so.assertedBy  = COALESCE(obj.assertedBy, so.assertedBy),
            so.description = COALESCE(obj.description, so.description),
            so.retrievedAt = COALESCE(obj.retrievedAt, so.retrievedAt),
            so.type        = COALESCE(obj.type, so.type),
            so.url         = COALESCE(obj.url, so.url)
    """, batch=batch)

def import_person_identifiers(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (i:Identifier {uri: obj.uri})
        SET
            i.id = COALESCE(obj.id, i.id),
            i.schemeName = COALESCE(obj.schemeName, i.schemeName),
            i.uri = COALESCE(obj.uri, i.uri)    
    """, batch=batch)

def import_entity_statements(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (fs:EntityStatement {statementID: obj.statementID})
        SET
            fs.replacesStatements = COALESCE(obj.replacesStatements, fs.replacesStatements),
            fs.statementType   = COALESCE(obj.statementType, fs.statementType),
            fs.isComponent = COALESCE(obj.isComponent, fs.isComponent),
            fs.statementDate = COALESCE(obj.statementDate, fs.statementDate),
            fs.pub_bodsVersion = COALESCE(obj.publicationDetails_bodsVersion, fs.pub_bodsVersion),
            fs.pub_license    = COALESCE(obj.publicationDetails_license, fs.pub_license),
            fs.pub_date       = COALESCE(obj.publicationDetails_publicationDate, fs.pub_date),
            fs.pub_publisher_name = COALESCE(obj.publicationDetails_publisher_name, fs.pub_publisher_name),
            fs.pub_publisher_url  = COALESCE(obj.publicationDetails_publisher_url, fs.pub_publisher_url)
    """, batch=batch)

def import_entitys(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (f:Entity {ident: obj.EntityID})
        SET
            f.name    = COALESCE(obj.name, f.name),
            f.alternateNames = COALESCE(obj.alternateNames, f.alternateNames),
            f.entityType = COALESCE(obj.entityType, f.entityType),  
            f.foundingDate = COALESCE(obj.foundingDate, f.foundingDate),
            f.incorporatedInJurisdiction_name = COALESCE(obj.incorporatedInJurisdiction_name, f.incorporatedInJurisdiction_name),
            f.incorporatedInJurisdiction_code = COALESCE(obj.incorporatedInJurisdiction_code, f.incorporatedInJurisdiction_code),
            f.dissolutionDate = COALESCE(obj.dissolutionDate, f.dissolutionDate)
    """, batch=batch)

def import_entity_addresses(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        WITH DISTINCT obj.address AS addr, obj.country AS ctry, obj.type AS t
        WHERE addr IS NOT NULL AND ctry IS NOT NULL AND t IS NOT NULL

        // Sortering for å unngå deadlock-sykluser
        WITH addr, ctry, t
        ORDER BY addr, ctry, t

        MERGE (a:Address {address: addr, country: ctry, type: t})
    """, batch=batch)


def import_entity_sources(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        WITH obj WHERE obj.url IS NOT NULL
        MERGE (so:Source {url: obj.url})
        SET
            so.assertedBy  = COALESCE(obj.assertedBy, so.assertedBy),
            so.description = COALESCE(obj.description, so.description),
            so.retrievedAt = COALESCE(obj.retrievedAt, so.retrievedAt),
            so.type        = COALESCE(obj.type, so.type),
            so.url         = COALESCE(obj.url, so.url)
    """, batch=batch)

def import_entity_identifiers(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        MERGE (i:Identifier {uri: obj.uri})
        SET
            i.id = COALESCE(obj.id, i.id),
            i.scheme = COALESCE(obj.scheme, i.scheme),
            i.schemeName = COALESCE(obj.schemeName, i.schemeName),
            i.uri = COALESCE(obj.uri, i.uri)
    """, batch=batch)

def import_OOC_statements(tx, batch): 
    tx.run("""
        UNWIND $batch AS obj
        MERGE (os:OOCStatement {statementID: obj.statementID})   
        SET
            os.statementType = COALESCE(obj.statementType, os.statementType),
            os.statementDate = COALESCE(obj.statementDate, os.statementDate),
            os.isComponent   = COALESCE(obj.isComponent, os.isComponent),
            os.interestedParty_describedByPersonStatement = COALESCE(obj.interestedParty_describedByPersonStatement, os.interestedParty_describedByPersonStatement),
            os.interestedParty_describedByEntityStatement = COALESCE(obj.interestedParty_describedByEntityStatement, os.interestedParty_describedByEntityStatement),
            os.pub_bodsVersion = COALESCE(obj.publicationDetails_bodsVersion, os.pub_bodsVersion),
            os.pub_license    = COALESCE(obj.publicationDetails_license, os.pub_license),
            os.pub_date       = COALESCE(obj.publicationDetails_publicationDate, os.pub_date),
            os.pub_publisher_name = COALESCE(obj.publicationDetails_publisher_name, os.pub_publisher_name),
            os.pub_publisher_url  = COALESCE(obj.publicationDetails_publisher_url, os.pub_publisher_url),
            os.subject_describedByEntityStatement = COALESCE(obj.subject_describedByEntityStatement, os.subject_describedByEntityStatement)
    """, batch=batch)

def import_OOC_sources(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        WITH obj WHERE obj.url IS NOT NULL
        MERGE (so:Source {url: obj.url})
        SET
            so.assertedBy  = COALESCE(obj.assertedBy, so.assertedBy),
            so.description = COALESCE(obj.description, so.description),
            so.retrievedAt = COALESCE(obj.retrievedAt, so.retrievedAt),
            so.type        = COALESCE(obj.type, so.type),
            so.url         = COALESCE(obj.url, so.url)
    """, batch=batch)

# ========================
# CONSTRAINTS (Kjøres i main)
# ========================
def create_constraints(session):
    # Person, Entity, OOC, PersonStatement, EntityStatement, Identifier, Source
    session.run("CREATE CONSTRAINT person_ident IF NOT EXISTS FOR (p:Person) REQUIRE p.ident IS UNIQUE")
    session.run("CREATE CONSTRAINT entity_ident IF NOT EXISTS FOR (e:Entity) REQUIRE e.ident IS UNIQUE")
    session.run("CREATE CONSTRAINT ooc_statement_ident IF NOT EXISTS FOR (os:OOCStatement) REQUIRE os.statementID IS UNIQUE")
    session.run("CREATE CONSTRAINT person_statement_id IF NOT EXISTS FOR (ps:PersonStatement) REQUIRE ps.statementID IS UNIQUE")
    session.run("CREATE CONSTRAINT entity_statement_id IF NOT EXISTS FOR (es:EntityStatement) REQUIRE es.statementID IS UNIQUE")
    session.run("CREATE CONSTRAINT address_unique IF NOT EXISTS FOR (a:Address) REQUIRE (a.address, a.country, a.type) IS UNIQUE")
    session.run("CREATE CONSTRAINT identifier_uri IF NOT EXISTS FOR (i:Identifier) REQUIRE i.uri IS UNIQUE")
    
    # VIKTIG: Constraint på Source URL for å håndtere unikhet
    session.run("CREATE CONSTRAINT source_url IF NOT EXISTS FOR (s:Source) REQUIRE s.url IS UNIQUE")
    print("✅ Constraints created for Person, Entity, OOCStatement, PersonStatement, EntityStatement, Identifier, and Source.")


# ========================
# WORKER PROCESS
# ========================
def worker_process(chunk_info, shared_counter):
    """
    Kjøres av hver prosess. Leser en del av filen og importerer til Neo4j.
    """
    file_path, start_byte, end_byte, worker_id = chunk_info
    
    # Koble til Neo4j i hver prosess
    driver = GraphDatabase.driver(URI, auth=AUTH)
    
    # Lokale batch-lister
    batches = {
        "personStatement": [], "person": [], "nationalities": [], "personaddress": [], "personsource": [], "personidentifier": [],
        "entityStatement": [], "entity": [], "entityaddress": [], "entitysource": [], "entityidentifier": [],
        "OOCStatement": [], "OOCsource": []
    }

    local_processed_count = 0

    try:
        with open(file_path, "rb") as f:
            f.seek(start_byte)
            # Hvis vi ikke er på starten, hopp over første (delvise) linje
            if start_byte > 0:
                f.readline()

            while True:
                # Sjekk om vi er ferdig med vår del
                if f.tell() > end_byte:
                    break
                
                line = f.readline()
                if not line: break

                try:
                    obj = orjson.loads(line)
                except:
                    continue

                stype = obj.get("statementType")
                if not stype: continue

                sid = obj.get("statementID")

                # ========================
                # MAPPING 
                # ========================

                if stype == "personStatement":
                    batches["personStatement"].append({
                        "statementID": sid,
                        "personType": obj.get("personType"),
                        "statementType": obj.get("statementType"),
                        "isComponent": obj.get("isComponent"),
                        "replacesStatements": obj.get("replacesStatements"),
                        "statementDate": obj.get("statementDate"),
                        "publicationDetails_bodsVersion": obj.get("publicationDetails", {}).get("bodsVersion"),
                        "publicationDetails_license": obj.get("publicationDetails", {}).get("license"),
                        "publicationDetails_publicationDate": obj.get("publicationDetails", {}).get("publicationDate"),
                        "publicationDetails_publisher_name": obj.get("publicationDetails", {}).get("publisher", {}).get("name"),
                        "publicationDetails_publisher_url": obj.get("publicationDetails", {}).get("publisher", {}).get("url")
                    })

                    names = obj.get("names", [])
                    name = names[0] if names else {}
                  
                    

                    batches["person"].append({ 
                        "personID": sid,
                        "fullName": name.get("fullName"),
                        "givenName": name.get("givenName"),
                        "familyName": name.get("familyName"),
                        "type": name.get("type"),
                        "birthDate": obj.get("birthDate"),
                        "personType": obj.get("personType")
                    })

                    for nat in obj.get("nationalities", []):
                        if nat.get("code"):  # Kun hvis code finnes
                            batches["nationalities"].append({
                                "code": nat.get("code"),
                                "name": nat.get("name"),
                            })

                    
                    for addr in obj.get("addresses", []):
                        if addr.get("address"):
                            batches["personaddress"].append({"address": addr.get("address"), "country": addr.get("country"), "type": addr.get("type")})

                    source = obj.get("source")
                    if source and source.get("url"):
                        batches["personsource"].append(source) # Pass source dict directly, url is key

                    for ident in obj.get("identifiers", []):
                        if ident.get("uri"):
                            batches["personidentifier"].append(ident)

                elif stype == "entityStatement":
                    batches["entityStatement"].append({
                        "statementID": sid,
                        "replacesStatements": obj.get("replacesStatements"),
                        "statementType": obj.get("statementType"),
                        "isComponent": obj.get("isComponent"),
                        "statementDate": obj.get("statementDate"),
                        "publicationDetails_bodsVersion": obj.get("publicationDetails", {}).get("bodsVersion"),
                        "publicationDetails_license": obj.get("publicationDetails", {}).get("license"),
                        "publicationDetails_publicationDate": obj.get("publicationDetails", {}).get("publicationDate"),
                        "publicationDetails_publisher_name": obj.get("publicationDetails", {}).get("publisher", {}).get("name"),
                        "publicationDetails_publisher_url": obj.get("publicationDetails", {}).get("publisher", {}).get("url")
                    })

                    batches["entity"].append({
                        "EntityID": sid,
                        "name": obj.get("name"),
                        "alternateNames": obj.get("alternateNames"),
                        "entityType": obj.get("entityType"),
                        "foundingDate": obj.get("foundingDate"),
                        "incorporatedInJurisdiction_name": obj.get("incorporatedInJurisdiction", {}).get("name"),
                        "incorporatedInJurisdiction_code": obj.get("incorporatedInJurisdiction", {}).get("code"),
                        "dissolutionDate": obj.get("dissolutionDate")
                    })

                    for addr in obj.get("addresses", []):
                        if addr.get("address"):
                            batches["entityaddress"].append({"address": addr.get("address"), "country": addr.get("country"), "type": addr.get("type")})

                    source = obj.get("source")
                    if source and source.get("url"):
                        batches["entitysource"].append(source)

                    for ident in obj.get("identifiers", []):
                        if ident.get("uri"):
                            batches["entityidentifier"].append(ident)

                elif stype == "ownershipOrControlStatement":
                    batches["OOCStatement"].append({
                        "statementID": sid,
                        "statementType": obj.get("statementType"),
                        "statementDate": obj.get("statementDate"),
                        "isComponent": obj.get("isComponent"),
                        "interestedParty_describedByPersonStatement": obj.get("interestedParty", {}).get("describedByPersonStatement"),
                        "interestedParty_describedByEntityStatement": obj.get("interestedParty", {}).get("describedByEntityStatement"),
                        "publicationDetails_bodsVersion": obj.get("publicationDetails", {}).get("bodsVersion"),
                        "publicationDetails_license": obj.get("publicationDetails", {}).get("license"),
                        "publicationDetails_publicationDate": obj.get("publicationDetails", {}).get("publicationDate"),
                        "publicationDetails_publisher_name": obj.get("publicationDetails", {}).get("publisher", {}).get("name"),
                        "publicationDetails_publisher_url": obj.get("publicationDetails", {}).get("publisher", {}).get("url"),
                        "subject_describedByEntityStatement": obj.get("subject", {}).get("describedByEntityStatement"),
                    })

                    source = obj.get("source")
                    if source and source.get("url"):
                        batches["OOCsource"].append(source)

                local_processed_count += 1

                # ========================
                # FLUSH BATCHES (Hvis full)
                # ========================
                FLUSH_EVERY = 2000  # start her
                if local_processed_count % FLUSH_EVERY == 0:  # Sjekk batchstørrelser
                    with driver.session() as s:
                        if len(batches["personStatement"]) >= BATCH_SIZE:
                            run_with_retry(s, import_person_statements, batches["personStatement"])
                            batches["personStatement"].clear()
                        if len(batches["person"]) >= BATCH_SIZE:
                            run_with_retry(s, import_persons, batches["person"])
                            batches["person"].clear()
                        if len(batches["nationalities"]) >= BATCH_SIZE:
                            run_with_retry(s, import_nationalities, batches["nationalities"])
                            batches["nationalities"].clear()

                        if len(batches["personaddress"]) >= BATCH_SIZE:
                            run_with_retry(s, import_person_addresses, batches["personaddress"])
                            batches["personaddress"].clear()
                        if len(batches["personsource"]) >= BATCH_SIZE:
                            run_with_retry(s, import_person_sources, batches["personsource"])
                            batches["personsource"].clear()
                        if len(batches["personidentifier"]) >= BATCH_SIZE:
                            run_with_retry(s, import_person_identifiers, batches["personidentifier"])
                            batches["personidentifier"].clear()

                        if len(batches["entityStatement"]) >= BATCH_SIZE:
                            run_with_retry(s, import_entity_statements, batches["entityStatement"])
                            batches["entityStatement"].clear()
                        if len(batches["entity"]) >= BATCH_SIZE:
                            run_with_retry(s, import_entitys, batches["entity"])
                            batches["entity"].clear()
                        if len(batches["entityaddress"]) >= BATCH_SIZE:
                            run_with_retry(s, import_entity_addresses, batches["entityaddress"])
                            batches["entityaddress"].clear()
                        if len(batches["entitysource"]) >= BATCH_SIZE:
                            run_with_retry(s, import_entity_sources, batches["entitysource"])
                            batches["entitysource"].clear()
                        if len(batches["entityidentifier"]) >= BATCH_SIZE:
                            run_with_retry(s, import_entity_identifiers, batches["entityidentifier"])
                            batches["entityidentifier"].clear()

                        if len(batches["OOCStatement"]) >= BATCH_SIZE:
                            run_with_retry(s, import_OOC_statements, batches["OOCStatement"])
                            batches["OOCStatement"].clear()
                        if len(batches["OOCsource"]) >= BATCH_SIZE:
                            run_with_retry(s, import_OOC_sources, batches["OOCsource"])
                            batches["OOCsource"].clear()
                    
                    # Oppdater delt teller (trygt med multiprosessering)
                    with shared_counter.get_lock():
                        shared_counter.value += local_processed_count
                    local_processed_count = 0 # Nullstill lokal for å unngå dobbelttelling i batchlogikk

            # ========================
            # FLUSH REST (Slutten av chunk)
            # ========================
            with driver.session() as s:
                if batches["personStatement"]:
                    run_with_retry(s, import_person_statements, batches["personStatement"])
                if batches["person"]:
                    run_with_retry(s, import_persons, batches["person"])
                if batches["nationalities"]: 
                    run_with_retry(s, import_nationalities, batches["nationalities"])
                if batches["personaddress"]:
                    run_with_retry(s, import_person_addresses, batches["personaddress"])
                if batches["personsource"]:
                    run_with_retry(s, import_person_sources, batches["personsource"])
                if batches["personidentifier"]:
                    run_with_retry(s, import_person_identifiers, batches["personidentifier"])

                if batches["entityStatement"]:
                    run_with_retry(s, import_entity_statements, batches["entityStatement"])
                if batches["entity"]:
                    run_with_retry(s, import_entitys, batches["entity"])
                if batches["entityaddress"]:
                    run_with_retry(s, import_entity_addresses, batches["entityaddress"])
                if batches["entitysource"]:
                    run_with_retry(s, import_entity_sources, batches["entitysource"])
                if batches["entityidentifier"]:
                    run_with_retry(s, import_entity_identifiers, batches["entityidentifier"])

                if batches["OOCStatement"]:
                    run_with_retry(s, import_OOC_statements, batches["OOCStatement"])
                if batches["OOCsource"]:
                    run_with_retry(s, import_OOC_sources, batches["OOCsource"])
            
            # Oppdater teller med resten
            if local_processed_count > 0:
                with shared_counter.get_lock():
                    shared_counter.value += local_processed_count

    except Exception as e:
        print(f"🚨 Worker {worker_id} feilet: {e}")
    finally:
        driver.close()

# ========================
# MONITOR (Printer fremdrift)
# ========================
def monitor_progress(shared_counter, total_file_size):
    start_time = datetime.now()
    while True:
        time.sleep(LOG_INTERVAL)
        current_count = shared_counter.value
        elapsed = datetime.now() - start_time
        
        # Hvis vi vil stoppe (dette er en enkel implementasjon, main dreper prosessen når ferdig)
        print(f"📊 Processed objects: {current_count} | Tid: {elapsed}")

def prepopulate_nationalities(session, file_path):
    """Les gjennom filen én gang og lag alle unike Nationality-noder"""
    print("🌍 Pre-populerer Nationality-noder...")
    
    unique_nationalities = {}  # {code: name}
    
    with open(file_path, "rb") as f:
        for line in f:
            try:
                obj = orjson.loads(line)
                if obj.get("statementType") == "personStatement":
                    for nat in obj.get("nationalities", []):
                        code = nat.get("code")
                        if code:
                            unique_nationalities[code] = nat.get("name")
            except:
                continue
    
    print(f"   Funnet {len(unique_nationalities)} unike nasjonaliteter")
    
    # Opprett alle på en gang
    batch = [{"code": code, "name": name} for code, name in unique_nationalities.items()]
    
    session.execute_write(lambda tx: tx.run("""
        UNWIND $batch AS obj
        MERGE (n:Nationality {code: obj.code})
        SET n.name = COALESCE(obj.name, n.name)
    """, batch=batch))
    
    print(f"   ✅ {len(unique_nationalities)} Nationality-noder opprettet")
# ========================
# MAIN
# ========================
if __name__ == "__main__":
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(FILE_PATH)
    
    # 1. Oppsett av DB og Constraints
    print("🔌 Kobler til Neo4j for oppsett...")
    main_driver = GraphDatabase.driver(URI, auth=AUTH)
    main_driver.verify_connectivity()
    with main_driver.session() as s:
        create_constraints(s)
        prepopulate_nationalities(s, FILE_PATH)  # ← Pre-populer Nationality-noder
    main_driver.close()
    
    # 2. Forbered Multiprosessering
    file_size = os.path.getsize(FILE_PATH)
    chunk_size = file_size // CPU_CORES
    
    print(f"📦 Filstørrelse: {file_size / (1024*1024):.2f} MB")
    print(f"🚀 Starter {CPU_CORES} workers...")

    # Delt teller for alle prosessene
    shared_counter = Value('i', 0) 
    
    processes = []
    
    start_time_total = datetime.now()

    # 3. Start Workers
    for i in range(CPU_CORES):
        start = i * chunk_size
        end = file_size if i == CPU_CORES - 1 else (i + 1) * chunk_size
        chunk_args = (FILE_PATH, start, end, i+1)
        
        p = Process(target=worker_process, args=(chunk_args, shared_counter))
        p.start()
        processes.append(p)

    # 4. Start Monitor (for visuell output mens importen kjører)
    monitor_p = Process(target=monitor_progress, args=(shared_counter, file_size))
    monitor_p.start()

    # 5. Vent på at alle workers er ferdige
    for p in processes:
        p.join()
    
    # Stopp monitor
    monitor_p.terminate()

    # 6. Sluttrapport
    print("\n🎉 Import ferdig (Alle workers avsluttet)!")
    print(f"Total tid: {datetime.now() - start_time_total}")
    print(f"Totalt antall objekter lest ifølge teller: {shared_counter.value}")

    # Sjekk faktiske tall i DB
    print("\n🔍 Sjekker database-tall...")
    main_driver = GraphDatabase.driver(URI, auth=AUTH)
    with main_driver.session() as s:
        ps = s.run("MATCH (ps:PersonStatement) RETURN count(ps) AS c").single()["c"]
        fs = s.run("MATCH (fs:EntityStatement) RETURN count(fs) AS c").single()["c"]
        osc = s.run("MATCH (os:OOCStatement) RETURN count(os) AS c").single()["c"]
        p = s.run("MATCH (p:Person) RETURN count(p) AS c").single()["c"]
        f = s.run("MATCH (f:Entity) RETURN count(f) AS c").single()["c"]
        so = s.run("MATCH (so:Source) RETURN count(so) AS c").single()["c"]
        id = s.run("MATCH (i:Identifier) RETURN count(i) AS c").single()["c"]
        ad = s.run("MATCH (a:Address) RETURN count(a) AS c").single()["c"]
        na = s.run("MATCH (n:Nationality) RETURN count(n) AS c").single()["c"]
    main_driver.close()

    print(f"PersonStatement-noder: {ps}")
    print(f"EntityStatement-noder: {fs}")
    print(f"OOCStatement-noder: {osc}")
    print(f"Person-noder: {p}")
    print(f"Entity-noder: {f}")
    print(f"Source-noder: {so}")
    print(f"Identifier-noder: {id}")
    print(f"Address-noder: {ad}")
    print(f"Nationality-noder: {na}")