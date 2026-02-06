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
FILE_PATH = r"F:\100 tusen\100000.json"

BATCH_SIZE = 500
CPU_CORES = 8
LOG_INTERVAL = 2.0


def run_with_retry(session, func, batch, max_retries=5):
    """
    Kjør en skrivetransaksjon med enkel retry ved TransientError (inkl. deadlocks).
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


def parse_share(value):
    """Parse share values from string or number to float"""
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
# CYPHER QUERIES FOR RELATIONSHIPS
# ========================

def create_describes_person_relationships(tx, batch):
    """PersonStatement -[:DESCRIBES_PERSON]-> Person"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (ps:PersonStatement {statementID: obj.statementID})
        MATCH (p:Person {ident: obj.personID})
        MERGE (ps)-[:DESCRIBES_PERSON]->(p)
    """, batch=batch)



def create_person_has_address_relationships(tx, batch):
    """Person -[:HAS_ADDRESS]-> Address"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (p:Person {ident: obj.personID})
        MATCH (a:Address {address: obj.address, country: obj.country, type: obj.type})
        MERGE (p)-[:HAS_ADDRESS]->(a)
""", batch=batch)



def create_person_has_nationality_relationships(tx, batch):
    tx.run("""
        UNWIND $batch AS obj
        WITH obj WHERE obj.personID IS NOT NULL AND obj.code IS NOT NULL
        MATCH (p:Person {ident: obj.personID})
        MATCH (n:Nationality {code: obj.code})
        MERGE (p)-[:HAS_NATIONALITY]->(n)
    """, batch=batch)




def create_source_has_person_statement(tx, batch):
    """Source -[:HAS_STATEMENT]-> PersonStatement"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (s:Source {url: obj.url})
        MATCH (ps:PersonStatement {statementID: obj.statementID})
        MERGE (s)-[:HAS_STATEMENT]->(ps)
    """, batch=batch)


def create_person_statement_has_identifier_relationships(tx, batch):
    """PersonStatement -[:HAS_IDENTIFIER]-> Identifier"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (ps:PersonStatement {statementID: obj.statementID})
        MATCH (i:Identifier {uri: obj.uri})
        MERGE (ps)-[:HAS_IDENTIFIER]->(i)
    """, batch=batch)


def create_describes_entity_relationships(tx, batch):
    """EntityStatement -[:DESCRIBES_ENTITY]-> Entity"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (es:EntityStatement {statementID: obj.statementID})
        MATCH (e:Entity {ident: obj.EntityID})
        MERGE (es)-[:DESCRIBES_ENTITY]->(e)
    """, batch=batch)


def create_entity_has_address_relationships(tx, batch):
    """Entity -[:HAS_ADDRESS]-> Address"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (e:Entity {ident: obj.EntityID})
        MATCH (a:Address {address: obj.address, country: obj.country, type: obj.type})
        MERGE (e)-[:HAS_ADDRESS]->(a)
    """, batch=batch)


def create_source_has_entity_statement(tx, batch):
    """Source -[:HAS_STATEMENT]-> EntityStatement"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (s:Source {url: obj.url})
        MATCH (es:EntityStatement {statementID: obj.statementID})
        MERGE (s)-[:HAS_STATEMENT]->(es)
    """, batch=batch)


def create_entity_statement_has_identifier_relationships(tx, batch):
    """EntityStatement -[:HAS_IDENTIFIER]-> Identifier"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (es:EntityStatement {statementID: obj.statementID})
        MATCH (i:Identifier {uri: obj.uri})
        MERGE (es)-[:HAS_IDENTIFIER]->(i)
    """, batch=batch)


def create_source_has_ooc_statement(tx, batch):
    """Source -[:HAS_STATEMENT]-> OOCStatement"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (s:Source {url: obj.url})
        MATCH (os:OOCStatement {statementID: obj.statementID})
        MERGE (s)-[:HAS_STATEMENT]->(os)
    """, batch=batch)


def create_ooc_has_identifier_relationships(tx, batch):
    """OOCStatement -[:HAS_IDENTIFIER]-> Identifier"""
    tx.run("""
        UNWIND $batch AS obj
        MATCH (os:OOCStatement {statementID: obj.statementID})
        MATCH (i:Identifier {uri: obj.uri})
        MERGE (os)-[:HAS_IDENTIFIER]->(i)
    """, batch=batch)


def create_person_is_interested_party_relationships(tx, batch):
    """Person -[:IS_INTERESTED_PARTY]-> OOCStatement"""
    tx.run("""
        UNWIND $batch AS obj
        // Bare hvis person finnes
        WITH obj WHERE obj.interestedParty_describedByPersonStatement IS NOT NULL
        MATCH (p:Person {ident: obj.interestedParty_describedByPersonStatement})
        MATCH (os:OOCStatement {statementID: obj.statementID})
        MERGE (p)-[:IS_INTERESTED_PARTY]->(os)
    """, batch=batch)


def create_entity_is_interested_party_relationships(tx, batch):
    """Entity -[:IS_INTERESTED_PARTY]-> OOCStatement"""
    tx.run("""
        UNWIND $batch AS obj
        // Bare hvis entity finnes
        WITH obj WHERE obj.interestedParty_describedByEntityStatement IS NOT NULL
        MATCH (e:Entity {ident: obj.interestedParty_describedByEntityStatement})
        MATCH (os:OOCStatement {statementID: obj.statementID})
        MERGE (e)-[:IS_INTERESTED_PARTY]->(os)
    """, batch=batch)



def create_ooc_interest_in_entity_relationships(tx, batch):
    """OOCStatement -[:INTEREST_IN]-> Entity (with properties)"""
    tx.run("""
        UNWIND $batch AS obj
        WITH obj WHERE obj.subject_id IS NOT NULL
        MATCH (os:OOCStatement {statementID: obj.statementID})
        MATCH (e:Entity {ident: obj.subject_id})
        MERGE (os)-[r:INTEREST_IN]->(e)
        SET r.type = obj.interest_type,
            r.startDate = obj.startDate,
            r.share_exact = obj.share_exact,
            r.share_maximum = obj.share_maximum,
            r.share_minimum = obj.share_minimum,
            r.endDate = obj.endDate,
            r.details = obj.details,
            r.share_exclusiveMaximum = obj.share_exclusiveMaximum,
            r.share_exclusiveMinimum = obj.share_exclusiveMinimum
    """, batch=batch)

# ========================
# WORKER PROCESS FOR RELATIONSHIPS
# ========================
def worker_process(chunk_info, shared_counter):
    """
    Kjøres av hver prosess. Leser en del av filen og lager relasjoner.
    """
    file_path, start_byte, end_byte, worker_id = chunk_info
    
    driver = GraphDatabase.driver(URI, auth=AUTH)
    
    # Lokale batch-lister for relasjoner
    batches = {
    "describes_person": [],
    "person_has_address": [],
    "person_has_nationality": [],
    "source_has_person_statement": [], 
    "person_statement_has_identifier": [],
    "describes_entity": [],
    "entity_has_address": [],
    "source_has_entity_statement": [], 
    "entity_statement_has_identifier": [],
    "person_is_interested_party": [],
    "entity_is_interested_party": [],
    "source_has_ooc_statement": [],     
    "ooc_has_identifier": [],
    "ooc_interest_in_entity": []
}

    local_processed_count = 0

    try:
        with open(file_path, "rb") as f:
            f.seek(start_byte)
            if start_byte > 0:
                f.readline()

            while True:
                if f.tell() > end_byte:
                    break
                
                line = f.readline()
                if not line:
                    break

                try:
                    obj = orjson.loads(line)
                except:
                    continue

                stype = obj.get("statementType")
                if not stype:
                    continue

                sid = obj.get("statementID")

                # ========================
                # RELATIONSHIP MAPPING
                # ========================

                if stype == "personStatement":
                    # PersonStatement -[:DESCRIBES_PERSON]-> Person
                    batches["describes_person"].append({
                        "statementID": sid,
                        "personID": sid
                    })

                    # Person -[:HAS_ADDRESS]-> Address
                    for addr in obj.get("addresses", []):
                        if addr.get("address"):
                            batches["person_has_address"].append({
                                "personID": sid,
                                "address": addr.get("address"),
                                "country": addr.get("country"),
                                "type": addr.get("type")
                            })
                    # Person -[:HAS_NATIONALITY]-> Nationality
                    for nat in obj.get("nationalities", []):
                        if nat.get("code"):
                            batches["person_has_nationality"].append({
                                "personID": sid,
                                "code": nat.get("code")  # <- Kun code!
                            })

                  # Source -[:HAS_STATEMENT]-> Personstatement
                    source = obj.get("source")
                    if source and source.get("url"):
                        batches["source_has_person_statement"].append({
                            "statementID": sid,
                            "url": source.get("url")
                        })


                    # PersonStatement -[:HAS_IDENTIFIER]-> Identifier
                    for ident in obj.get("identifiers", []):
                        if ident.get("uri"):
                            batches["person_statement_has_identifier"].append({
                                "statementID": sid,
                                "uri": ident.get("uri")
                            })

                elif stype == "entityStatement":
                    # EntityStatement -[:DESCRIBES_ENTITY]-> Entity
                    batches["describes_entity"].append({
                        "statementID": sid,
                        "EntityID": sid
                    })

                    # Entity -[:HAS_ADDRESS]-> Address
                    for addr in obj.get("addresses", []):
                        if addr.get("address"):
                            batches["entity_has_address"].append({
                                "EntityID": sid,
                                "address": addr.get("address"),
                                "country": addr.get("country"),
                                "type": addr.get("type")
                            })

                    # Source -[:HAS_STATEMENT]-> EntityStatement
                    source = obj.get("source")
                    if source and source.get("url"):
                        batches["source_has_entity_statement"].append({
                            "statementID": sid,
                            "url": source.get("url")
                        })

                    # EntityStatement -[:HAS_IDENTIFIER]-> Identifier
                    for ident in obj.get("identifiers", []):
                        if ident.get("uri"):
                            batches["entity_statement_has_identifier"].append({
                                "statementID": sid,
                                "uri": ident.get("uri")
                            })

                elif stype == "ownershipOrControlStatement":
                    # Person/Entity -[:IS_INTERESTED_PARTY]-> OOCStatement
                    interested_party = obj.get("interestedParty", {})
                    interested_person_id = interested_party.get("describedByPersonStatement")
                    interested_entity_id = interested_party.get("describedByEntityStatement")
                    
                    if interested_person_id:
                        batches["person_is_interested_party"].append({
                            "statementID": sid,
                            "interestedParty_describedByPersonStatement": interested_person_id
                        })
                    
                    if interested_entity_id:
                        batches["entity_is_interested_party"].append({
                            "statementID": sid,
                            "interestedParty_describedByEntityStatement": interested_entity_id
                        })

                    # Source -[:HAS_OOC_STATEMENT]-> OOCStatement
                    source = obj.get("source")
                    if source and source.get("url"):
                        batches["source_has_ooc_statement"].append({
                            "statementID": sid,
                            "url": source.get("url")
                        })

                    # OOCStatement -[:HAS_IDENTIFIER]-> Identifier
                    for ident in obj.get("identifiers", []):
                        if ident.get("uri"):
                            batches["ooc_has_identifier"].append({
                                "statementID": sid,
                                "uri": ident.get("uri")
                            })

                    # FIX: OOCStatement -[:INTEREST_IN]-> Entity (subject/firma som eies)
                    subject = obj.get("subject", {})
                    subject_entity_id = subject.get("describedByEntityStatement")
                    
                    if subject_entity_id:  # Kun hvis subject (firma) finnes
                        interests = obj.get("interests", [])
                        for interest in interests:
                            share_data = interest.get("share", {})
                            
                            interest_data = {
                                "statementID": sid,
                                "subject_id": subject_entity_id,  # ← ENDRET: Peker til subject (firma som eies)
                                "interest_type": interest.get("type"),
                                "startDate": interest.get("startDate"),
                                "share_exact": parse_share(share_data.get("exact")),
                                "share_maximum": parse_share(share_data.get("maximum")),
                                "share_minimum": parse_share(share_data.get("minimum")),
                                "endDate": interest.get("endDate"),
                                "details": interest.get("interestLevel"),
                                "share_exclusiveMaximum": share_data.get("exclusiveMaximum"),
                                "share_exclusiveMinimum": share_data.get("exclusiveMinimum")
                            }
                            
                            batches["ooc_interest_in_entity"].append(interest_data)
                        
                local_processed_count += 1

                # ========================
                # FLUSH BATCHES
                # ========================
                FLUSH_EVERY = 2000
                if local_processed_count % FLUSH_EVERY == 0:
                    with driver.session() as s:
                        if len(batches["describes_person"]) >= BATCH_SIZE:
                            run_with_retry(s, create_describes_person_relationships, batches["describes_person"])
                            batches["describes_person"].clear()
                        if len(batches["person_has_address"]) >= BATCH_SIZE:
                            run_with_retry(s, create_person_has_address_relationships, batches["person_has_address"])
                            batches["person_has_address"].clear()

                        if len(batches["person_has_nationality"]) >= BATCH_SIZE:
                            run_with_retry(s, create_person_has_nationality_relationships, batches["person_has_nationality"])
                            batches["person_has_nationality"].clear()
                       
                        if len(batches["source_has_person_statement"]) >= BATCH_SIZE:
                            run_with_retry(s, create_source_has_person_statement, batches["source_has_person_statement"])
                            batches["source_has_person_statement"].clear()
                        if len(batches["person_statement_has_identifier"]) >= BATCH_SIZE:
                            run_with_retry(s, create_person_statement_has_identifier_relationships, batches["person_statement_has_identifier"])
                            batches["person_statement_has_identifier"].clear()

                        if len(batches["describes_entity"]) >= BATCH_SIZE:
                            run_with_retry(s, create_describes_entity_relationships, batches["describes_entity"])
                            batches["describes_entity"].clear()
                        if len(batches["entity_has_address"]) >= BATCH_SIZE:
                            run_with_retry(s, create_entity_has_address_relationships, batches["entity_has_address"])
                            batches["entity_has_address"].clear()
                        if len(batches["source_has_entity_statement"]) >= BATCH_SIZE:
                            run_with_retry(s, create_source_has_entity_statement, batches["source_has_entity_statement"])
                            batches["source_has_entity_statement"].clear()
                        if len(batches["entity_statement_has_identifier"]) >= BATCH_SIZE:
                            run_with_retry(s, create_entity_statement_has_identifier_relationships, batches["entity_statement_has_identifier"])
                            batches["entity_statement_has_identifier"].clear()

                        if len(batches["person_is_interested_party"]) >= BATCH_SIZE:
                            run_with_retry(s, create_person_is_interested_party_relationships, batches["person_is_interested_party"])
                            batches["person_is_interested_party"].clear()
                        if len(batches["entity_is_interested_party"]) >= BATCH_SIZE:
                            run_with_retry(s, create_entity_is_interested_party_relationships, batches["entity_is_interested_party"])
                            batches["entity_is_interested_party"].clear()
                        if len(batches["source_has_ooc_statement"]) >= BATCH_SIZE:
                            run_with_retry(s, create_source_has_ooc_statement, batches["source_has_ooc_statement"])
                            batches["source_has_ooc_statement"].clear()
                        if len(batches["ooc_has_identifier"]) >= BATCH_SIZE:
                            run_with_retry(s, create_ooc_has_identifier_relationships, batches["ooc_has_identifier"])
                            batches["ooc_has_identifier"].clear()
                        if len(batches["ooc_interest_in_entity"]) >= BATCH_SIZE:
                            run_with_retry(s, create_ooc_interest_in_entity_relationships, batches["ooc_interest_in_entity"])
                            batches["ooc_interest_in_entity"].clear()
                    
                    with shared_counter.get_lock():
                        shared_counter.value += local_processed_count
                    local_processed_count = 0

            # ========================
            # FLUSH REST
            # ========================
            with driver.session() as s:
                if batches["describes_person"]:
                    run_with_retry(s, create_describes_person_relationships, batches["describes_person"])
                if batches["person_has_address"]:
                    run_with_retry(s, create_person_has_address_relationships, batches["person_has_address"])

                if len(batches["person_has_nationality"]) >= BATCH_SIZE:
                    run_with_retry(s, create_person_has_nationality_relationships, batches["person_has_nationality"])
                            
                if batches["source_has_person_statement"]:
                    run_with_retry(s, create_source_has_person_statement, batches["source_has_person_statement"])
                if batches["person_statement_has_identifier"]:
                    run_with_retry(s, create_person_statement_has_identifier_relationships, batches["person_statement_has_identifier"])

                if batches["describes_entity"]:
                    run_with_retry(s, create_describes_entity_relationships, batches["describes_entity"])
                if batches["entity_has_address"]:
                    run_with_retry(s, create_entity_has_address_relationships, batches["entity_has_address"])
                if batches["source_has_entity_statement"]:
                    run_with_retry(s, create_source_has_entity_statement, batches["source_has_entity_statement"])
                if batches["entity_statement_has_identifier"]:
                    run_with_retry(s, create_entity_statement_has_identifier_relationships, batches["entity_statement_has_identifier"])

                if batches["person_is_interested_party"]:
                    run_with_retry(s, create_person_is_interested_party_relationships, batches["person_is_interested_party"])
                if batches["entity_is_interested_party"]:
                    run_with_retry(s, create_entity_is_interested_party_relationships, batches["entity_is_interested_party"])
                if batches["source_has_ooc_statement"]:
                    run_with_retry(s, create_source_has_ooc_statement, batches["source_has_ooc_statement"])
                if batches["ooc_has_identifier"]:
                    run_with_retry(s, create_ooc_has_identifier_relationships, batches["ooc_has_identifier"])
                if batches["ooc_interest_in_entity"]:
                    run_with_retry(s, create_ooc_interest_in_entity_relationships, batches["ooc_interest_in_entity"])
            
            if local_processed_count > 0:
                with shared_counter.get_lock():
                    shared_counter.value += local_processed_count

    except Exception as e:
        print(f"🚨 Worker {worker_id} feilet: {e}")
    finally:
        driver.close()


# ========================
# MONITOR
# ========================
def monitor_progress(shared_counter, total_file_size):
    start_time = datetime.now()
    while True:
        time.sleep(LOG_INTERVAL)
        current_count = shared_counter.value
        elapsed = datetime.now() - start_time
        print(f"📊 Processed relationships: {current_count} | Tid: {elapsed}")


# ========================
# MAIN
# ========================
if __name__ == "__main__":
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(FILE_PATH)
    
    print("🔗 FASE 2: IMPORTERER RELASJONER")
    print("=" * 50)
    
    # Verifiser tilkobling
    print("🔌 Kobler til Neo4j...")
    main_driver = GraphDatabase.driver(URI, auth=AUTH)
    main_driver.verify_connectivity()
    main_driver.close()
    
    # Forbered multiprosessering
    file_size = os.path.getsize(FILE_PATH)
    chunk_size = file_size // CPU_CORES
    
    print(f"📦 Filstørrelse: {file_size / (1024*1024):.2f} MB")
    print(f"🚀 Starter {CPU_CORES} workers for relasjoner...")

    shared_counter = Value('i', 0)
    processes = []
    start_time_total = datetime.now()

    # Start workers
    for i in range(CPU_CORES):
        start = i * chunk_size
        end = file_size if i == CPU_CORES - 1 else (i + 1) * chunk_size
        chunk_args = (FILE_PATH, start, end, i+1)
        
        p = Process(target=worker_process, args=(chunk_args, shared_counter))
        p.start()
        processes.append(p)

    # Start monitor
    monitor_p = Process(target=monitor_progress, args=(shared_counter, file_size))
    monitor_p.start()

    # Vent på workers
    for p in processes:
        p.join()
    
    monitor_p.terminate()

    # Sluttrapport
    print("\n🎉 Fase 2 ferdig (Alle relasjoner opprettet)!")
    print(f"Total tid: {datetime.now() - start_time_total}")
    print(f"Totalt antall objekter prosessert: {shared_counter.value}")

    # Sjekk relasjoner i DB
    print("\n🔍 Sjekker relasjons-tall...")
    main_driver = GraphDatabase.driver(URI, auth=AUTH)
    with main_driver.session() as s:
        describes_person = s.run("MATCH ()-[r:DESCRIBES_PERSON]->() RETURN count(r) AS c").single()["c"]
        describes_entity = s.run("MATCH ()-[r:DESCRIBES_ENTITY]->() RETURN count(r) AS c").single()["c"]
        has_nationality = s.run("MATCH ()-[r:HAS_NATIONALITY]->() RETURN count(r) AS c").single()["c"]
        has_addr = s.run("MATCH ()-[r:HAS_ADDRESS]->() RETURN count(r) AS c").single()["c"]
        src_st = s.run("MATCH ()-[r:HAS_STATEMENT]->() RETURN count(r) AS c").single()["c"]
        has_id = s.run("MATCH ()-[r:HAS_IDENTIFIER]->() RETURN count(r) AS c").single()["c"]
        is_interested = s.run("MATCH ()-[r:IS_INTERESTED_PARTY]->() RETURN count(r) AS c").single()["c"]
        interest_in = s.run("MATCH ()-[r:INTEREST_IN]->() RETURN count(r) AS c").single()["c"]
    main_driver.close()

    print(f"DESCRIBES_PERSON relasjoner: {describes_person}")
    print(f"DESCRIBES_ENTITY relasjoner: {describes_entity}")
    print(f"HAS_NATIONALITY relasjoner: {has_nationality}")
    print(f"HAS_ADDRESS relasjoner: {has_addr}")
    print(f"HAS_STATEMENT relasjoner: {src_st}")
    print(f"HAS_IDENTIFIER relasjoner: {has_id}")
    print(f"IS_INTERESTED_PARTY relasjoner: {is_interested}")
    print(f"INTEREST_IN relasjoner: {interest_in}")