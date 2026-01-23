#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from json import JSONDecoder
from functools import partial
from typing import Any, Dict, Generator, List, Optional, Tuple

from neo4j import GraphDatabase


# -----------------------
# Config (din)
# -----------------------
URI = "bolt://127.0.0.1:7687"
AUTH = ("neo4j", "test1234")

FILE_PATH = "/Users/schuttyh/Documents/GitHub/Bachelor/10000.json"

BATCH_SIZE = 2000
LOG_EVERY = 10000


# -----------------------
# Streaming JSON parser
# -----------------------
def json_parse(fileobj, decoder=JSONDecoder(), maxbuffer: int = 1 << 20) -> Generator[Dict[str, Any], None, None]:
    """
    Leser vilkårlige JSON-objekter fra en fil der objektene kan ligge etter hverandre.
    Antar ikke at objektene er per linje, og tåler whitespace mellom objekter.
    """
    data = ""
    for chunk in iter(partial(fileobj.read, maxbuffer), ""):
        data += chunk
        while data:
            try:
                cleaned = data.lstrip()
                obj, idx = decoder.raw_decode(cleaned)
                yield obj
                data = cleaned[idx:]
            except ValueError:
                # trenger mer data
                break


def safe_str(x: Any) -> Optional[str]:
    return None if x is None else str(x)


def get_path(d: Any, path: str) -> Any:
    """Hent nested felt med 'a.b.c' syntaks. Returnerer None hvis mangler."""
    cur = d
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def best_name(obj: Dict[str, Any]) -> Optional[str]:
    # entityStatement har ofte "name"
    if obj.get("name") is not None:
        return str(obj["name"])

    # personStatement har ofte names[0].fullName
    names = obj.get("names")
    if isinstance(names, list) and names:
        n0 = names[0] if isinstance(names[0], dict) else None
        if n0 and n0.get("fullName") is not None:
            return str(n0["fullName"])

    return None


def first_identifier(obj: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Returnerer (identifier_id, scheme/schemeName) fra identifiers[0], hvis finnes.
    """
    ids = obj.get("identifiers")
    if isinstance(ids, list) and ids:
        first = ids[0] if isinstance(ids[0], dict) else None
        if first:
            _id = first.get("id")
            scheme = first.get("scheme") or first.get("schemeName")
            return (safe_str(_id), safe_str(scheme))
    return None, None


# -----------------------
# Neo4j Cypher
# -----------------------
CREATE_CONSTRAINTS = [
    # Én stabil node-nøkkel for alle statement-IDs
    "CREATE CONSTRAINT oo_id_unique IF NOT EXISTS FOR (n:OO) REQUIRE n.id IS UNIQUE",
]

# Opprett/oppdater node (OO + label basert på statementType)
UPSERT_NODES_CYPHER = """
UNWIND $rows AS row
MERGE (n:OO {id: row.id})
SET n.statementType = coalesce(n.statementType, row.statementType),
    n.statementID   = coalesce(n.statementID, row.statementID),
    n.name          = coalesce(n.name, row.name),
    n.identifier    = coalesce(n.identifier, row.identifier),
    n.idScheme      = coalesce(n.idScheme, row.idScheme)
WITH n, row
FOREACH (_ IN CASE WHEN row.statementType = 'personStatement' THEN [1] ELSE [] END | SET n:Person)
FOREACH (_ IN CASE WHEN row.statementType = 'entityStatement' THEN [1] ELSE [] END | SET n:Entity)
FOREACH (_ IN CASE WHEN row.statementType = 'ownershipOrControlStatement' THEN [1] ELSE [] END | SET n:OwnershipStatement)
"""

# Opprett/oppdater relasjoner (person -> entity)
UPSERT_EDGES_CYPHER = """
UNWIND $rows AS row
MERGE (src:OO {id: row.src})
MERGE (dst:OO {id: row.dst})
MERGE (src)-[r:OWNS_OR_CONTROLS {statementID: row.statementID}]->(dst)
SET r.statementDate = coalesce(r.statementDate, row.statementDate),
    r.startDate      = coalesce(r.startDate, row.startDate),
    r.interestTypes  = coalesce(r.interestTypes, row.interestTypes),
    r.shareExact     = coalesce(r.shareExact, row.shareExact),
    r.shareMin       = coalesce(r.shareMin, row.shareMin),
    r.shareMax       = coalesce(r.shareMax, row.shareMax)
"""

# -----------------------
# Extract graph from file
# -----------------------
def extract_nodes_edges(file_path: str) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Leser hele fila og bygger:
      - nodes: dict id -> props
      - edges: liste med relasjoner (person->entity)
      - summary: tellinger
    """
    nodes: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []
    edges_seen: set[Tuple[str, str, str]] = set()  # (src,dst,statementID)

    summary: Dict[str, Any] = {
        "objects_total": 0,
        "nodes_total": 0,
        "edges_total": 0,
        "statement_types": {},
        "edges_by_type": {},
    }

    def ensure_node(node_id: str, updates: Dict[str, Any]) -> None:
        if node_id not in nodes:
            nodes[node_id] = {"id": node_id}
        for k, v in updates.items():
            if v is None:
                continue
            if nodes[node_id].get(k) is None:
                nodes[node_id][k] = v

    with open(file_path, "r", encoding="utf-8") as f:
        for obj in json_parse(f):
            summary["objects_total"] += 1

            stype = safe_str(obj.get("statementType")) or "UNKNOWN"
            summary["statement_types"][stype] = summary["statement_types"].get(stype, 0) + 1

            statement_id = safe_str(obj.get("statementID"))
            if statement_id:
                ident, scheme = first_identifier(obj)
                ensure_node(statement_id, {
                    "statementType": stype,
                    "statementID": statement_id,
                    "name": best_name(obj),
                    "identifier": ident,
                    "idScheme": scheme,
                })

            # Edge discovery (data-drevet): hvis begge feltene finnes -> lag relasjon
            src_ref = get_path(obj, "interestedParty.describedByPersonStatement")
            dst_ref = get_path(obj, "subject.describedByEntityStatement")
            if src_ref is not None and dst_ref is not None:
                src_id = safe_str(src_ref)
                dst_id = safe_str(dst_ref)
                if not src_id or not dst_id:
                    continue

                # Sørg for at endepunktene finnes som nodes (placeholder hvis ukjent)
                ensure_node(src_id, {"statementType": "personStatement"})
                ensure_node(dst_id, {"statementType": "entityStatement"})

                # Edge-metadata
                interests = obj.get("interests")
                interest_types = None
                start_date = None
                share_exact = share_min = share_max = None

                if isinstance(interests, list) and interests:
                    types = []
                    for it in interests:
                        if not isinstance(it, dict):
                            continue
                        if it.get("type") is not None:
                            types.append(str(it["type"]))
                        if start_date is None and it.get("startDate") is not None:
                            start_date = str(it["startDate"])
                        share = it.get("share")
                        if isinstance(share, dict):
                            if share_exact is None and share.get("exact") is not None:
                                share_exact = str(share["exact"])
                            if share_min is None and share.get("minimum") is not None:
                                share_min = str(share["minimum"])
                            if share_max is None and share.get("maximum") is not None:
                                share_max = str(share["maximum"])
                    if types:
                        interest_types = ",".join(sorted(set(types)))

                edge_statement_id = statement_id or safe_str(obj.get("statementID"))  # ownershipOrControlStatement id
                edge_key = (src_id, dst_id, edge_statement_id or "")
                if edge_key in edges_seen:
                    continue
                edges_seen.add(edge_key)

                e = {
                    "src": src_id,
                    "dst": dst_id,
                    "statementID": edge_statement_id,
                    "statementDate": safe_str(obj.get("statementDate")),
                    "startDate": start_date,
                    "interestTypes": interest_types,
                    "shareExact": share_exact,
                    "shareMin": share_min,
                    "shareMax": share_max,
                }
                edges.append(e)

                summary["edges_total"] += 1
                summary["edges_by_type"]["OWNS_OR_CONTROLS"] = summary["edges_by_type"].get("OWNS_OR_CONTROLS", 0) + 1

            if LOG_EVERY and summary["objects_total"] % LOG_EVERY == 0:
                print(f"[parse] objects={summary['objects_total']:,} nodes={len(nodes):,} edges={len(edges):,}")

    summary["nodes_total"] = len(nodes)
    return nodes, edges, summary


# -----------------------
# Neo4j load (batch)
# -----------------------
def chunks(lst: List[Dict[str, Any]], n: int) -> Generator[List[Dict[str, Any]], None, None]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def ensure_constraints(driver) -> None:
    with driver.session() as session:
        for q in CREATE_CONSTRAINTS:
            session.run(q)


def load_nodes(driver, nodes: Dict[str, Dict[str, Any]]) -> None:
    rows = list(nodes.values())
    total = len(rows)
    print(f"[neo4j] loading nodes: {total:,} (batch={BATCH_SIZE})")
    with driver.session() as session:
        for i, batch in enumerate(chunks(rows, BATCH_SIZE), start=1):
            session.run(UPSERT_NODES_CYPHER, rows=batch)
            if i % max(1, (LOG_EVERY // max(BATCH_SIZE, 1))) == 0:
                done = min(i * BATCH_SIZE, total)
                print(f"[neo4j] nodes done: {done:,}/{total:,}")


def load_edges(driver, edges: List[Dict[str, Any]]) -> None:
    total = len(edges)
    print(f"[neo4j] loading edges: {total:,} (batch={BATCH_SIZE})")
    with driver.session() as session:
        for i, batch in enumerate(chunks(edges, BATCH_SIZE), start=1):
            session.run(UPSERT_EDGES_CYPHER, rows=batch)
            if i % max(1, (LOG_EVERY // max(BATCH_SIZE, 1))) == 0:
                done = min(i * BATCH_SIZE, total)
                print(f"[neo4j] edges done: {done:,}/{total:,}")


# -----------------------
# Main
# -----------------------
def main():
    print(f"[config] FILE_PATH={FILE_PATH}")
    print(f"[config] URI={URI} AUTH=({AUTH[0]}, ******) BATCH_SIZE={BATCH_SIZE} LOG_EVERY={LOG_EVERY}")

    # 1) Parse file -> discover nodes/edges
    nodes, edges, summary = extract_nodes_edges(FILE_PATH)
    print("[summary]")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    # 2) Connect Neo4j
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        # 3) Ensure constraints
        ensure_constraints(driver)

        # 4) Load nodes then edges (anbefalt)
        load_nodes(driver, nodes)
        load_edges(driver, edges)

        print("[done] loaded into Neo4j.")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
