import time
import json
import pandas as pd
import psycopg2
from neo4j import GraphDatabase
from .queries import QUERIES

# --- Config ---
SCALES      = [25, 50, 75, 100]
N_RUNS      = 10
PG_BASE     = "postgresql://user:password@localhost:5432/recipes_db"
NEO4J_URI   = "bolt://localhost:7687"
NEO4J_AUTH  = ("neo4j", "password")

FIXED_PARAMS = {
    "recipe_id": 105275,
    "nutrient_id": 203,    # protein
    "amount": 10.0,
    "min_time": 10,
    "max_time": 60,
    "max_hops": 2,
}

# --- Helpers ---
def resolve_params(params: dict) -> dict:
    return {k: FIXED_PARAMS.get(k, v) for k, v in params.items()}

def pg_connect(scale):
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname="recipes_db",
        user="user", password="password",
        options=f"-c search_path=scale_{scale}"
    )

# --- PostgreSQL ---
def run_pg_timing(conn, sql, params, n_runs):
    times = []
    with conn.cursor() as cur:
        for _ in range(n_runs):
            start = time.perf_counter()
            cur.execute(sql, params)
            cur.fetchall()
            times.append(time.perf_counter() - start)
    return times

def run_pg_explain(conn, sql, params):
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}", params)
        plan = cur.fetchone()[0][0]  # returns list with one element
    return {
        "total_cost":        plan["Plan"]["Total Cost"],
        "actual_total_time": plan["Plan"]["Actual Total Time"],
        "rows":              plan["Plan"]["Actual Rows"],
        "node_type":         plan["Plan"]["Node Type"],
        "plan_raw":          json.dumps(plan),
    }

# --- Neo4j ---
def run_neo4j_timing(session, cypher, params, n_runs):
    times = []
    for _ in range(n_runs):
        start = time.perf_counter()
        result = session.run(cypher, **params)
        result.consume()  # ensure query completes
        times.append(time.perf_counter() - start)
    return times

def _sum_profile(plan):
    hits = plan.get("dbHits", 0)
    rows = plan.get("rows", 0)
    pch  = plan.get("pageCacheHits", 0)
    pcm  = plan.get("pageCacheMisses", 0)
    for child in plan.get("children", []):
        h, r, pc, pm = _sum_profile(child)
        hits += h; rows += r; pch += pc; pcm += pm
    return hits, rows, pch, pcm

def run_neo4j_profile(session, cypher, params):
    result  = session.run(f"PROFILE {cypher}", **params)
    result.data()              # ← materialize all rows first
    summary = result.consume() # ← now profile stats are populated
    if summary.profile is None:
        return {"db_hits": 0, "rows": 0, "page_cache_hits": 0, "page_cache_misses": 0}
    db_hits, rows, pch, pcm = _sum_profile(summary.profile)
    return {
        "db_hits":           db_hits,
        "rows":              rows,
        "page_cache_hits":   pch,
        "page_cache_misses": pcm,
    }

# --- Main ---
def main():
    timing_rows = []
    plan_rows   = []

    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

    for scale in SCALES:
        print(f"\n=== Scale {scale}% ===")
        pg_conn = pg_connect(scale)

        with neo4j_driver.session(database="neo4j") as neo4j_session:

            for q in QUERIES:
                print(f"  [{q['category']}] {q['id']}")

                params = resolve_params(q["params"])
                pg_params  = dict(params)
                neo_params = {**params, "scale": scale}
                cypher = q["cypher"].replace("{max_hops}", str(neo_params.pop("max_hops", 2)))

                # --- Timing ---
                try:
                    pg_times = run_pg_timing(pg_conn, q["sql"], pg_params, N_RUNS)
                except Exception as e:
                    print(f"    PG timing error: {e}")
                    pg_times = [None] * N_RUNS

                try:
                    neo_times = run_neo4j_timing(neo4j_session, cypher, neo_params, N_RUNS)
                except Exception as e:
                    print(f"    Neo4j timing error: {e}")
                    neo_times = [None] * N_RUNS

                for run_i, (pg_t, neo_t) in enumerate(zip(pg_times, neo_times)):
                    timing_rows.append({
                        "query_id":   q["id"],
                        "category":   q["category"],
                        "scale":      scale,
                        "run":        run_i,
                        "pg_time_s":  pg_t,
                        "neo_time_s": neo_t,
                    })

                # --- Plan analysis (once per query per scale) ---
                pg_conn.rollback()  # reset after explain
                try:
                    pg_plan = run_pg_explain(pg_conn, q["sql"], pg_params)
                except Exception as e:
                    print(f"    PG explain error: {e}")
                    pg_plan = {}

                try:
                    neo_plan = run_neo4j_profile(neo4j_session, cypher, neo_params)
                except Exception as e:
                    print(f"    Neo4j profile error: {e}")
                    neo_plan = {}

                plan_rows.append({
                    "query_id": q["id"],
                    "category": q["category"],
                    "scale":    scale,
                    **{f"pg_{k}":  v for k, v in pg_plan.items()},
                    **{f"neo_{k}": v for k, v in neo_plan.items()},
                })

        pg_conn.close()

    neo4j_driver.close()

    # --- Save results ---
    timing_df = pd.DataFrame(timing_rows)
    plan_df   = pd.DataFrame(plan_rows)

    timing_df.to_csv("benchmark/results/timing.csv", index=False)
    plan_df.to_csv("benchmark/results/plans.csv",   index=False)

    print("\nResults saved to benchmark/results/")

if __name__ == "__main__":
    main()