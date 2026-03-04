from data.generate_clean_data import generate_clean_data
from data.postgresdb.load import load_postgres
from data.neo4jdb.load import load_neo4j
from data.generate_scales import generate_scales
from benchmark.runner import main as run_benchmark


def main():
    print("=" * 50)
    print("Step 1/54 — Generate clean data")
    print("=" * 50)
    generate_clean_data()

    print()
    print("=" * 50)
    print("Step 2/5 — Load full dataset into PostgreSQL")
    print("=" * 50)
    load_postgres()

    print()
    print("=" * 50)
    print("Step 3/5 — Load full dataset into Neo4j")
    print("=" * 50)
    load_neo4j()

    print()
    print("=" * 50)
    print("Step 4/5 — Generate scales + load into PostgreSQL & Neo4j")
    print("=" * 50)
    generate_scales()

    print("=" * 50)
    print("Step 5/5 — Run test queries")
    print("=" * 50)
    run_benchmark()

    print()
    print("All steps completed successfully.")


if __name__ == "__main__":
    main()