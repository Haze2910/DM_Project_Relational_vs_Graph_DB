def load_neo4j_scale(scale: int):
    import pandas as pd
    from neo4j import GraphDatabase
    SCALE = scale

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    nutrients = pd.read_parquet("data/nutrients_clean.parquet")
    recipes   = pd.read_parquet(f"data/scales/recipes_{SCALE}.parquet")
    ings      = pd.read_parquet(f"data/scales/ingredients_{SCALE}.parquet")
    has_ing   = pd.read_parquet(f"data/scales/has_ingredient_{SCALE}.parquet")
    has_nut   = pd.read_parquet(f"data/scales/has_nutrient_{SCALE}.parquet")

    BATCH_SIZE = 500

    def batch(df):
        records = df.to_dict(orient="records")
        for i in range(0, len(records), BATCH_SIZE):
            yield records[i:i + BATCH_SIZE]

    with driver.session(database="neo4j") as session:
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (r:Recipe)     REQUIRE r.recipe_id     IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (i:Ingredient) REQUIRE i.ingredient_id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Nutrient)   REQUIRE n.nutrient_id   IS UNIQUE")

        # Tag each recipe with the scales it belongs to
        # We accumulate: a recipe in 25% sample gets [25, 50, 75, 100]
        # a recipe only in 50% gets [50, 75, 100], etc.
        for b in batch(recipes):
            session.run("""
                UNWIND $rows AS row
                MERGE (r:Recipe {recipe_id: row.recipe_id})
                SET r.name = row.name, r.minutes = row.minutes
                WITH r
                WHERE NOT $scale IN coalesce(r.scales, [])
                SET r.scales = coalesce(r.scales, []) + $scale
            """, rows=b, scale=SCALE)
        print("Recipes loaded.")

        for b in batch(ings):
            session.run("""
                UNWIND $rows AS row
                MERGE (i:Ingredient {ingredient_id: row.ingredient_id})
                SET i.name = row.ingredient_name, i.serving_unit = row.serving_unit, i.serving_qty = row.serving_qty
            """, rows=b)
        print("Ingredients loaded.")

        for b in batch(nutrients):
            session.run("""
                UNWIND $rows AS row
                MERGE (n:Nutrient {nutrient_id: row.nutrient_id})
                SET n.name = row.name, n.usda_tag = row.usda_tag, n.unit = row.unit
            """, rows=b)
        print("Nutrients loaded.")

        for b in batch(has_ing):
            session.run("""
                UNWIND $rows AS row
                MATCH (r:Recipe     {recipe_id:     row.recipe_id})
                MATCH (i:Ingredient {ingredient_id: row.ingredient_id})
                MERGE (r)-[:HAS_INGREDIENT]->(i)
            """, rows=b)
        print("HAS_INGREDIENT loaded.")

        for b in batch(has_nut):
            session.run("""
                UNWIND $rows AS row
                MATCH (i:Ingredient {ingredient_id: row.ingredient_id})
                MATCH (n:Nutrient   {nutrient_id:   row.nutrient_id})
                MERGE (i)-[:HAS_NUTRIENT {amount: row.amount}]->(n)
            """, rows=b)
        print("HAS_NUTRIENT loaded.")

    driver.close()
    print(f"Neo4j scale {SCALE} done.")