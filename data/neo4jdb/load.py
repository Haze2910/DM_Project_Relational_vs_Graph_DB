def load_neo4j():
    import pandas as pd
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    recipes     = pd.read_parquet("data/recipes_clean.parquet")
    ingredients = pd.read_parquet("data/ingredients_clean.parquet")
    ingredients = ingredients.rename(columns={"ingredient_name": "name"})
    nutrients   = pd.read_parquet("data/nutrients_clean.parquet")
    has_ing     = pd.read_parquet("data/has_ingredient.parquet")
    has_nut     = pd.read_parquet("data/has_nutrient.parquet")

    BATCH_SIZE = 500

    def batch(df):
        records = df.to_dict(orient="records")
        for i in range(0, len(records), BATCH_SIZE):
            yield records[i:i + BATCH_SIZE]

    with driver.session() as session:

        # --- Constraints (also create indexes automatically) ---
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (r:Recipe)     REQUIRE r.recipe_id     IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (i:Ingredient) REQUIRE i.ingredient_id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Nutrient)   REQUIRE n.nutrient_id   IS UNIQUE")
        print("Constraints created.")

        # --- Nodes ---
        for b in batch(recipes):
            session.run("""
                UNWIND $rows AS row
                MERGE (r:Recipe {recipe_id: row.recipe_id})
                SET r.name = row.name, r.minutes = row.minutes
            """, rows=b)
        print("Recipes loaded.")

        for b in batch(ingredients):
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

        # --- Edges ---
        for b in batch(has_ing):
            session.run("""
                UNWIND $rows AS row
                MATCH (r:Recipe     {recipe_id:     row.recipe_id})
                MATCH (i:Ingredient {ingredient_id: row.ingredient_id})
                MERGE (r)-[:HAS_INGREDIENT]->(i)
            """, rows=b)
        print("HAS_INGREDIENT edges loaded.")

        for b in batch(has_nut):
            session.run("""
                UNWIND $rows AS row
                MATCH (i:Ingredient {ingredient_id: row.ingredient_id})
                MATCH (n:Nutrient   {nutrient_id:   row.nutrient_id})
                MERGE (i)-[:HAS_NUTRIENT {amount: row.amount}]->(n)
            """, rows=b)
        print("HAS_NUTRIENT edges loaded.")

    driver.close()
    print("Neo4j loading done.")