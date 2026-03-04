def load_postgres():
    import pandas as pd
    from sqlalchemy import create_engine, text
    from .utils import upsert

    engine = create_engine("postgresql://user:password@localhost:5432/recipes_db")

    # --- Schema ---
    schema = """
    CREATE TABLE IF NOT EXISTS recipe (
        recipe_id     INTEGER PRIMARY KEY,
        name          TEXT,
        minutes       INTEGER
    );

    CREATE TABLE IF NOT EXISTS ingredient (
        ingredient_id INTEGER PRIMARY KEY,
        name          TEXT,
        serving_unit  TEXT,
        serving_qty   FLOAT
    );

    CREATE TABLE IF NOT EXISTS nutrient (
        nutrient_id   INTEGER PRIMARY KEY,
        usda_tag      TEXT,
        name          TEXT,
        unit          TEXT
    );

    CREATE TABLE IF NOT EXISTS has_ingredient (
        recipe_id     INTEGER REFERENCES recipe(recipe_id),
        ingredient_id INTEGER REFERENCES ingredient(ingredient_id),
        PRIMARY KEY (recipe_id, ingredient_id)
    );

    CREATE TABLE IF NOT EXISTS has_nutrient (
        ingredient_id INTEGER REFERENCES ingredient(ingredient_id),
        nutrient_id   INTEGER REFERENCES nutrient(nutrient_id),
        amount        FLOAT,
        PRIMARY KEY (ingredient_id, nutrient_id)
    );
    """

    with engine.connect() as conn:
        conn.execute(text(schema))
        conn.commit()
        print("Schema created.")

    # --- Load data ---
    recipes     = pd.read_parquet("data/recipes_clean.parquet")
    ingredients = pd.read_parquet("data/ingredients_clean.parquet")
    ingredients = ingredients.rename(columns={"ingredient_name": "name"})
    nutrients   = pd.read_parquet("data/nutrients_clean.parquet")
    has_ing     = pd.read_parquet("data/has_ingredient.parquet")
    has_nut     = pd.read_parquet("data/has_nutrient.parquet")

    recipes.to_sql("recipe",         engine, if_exists="append", index=False, method=upsert)
    ingredients.to_sql("ingredient", engine, if_exists="append", index=False, method=upsert)
    nutrients.to_sql("nutrient",     engine, if_exists="append", index=False, method=upsert)
    has_ing.to_sql("has_ingredient", engine, if_exists="append", index=False, method=upsert)
    has_nut.to_sql("has_nutrient",   engine, if_exists="append", index=False, method=upsert)

    print("PostgreSQL loading done.")