def load_postgres_scale(scale: int):
    import pandas as pd
    from sqlalchemy import create_engine, text
    from .utils import upsert
    SCALE = scale

    engine = create_engine("postgresql://user:password@localhost:5432/recipes_db")

    schema = f"""
    CREATE SCHEMA IF NOT EXISTS scale_{SCALE};

    CREATE TABLE IF NOT EXISTS scale_{SCALE}.recipe (
        recipe_id INTEGER PRIMARY KEY,
        name      TEXT,
        minutes   INTEGER
    );
    CREATE TABLE IF NOT EXISTS scale_{SCALE}.ingredient (
        ingredient_id INTEGER PRIMARY KEY,
        name          TEXT,
        serving_unit  TEXT,
        serving_qty   FLOAT
    );
    CREATE TABLE IF NOT EXISTS scale_{SCALE}.nutrient (
        nutrient_id INTEGER PRIMARY KEY,
        usda_tag    TEXT,
        name        TEXT,
        unit        TEXT
    );
    CREATE TABLE IF NOT EXISTS scale_{SCALE}.has_ingredient (
        recipe_id     INTEGER REFERENCES scale_{SCALE}.recipe(recipe_id),
        ingredient_id INTEGER REFERENCES scale_{SCALE}.ingredient(ingredient_id),
        PRIMARY KEY (recipe_id, ingredient_id)
    );
    CREATE TABLE IF NOT EXISTS scale_{SCALE}.has_nutrient (
        ingredient_id INTEGER REFERENCES scale_{SCALE}.ingredient(ingredient_id),
        nutrient_id   INTEGER REFERENCES scale_{SCALE}.nutrient(nutrient_id),
        amount        FLOAT,
        PRIMARY KEY (ingredient_id, nutrient_id)
    );
    """

    with engine.connect() as conn:
        conn.execute(text(schema))
        conn.commit()
        print(f"Schema scale_{SCALE} created.")

    nutrients = pd.read_parquet("data/nutrients_clean.parquet")
    recipes   = pd.read_parquet(f"data/scales/recipes_{SCALE}.parquet")
    ings      = pd.read_parquet(f"data/scales/ingredients_{SCALE}.parquet").rename(columns={"ingredient_name": "name"})
    has_ing   = pd.read_parquet(f"data/scales/has_ingredient_{SCALE}.parquet")
    has_nut   = pd.read_parquet(f"data/scales/has_nutrient_{SCALE}.parquet")

    for df, table in [
        (recipes,  f"scale_{SCALE}.recipe"),
        (ings,     f"scale_{SCALE}.ingredient"),
        (nutrients,f"scale_{SCALE}.nutrient"),
        (has_ing,  f"scale_{SCALE}.has_ingredient"),
        (has_nut,  f"scale_{SCALE}.has_nutrient"),
    ]:
        df.to_sql(table.split(".")[1], engine, schema=f"scale_{SCALE}", if_exists="append", index=False, method=upsert)
        print(f"Loaded {table}")

    print(f"PostgreSQL scale_{SCALE} done.")