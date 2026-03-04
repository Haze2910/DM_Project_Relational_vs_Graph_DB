
def generate_scales():

    import pandas as pd
    from .postgresdb.load_scale import load_postgres_scale
    from .neo4jdb.load_scale import load_neo4j_scale

    recipes     = pd.read_parquet("data/recipes_clean.parquet")
    ingredients = pd.read_parquet("data/ingredients_clean.parquet")
    nutrients   = pd.read_parquet("data/nutrients_clean.parquet")
    has_ing     = pd.read_parquet("data/has_ingredient.parquet")
    has_nut     = pd.read_parquet("data/has_nutrient.parquet")

    SCALES = [0.25, 0.50, 0.75, 1.0]

    for scale in SCALES:
        sampled_recipes = recipes.sample(frac=scale, random_state=42)
        sampled_has_ing = has_ing[has_ing["recipe_id"].isin(sampled_recipes["recipe_id"])]
        
        # only keep ingredients reachable from sampled recipes
        reachable_ingredients = sampled_has_ing["ingredient_id"].unique()
        sampled_ingredients = ingredients[ingredients["ingredient_id"].isin(reachable_ingredients)]
        sampled_has_nut = has_nut[has_nut["ingredient_id"].isin(reachable_ingredients)]
        
        # nutrients are always the same ~200, no need to filter
        
        label = int(scale * 100)
        sampled_recipes.to_parquet(f"data/scales/recipes_{label}.parquet", index=False)
        sampled_ingredients.to_parquet(f"data/scales/ingredients_{label}.parquet", index=False)
        sampled_has_ing.to_parquet(f"data/scales/has_ingredient_{label}.parquet", index=False)
        sampled_has_nut.to_parquet(f"data/scales/has_nutrient_{label}.parquet", index=False)
        
        print(f"{label}%: {len(sampled_recipes):,} recipes, {len(sampled_ingredients):,} ingredients, "
            f"{len(sampled_has_ing):,} has_ingredient, {len(sampled_has_nut):,} has_nutrient")
        
        label = int(scale * 100)
        print(f"\n=== Loading scale {label}% ===")
        print("  -> PostgreSQL...")
        load_postgres_scale(label)
        print("  -> Neo4j...")
        load_neo4j_scale(label)
        
        print(f"  Scale {scale}% done.")