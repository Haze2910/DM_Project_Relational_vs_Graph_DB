def generate_clean_data():

    import pandas as pd
    import ast


    BASE = "hf://datasets/Haze2910/Recipes"

    # --- Load data ---
    """ read from disk instead of from hugginface 
    recipes = pd.read_parquet("data/recipes.parquet")
    ingredients = pd.read_parquet("data/ingredients.parquet")
    nutrients = pd.read_parquet("data/nutrients.parquet")
    """

    recipes     = pd.read_parquet(f"{BASE}/recipes.parquet")
    ingredients = pd.read_parquet(f"{BASE}/ingredients.parquet")
    nutrients   = pd.read_parquet(f"{BASE}/nutrients.parquet")

    # --- Assign IDs ---
    ingredients = ingredients.reset_index(drop=True)
    ingredients["ingredient_id"] = ingredients.index

    recipes = recipes.rename(columns={"id": "recipe_id"})
    nutrients = nutrients.rename(columns={"attr_id": "nutrient_id"})

    # --- has_nutrient ---
    if isinstance(ingredients["full_nutrients"].iloc[0], str):
        ingredients["full_nutrients"] = ingredients["full_nutrients"].apply(ast.literal_eval)

    has_nutrient = (
        ingredients[["ingredient_id", "full_nutrients"]]
        .explode("full_nutrients")
        .assign(
            nutrient_id=lambda df: df["full_nutrients"].apply(lambda x: x["attr_id"]),
            amount=lambda df: df["full_nutrients"].apply(lambda x: x["value"])
        )
        .drop(columns="full_nutrients")
    )

    valid_nutrient_ids = set(nutrients["nutrient_id"])
    has_nutrient = has_nutrient[
        has_nutrient["nutrient_id"].isin(valid_nutrient_ids) & (has_nutrient["amount"] > 0)
    ]

    # --- has_ingredient ---
    if isinstance(recipes["ingredients"].iloc[0], str):
        recipes["ingredients"] = recipes["ingredients"].apply(ast.literal_eval)

    name_to_id = dict(zip(ingredients["ingredient_name"], ingredients["ingredient_id"]))

    has_ingredient = (
        recipes[["recipe_id", "ingredients"]]
        .explode("ingredients")
        .rename(columns={"ingredients": "ingredient_name"})
        .assign(ingredient_id=lambda df: df["ingredient_name"].map(name_to_id))
        .dropna(subset=["ingredient_id"])
        .astype({"ingredient_id": int})
        .drop(columns="ingredient_name")
        .drop_duplicates()
    )

    print(f"has_nutrient:   {len(has_nutrient):,} rows")
    print(f"has_ingredient: {len(has_ingredient):,} rows")

    # --- Save ---
    has_nutrient.to_parquet("data/has_nutrient.parquet", index=False)
    has_ingredient.to_parquet("data/has_ingredient.parquet", index=False)
    ingredients.drop(columns="full_nutrients").to_parquet("data/ingredients_clean.parquet", index=False)
    nutrients.rename(columns={"attr_id": "nutrient_id"} if "attr_id" in nutrients.columns else {}).to_parquet("data/nutrients_clean.parquet", index=False)
    recipes[["recipe_id", "name", "minutes"]].to_parquet("data/recipes_clean.parquet", index=False)

    total = recipes["ingredients"].explode().nunique()
    matched = name_to_id.keys()
    print(f"Unmatched: {recipes['ingredients'].explode()[~recipes['ingredients'].explode().isin(matched)].unique()}")

    print("Done.")