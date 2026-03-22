# benchmark/queries.py

QUERIES = [
    # -------------------------------------------------------------------------
    # MULTI-HOP TRAVERSAL
    # -------------------------------------------------------------------------
    {
        "id": "multihop_nutrients_for_recipe",
        "category": "multi-hop",
        "description": "Get all nutrients for a given recipe (2-hop)",
        "sql": """
            SELECT n.name, n.unit, hn.amount
            FROM has_ingredient hi
            JOIN has_nutrient hn ON hi.ingredient_id = hn.ingredient_id
            JOIN nutrient n      ON hn.nutrient_id   = n.nutrient_id
            WHERE hi.recipe_id = %(recipe_id)s
        """,
        "cypher": """
            MATCH (r:Recipe {recipe_id: $recipe_id})-[:HAS_INGREDIENT]->(i:Ingredient)-[hn:HAS_NUTRIENT]->(n:Nutrient)
            WHERE $scale IN r.scales
            RETURN n.name, n.unit, hn.amount
        """,
        "params": {"recipe_id": None},
    },
    {
        "id": "multihop_recipes_sharing_ingredient",
        "category": "multi-hop",
        "description": "Find all recipes that share at least one ingredient with a given recipe",
        "sql": """
            SELECT DISTINCT r2.recipe_id, r2.name
            FROM has_ingredient hi1
            JOIN has_ingredient hi2 ON hi1.ingredient_id = hi2.ingredient_id
            JOIN recipe r2          ON hi2.recipe_id     = r2.recipe_id
            WHERE hi1.recipe_id = %(recipe_id)s
              AND hi2.recipe_id != %(recipe_id)s
        """,
        "cypher": """
            MATCH (r1:Recipe {recipe_id: $recipe_id})-[:HAS_INGREDIENT]->(i:Ingredient)<-[:HAS_INGREDIENT]-(r2:Recipe)
            WHERE $scale IN r1.scales AND $scale IN r2.scales AND r2.recipe_id <> $recipe_id
            RETURN DISTINCT r2.recipe_id, r2.name
        """,
        "params": {"recipe_id": None},
    },
    {
        "id": "multihop_filter_mid_traversal",
        "category": "multi-hop",
        "description": "Recipes containing an ingredient with more than X amount of a given nutrient",
        "sql": """
            SELECT DISTINCT r.recipe_id, r.name
            FROM recipe r
            JOIN has_ingredient hi ON r.recipe_id      = hi.recipe_id
            JOIN has_nutrient hn   ON hi.ingredient_id = hn.ingredient_id
            WHERE hn.nutrient_id = %(nutrient_id)s
              AND hn.amount > %(amount)s
        """,
        "cypher": """
            MATCH (r:Recipe)-[:HAS_INGREDIENT]->(i:Ingredient)-[hn:HAS_NUTRIENT]->(n:Nutrient {nutrient_id: $nutrient_id})
            WHERE $scale IN r.scales AND hn.amount > $amount
            RETURN DISTINCT r.recipe_id, r.name
        """,
        "params": {"nutrient_id": None, "amount": 10.0},
    },

    # -------------------------------------------------------------------------
    # AGGREGATION
    # -------------------------------------------------------------------------
    {
        "id": "agg_total_nutrient_in_recipe",
        "category": "aggregation",
        "description": "Total amount of a specific nutrient across all ingredients of a recipe",
        "sql": """
            SELECT SUM(hn.amount) AS total
            FROM has_ingredient hi
            JOIN has_nutrient hn ON hi.ingredient_id = hn.ingredient_id
            WHERE hi.recipe_id   = %(recipe_id)s
              AND hn.nutrient_id = %(nutrient_id)s
        """,
        "cypher": """
            MATCH (r:Recipe {recipe_id: $recipe_id})-[:HAS_INGREDIENT]->(i:Ingredient)-[hn:HAS_NUTRIENT]->(n:Nutrient {nutrient_id: $nutrient_id})
            WHERE $scale IN r.scales
            RETURN sum(hn.amount) AS total
        """,
        "params": {"recipe_id": None, "nutrient_id": None},
    },
    {
        "id": "agg_avg_nutrient_per_recipe",
        "category": "aggregation",
        "description": "Top 10 recipes by average nutrient amount across all ingredients and nutrients",
        "sql": """
            SELECT r.recipe_id, r.name,
                AVG(hn.amount)                   AS avg_nutrient_amount,
                COUNT(DISTINCT hi.ingredient_id) AS n_ingredients,
                COUNT(DISTINCT hn.nutrient_id)   AS n_nutrients
            FROM recipe r
            JOIN has_ingredient hi ON r.recipe_id      = hi.recipe_id
            JOIN has_nutrient   hn ON hi.ingredient_id = hn.ingredient_id
            GROUP BY r.recipe_id, r.name
            ORDER BY avg_nutrient_amount DESC
            LIMIT 10
        """,
        "cypher": """
            MATCH (r:Recipe)-[:HAS_INGREDIENT]->(i:Ingredient)-[hn:HAS_NUTRIENT]->(n:Nutrient)
            WHERE $scale IN r.scales
            RETURN r.recipe_id, r.name,
                avg(hn.amount)        AS avg_nutrient_amount,
                count(DISTINCT i)     AS n_ingredients,
                count(DISTINCT n)     AS n_nutrients
            ORDER BY avg_nutrient_amount DESC
            LIMIT 10
        """,
        "params": {},
    },
    {
        "id": "agg_recipes_by_prep_time",
        "category": "aggregation",
        "description": "Recipes with prep time in range, ordered by number of ingredients",
        "sql": """
            SELECT r.recipe_id, r.name, r.minutes, COUNT(hi.ingredient_id) AS n_ingredients
            FROM recipe r
            JOIN has_ingredient hi ON r.recipe_id = hi.recipe_id
            WHERE r.minutes BETWEEN %(min_time)s AND %(max_time)s
            GROUP BY r.recipe_id, r.name, r.minutes
            ORDER BY n_ingredients DESC
        """,
        "cypher": """
            MATCH (r:Recipe)-[:HAS_INGREDIENT]->(i:Ingredient)
            WHERE $scale IN r.scales AND r.minutes >= $min_time AND r.minutes <= $max_time
            RETURN r.recipe_id, r.name, r.minutes, count(i) AS n_ingredients
            ORDER BY n_ingredients DESC
        """,
        "params": {"min_time": 10, "max_time": 60},
    },

    # -------------------------------------------------------------------------
    # MIXED
    # -------------------------------------------------------------------------
    {
        "id": "mixed_most_used_ingredients",
        "category": "mixed",
        "description": "Most used ingredients across all recipes (top 10)",
        "sql": """
            SELECT i.name, COUNT(hi.recipe_id) AS recipe_count
            FROM ingredient i
            JOIN has_ingredient hi ON i.ingredient_id = hi.ingredient_id
            GROUP BY i.ingredient_id, i.name
            ORDER BY recipe_count DESC
            LIMIT 10
        """,
        "cypher": """
            MATCH (r:Recipe)-[:HAS_INGREDIENT]->(i:Ingredient)
            WHERE $scale IN r.scales
            RETURN i.name, count(r) AS recipe_count
            ORDER BY recipe_count DESC
            LIMIT 10
        """,
        "params": {},
    },
    {
        "id": "mixed_ingredient_cooccurrence",
        "category": "mixed",
        "description": "Top 10 ingredient pairs that co-occur most across recipes",
        "sql": """
            SELECT i1.name, i2.name, COUNT(DISTINCT hi1.recipe_id) AS co_occurrences
            FROM has_ingredient hi1
            JOIN has_ingredient hi2 ON hi1.recipe_id     = hi2.recipe_id
                                    AND hi1.ingredient_id < hi2.ingredient_id
            JOIN ingredient i1      ON hi1.ingredient_id = i1.ingredient_id
            JOIN ingredient i2      ON hi2.ingredient_id = i2.ingredient_id
            GROUP BY i1.ingredient_id, i1.name, i2.ingredient_id, i2.name
            ORDER BY co_occurrences DESC
            LIMIT 10
        """,
        "cypher": """
            MATCH (i1:Ingredient)<-[:HAS_INGREDIENT]-(r:Recipe)-[:HAS_INGREDIENT]->(i2:Ingredient)
            WHERE $scale IN r.scales AND i1.ingredient_id < i2.ingredient_id
            RETURN i1.name, i2.name, count(DISTINCT r) AS co_occurrences
            ORDER BY co_occurrences DESC
            LIMIT 10
        """,
        "params": {},
    },
    {
        "id": "mixed_recipes_above_avg_nutrient",
        "category": "mixed",
        "description": "Recipes whose average nutrient amount exceeds the global average",
        "sql": """
            WITH global_avg AS (
                SELECT AVG(hn.amount) AS avg_amount
                FROM has_ingredient hi
                JOIN has_nutrient hn ON hi.ingredient_id = hn.ingredient_id
            )
            SELECT r.recipe_id, r.name, AVG(hn.amount) AS avg_nutrient_amount
            FROM recipe r
            JOIN has_ingredient hi ON r.recipe_id      = hi.recipe_id
            JOIN has_nutrient   hn ON hi.ingredient_id = hn.ingredient_id
            GROUP BY r.recipe_id, r.name
            HAVING AVG(hn.amount) > (SELECT avg_amount FROM global_avg)
            ORDER BY avg_nutrient_amount DESC
            LIMIT 10
        """,
        "cypher": """
            MATCH (r:Recipe)-[:HAS_INGREDIENT]->(i:Ingredient)-[hn:HAS_NUTRIENT]->(n:Nutrient)
            WHERE $scale IN r.scales
            WITH avg(hn.amount) AS global_avg
            MATCH (r2:Recipe)-[:HAS_INGREDIENT]->(i2:Ingredient)-[hn2:HAS_NUTRIENT]->(n2:Nutrient)
            WHERE $scale IN r2.scales
            WITH r2, avg(hn2.amount) AS avg_amount, global_avg
            WHERE avg_amount > global_avg
            RETURN r2.recipe_id, r2.name, avg_amount
            ORDER BY avg_amount DESC
            LIMIT 10
        """,
        "params": {},
    },

    # -------------------------------------------------------------------------
    # VARIABLE LENGTH
    # -------------------------------------------------------------------------
    {
        "id": "varlen_reachable_recipes",
        "category": "variable-length",
        "description": "Count distinct recipes reachable within N hops via shared ingredients",
        "sql": """
            WITH RECURSIVE bfs(recipe_id, depth, visited) AS (
                SELECT DISTINCT
                    hi2.recipe_id,
                    1,
                    ARRAY[%(recipe_id)s, hi2.recipe_id]
                FROM has_ingredient hi1
                JOIN has_ingredient hi2 ON hi1.ingredient_id = hi2.ingredient_id
                WHERE hi1.recipe_id = %(recipe_id)s
                AND hi2.recipe_id != %(recipe_id)s

                UNION ALL

                SELECT DISTINCT
                    hi2.recipe_id,
                    b.depth + 1,
                    b.visited || hi2.recipe_id
                FROM bfs b
                JOIN has_ingredient hi1 ON b.recipe_id       = hi1.recipe_id
                JOIN has_ingredient hi2 ON hi1.ingredient_id = hi2.ingredient_id
                WHERE hi2.recipe_id != ALL(b.visited)
                AND b.depth < %(max_hops)s
            )
            SELECT COUNT(DISTINCT recipe_id) AS reachable
            FROM bfs
        """,
        "cypher": """
            MATCH (r:Recipe {recipe_id: $recipe_id})-[:HAS_INGREDIENT*1..{max_hops}]-(r2:Recipe)
            WHERE $scale IN r.scales AND $scale IN r2.scales
            AND r2.recipe_id <> $recipe_id
            RETURN count(DISTINCT r2) AS reachable
        """,
        "params": {"recipe_id": 105275, "max_hops": 2},
    },
]