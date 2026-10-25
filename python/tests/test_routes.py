from usagerail.routes import RouteTable


def setting(method: str, route: str, weight: int = 1) -> dict:
    return {"method": method, "route": route, "weight": weight, "reserve": weight}


def first(table: RouteTable, method: str, path: str):
    """The tests only care about the first match (the exact one comes first)"""
    found = table.candidates_by_path(method, path)
    return found[0] if found else None


def weights(table: RouteTable, method: str, path: str) -> list[int]:
    return [item["weight"] for item in table.candidates_by_path(method, path)]


def test_raw_paths_match_seller_templates_at_the_start_and_templates_are_looked_up_exactly_at_the_end():
    table = RouteTable(
        [
            setting("POST", "/v1/images", 10),
            setting("GET", "/v1/users/:id", 2),
            setting("GET", "/v1/files/:name?", 3),
            setting("ALL", "/v1/batch/*", 4),
            setting("GET", "/v1/orders/:id{[0-9]+}", 5),
        ]
    )
    assert first(table, "POST", "/v1/images")["weight"] == 10
    assert first(table, "GET", "/v1/images") is None
    assert first(table, "GET", "/v1/users/42")["weight"] == 2
    assert first(table, "GET", "/v1/users/42/")["weight"] == 2
    assert first(table, "GET", "/v1/users/42/posts") is None
    assert first(table, "GET", "/v1/files")["weight"] == 3
    assert first(table, "GET", "/v1/files/a.txt")["weight"] == 3
    # ALL matches every method
    assert first(table, "DELETE", "/v1/batch/7/items")["weight"] == 4
    assert first(table, "GET", "/v1/orders/77")["weight"] == 5
    # At the end of a request the framework gives the template, looked up exactly
    assert table.by_template("GET", "/v1/users/:id")["weight"] == 2
    assert table.by_template("PUT", "/v1/batch/*")["weight"] == 4
    # Regex special characters in the path match literally
    assert first(RouteTable([setting("GET", "/v1/a.b")]), "GET", "/v1/aXb") is None


def test_starlette_flask_and_django_parameters_are_understood():
    table = RouteTable(
        [
            setting("GET", "/v1/users/{user_id}", 2),
            setting("GET", "/v1/files/{rest:path}", 3),
            setting("GET", "/v2/users/<int:pk>/", 4),
            setting("GET", "/v2/pages/<path:rest>", 5),
            setting("GET", "/v3/reports/{name}.{ext}", 6),
        ]
    )
    assert first(table, "GET", "/v1/users/42")["weight"] == 2
    assert first(table, "GET", "/v1/files/a/b/c.txt")["weight"] == 3
    assert first(table, "GET", "/v2/users/7")["weight"] == 4
    assert first(table, "GET", "/v2/pages/docs/intro")["weight"] == 5
    assert first(table, "GET", "/v3/reports/q3.pdf")["weight"] == 6
    assert first(table, "GET", "/v1/users/42/posts") is None


def test_paths_match_case_sensitively_like_the_python_frameworks_with_one_optional_trailing_slash():
    # Starlette, Flask and Django all route case-sensitively: two routes differing only in case are separate, and the cheaper one doesn't reserve like the expensive one
    table = RouteTable([setting("POST", "/v1/images", 10), setting("POST", "/v1/IMAGES", 1), setting("GET", "/v1/users/:id/", 2), setting("GET", "/", 3)])
    assert weights(table, "POST", "/v1/IMAGES") == [1]
    assert first(table, "POST", "/v1/images/")["weight"] == 10
    assert first(table, "POST", "/v1/images//") is None
    assert first(table, "GET", "/v1/users/42")["weight"] == 2
    assert first(table, "GET", "/v1/users/42/")["weight"] == 2
    assert first(table, "GET", "/V1/Users/42") is None
    assert first(table, "GET", "/")["weight"] == 3
    assert first(table, "GET", "/v1") is None
    # Templates the framework gives at the end are still looked up exactly
    assert table.by_template("POST", "/V1/IMAGES") is None


def test_every_seller_template_that_matches_a_path_is_a_candidate_exact_first_then_in_the_sellers_order():
    table = RouteTable([setting("GET", "/v1/users/:id", 10), setting("GET", "/v1/users/me", 1), setting("ALL", "/v1/*", 3)])
    assert weights(table, "GET", "/v1/users/me") == [1, 10, 3]
    assert weights(table, "GET", "/v1/users/42") == [10, 3]


def test_an_empty_segment_inside_a_template_is_kept_only_a_trailing_slash_is_optional():
    # /v1//users/{id} only matches /v1//users/7, which the framework routes to it; a trailing slash is still optional
    table = RouteTable([setting("POST", "/v1//users/{id}", 10), setting("GET", "/v1/items/", 2)])
    assert weights(table, "POST", "/v1//users/7") == [10]
    assert weights(table, "POST", "/v1/users/7") == []
    assert weights(table, "GET", "/v1/items") == [2]
    assert weights(table, "GET", "/v1/items/") == [2]
