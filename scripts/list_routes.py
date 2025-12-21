import app

ROUTE_FILTERS = ("admin", "security", "activity")

for rule in app.app.url_map.iter_rules():
    if any(k in rule.rule for k in ROUTE_FILTERS):
        print(rule.rule, "->", rule.endpoint)
