def table(catalog: str, schema: str, table: str) -> str:
    return "%s.%s.%s" % (catalog, schema, table)
