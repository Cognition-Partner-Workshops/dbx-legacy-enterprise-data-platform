from dbx_etl_common import naming, schema, views


def test_all_tables_and_views_exist(spark, catalog):
    for t in schema.TABLES:
        assert spark.catalog.tableExists(naming.controlTable(catalog, t.name)), t.name
    for v in views.VIEW_NAMES:
        assert spark.catalog.tableExists(naming.controlTable(catalog, v)), v


def test_seeds_are_idempotent(spark, catalog):
    from dbx_etl_common import seeds
    before = {s.table: spark.table(naming.controlTable(catalog, s.table)).count() for s in seeds.SEEDS}
    seeds.applySeeds(spark, catalog)
    after = {s.table: spark.table(naming.controlTable(catalog, s.table)).count() for s in seeds.SEEDS}
    assert before == after
    assert before["source_system"] == 11
    assert before["configuration"] == 25
    assert before["reconciliation_exemption"] == 10
    assert before["data_quality_rule"] == 29
