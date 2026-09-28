import pytest

from wwi_sqlserver_extract import jdbc


def test_render_sql_replaces_placeholders_in_order():
    sql = "SELECT 1 FROM t WHERE a > ? AND a <= ? AND c = ?"
    rendered = jdbc.renderSql(sql, ("watermarkFrom", "watermarkTo", "code"), {"watermarkFrom": 10, "watermarkTo": 20, "code": "O'Brien"})
    assert rendered == "SELECT 1 FROM t WHERE a > 10 AND a <= 20 AND c = N'O''Brien'"


def test_render_sql_rejects_mismatched_parameters():
    with pytest.raises(ValueError):
        jdbc.renderSql("SELECT ?", ("a", "b"), {"a": 1, "b": 2})
    with pytest.raises(KeyError):
        jdbc.renderSql("SELECT ?", ("a",), {})


def test_connection_from_widgets_uses_secret_scope(dbutils):
    dbutils.widgets.values.update({"SqlServerHost": "wwi-sql.example.net", "SqlServerPort": "1433", "SqlServerOltpDb": "WideWorldImporters",
                                   "SqlServerTrustServerCertificate": "true"})
    connection = jdbc.SqlServerConnection.fromWidgets(dbutils)
    options = connection.readerOptions("SELECT 1", queryTimeoutSeconds=90)
    assert options["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert options["url"].startswith("jdbc:sqlserver://wwi-sql.example.net:1433;databaseName=WideWorldImporters;encrypt=true;trustServerCertificate=true")
    assert options["user"] == "secret:wwi/sqlserver-oltp-user"
    assert options["password"] == "secret:wwi/sqlserver-oltp-password"
    assert options["queryTimeout"] == "90" and options["fetchsize"] == "100000"
