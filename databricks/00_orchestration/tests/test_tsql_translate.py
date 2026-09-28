"""T-SQL subset used by the plan's control nodes -> Spark SQL / dbx_etl_common calls."""
import pytest

from wwi_orchestration import tsql_translate as tt


def test_common_rewrites():
    sql = tt.translateSql("SELECT TOP (1) ISNULL(COUNT_BIG(*), 0) FROM etl.Batch WHERE IsActive = 1 AND StartedAtUtc > DATEADD(HOUR, -6, SYSUTCDATETIME()) AND BatchName = N'x' AND BusinessDate = ?;")
    assert sql == ("SELECT coalesce(count(*), 0) FROM etl.Batch WHERE IsActive = true AND StartedAtUtc > "
                   "(current_timestamp() + INTERVAL -6 HOUR) AND BatchName = 'x' AND BusinessDate = :p0 LIMIT 1")


def test_bind_parameters_positional():
    sql, params = tt.bindParameters("SELECT :p0, :p1", ["a", 2])
    assert params == {"p0": "a", "p1": 2}
    with pytest.raises(ValueError):
        tt.bindParameters("SELECT :p0", [])


def test_log_error_call_is_structured():
    out = tt.translateStatement("EXEC etl.usp_LogError @BatchId = ?, @ErrorSeverity = N'Warning', @SourceName = N'Master_Daily_ETL', @ErrorDescription = N'It''s late';")
    assert out["call"] == "logError"
    assert out["args"]["errorSeverity"] == "Warning"
    assert out["args"]["errorDescription"] == "It's late"


def test_other_procedures_are_rejected():
    with pytest.raises(ValueError):
        tt.translateStatement("EXEC etl.usp_Something ?")


def test_every_plan_statement_translates(plan):
    seen = 0
    for root in plan["roots"]:
        for n in root["nodes"]:
            sql = n.get("sql")
            if sql:
                out = tt.translateStatement(sql)
                assert ("sql" in out) or (out.get("call") == "logError")
                seen += 1
    assert seen > 0
