"""Reusable control-flow patterns shared by every package family in the estate.

These helpers encode the house ETL conventions so that a package author writes
only the parts that are genuinely different (source query, transforms, target),
while batch registration, watermarking, row-count auditing and error handling
stay consistent across all ~200 packages.

Every generated package follows this control flow:

    [Init Batch Variables] -> [Log Package Start] -> ( optional [Get Watermark] )
        -> <package specific work>
        -> ( optional [Set Watermark] ) -> [Log Row Counts] -> [Log Package Success]

with an OnError event handler that writes to etl.ErrorLog and marks the package
execution failed.
"""

from __future__ import annotations

from ssisgen import Container, DataFlow, DataFlowTask, ExecuteSql, Expression, Package

CONTROL_CONNECTION = "WWI_Staging_DB"

CONN_ORACLE = "WWI_Oracle_ERP"
CONN_OLTP = "WWI_Source_DB"
CONN_STAGING = "WWI_Staging_DB"
CONN_DW = "WWI_DW_Destination_DB"
CONN_FILES = "WWI_Inbound_Files"


def standard_parameters(pkg, source_system=None):
    pkg.add_parameter("BatchId", 0, dtype="int", description="Batch identifier supplied by the master package.")
    pkg.add_parameter("ReloadFullHistory", "False", dtype="bool",
                      description="When True the package ignores the stored watermark and reloads all history.")
    if source_system:
        pkg.add_parameter("SourceSystemCode", source_system, dtype="string",
                          description="etl.SourceSystem code used for watermark and audit lookups.")
    return pkg


def standard_variables(pkg, extra=None):
    pkg.add_variable("PackageExecutionId", 0, dtype="long")
    pkg.add_variable("RowsRead", 0, dtype="int")
    pkg.add_variable("RowsInserted", 0, dtype="int")
    pkg.add_variable("RowsUpdated", 0, dtype="int")
    pkg.add_variable("RowsRejected", 0, dtype="int")
    pkg.add_variable("WatermarkFrom", "1900-01-01", dtype="string")
    pkg.add_variable("WatermarkTo", "1900-01-01", dtype="string")
    pkg.add_variable("ErrorMessage", "", dtype="string")
    for name, value, dtype in extra or []:
        pkg.add_variable(name, value, dtype=dtype)
    return pkg


def log_package_start(pkg, name="Log Package Start"):
    return ExecuteSql(
        name,
        CONTROL_CONNECTION,
        "EXEC etl.usp_LogPackageStart @BatchId = ?, @PackageName = ?, @PackageExecutionId = ? OUTPUT;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[("$Package::BatchId", 0, "LONG"), ("System::PackageName", 1, "NVARCHAR")],
        result_bindings=[("0", "User::PackageExecutionId")],
        is_stored_procedure=True,
    )


def log_package_success(name="Log Package Success"):
    return ExecuteSql(
        name,
        CONTROL_CONNECTION,
        "EXEC etl.usp_LogPackageEnd @PackageExecutionId = ?, @Status = 'Succeeded', "
        "@RowsRead = ?, @RowsInserted = ?, @RowsUpdated = ?, @RowsRejected = ?;",
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("User::RowsRead", 1, "LONG"),
            ("User::RowsInserted", 2, "LONG"),
            ("User::RowsUpdated", 3, "LONG"),
            ("User::RowsRejected", 4, "LONG"),
        ],
        is_stored_procedure=True,
    )


def get_watermark(source_system_expr="$Package::SourceSystemCode", object_name="", name="Get Watermark"):
    return ExecuteSql(
        name,
        CONTROL_CONNECTION,
        "EXEC etl.usp_GetWatermark @SourceSystemCode = ?, @ObjectName = ?, @ReloadFullHistory = ?, "
        "@WatermarkFrom = ? OUTPUT, @WatermarkTo = ? OUTPUT;",
        result_type="ResultSetType_SingleRow",
        parameter_bindings=[
            (source_system_expr, 0, "NVARCHAR"),
            ("$Package::BatchId", 1, "LONG"),
            ("$Package::ReloadFullHistory", 2, "BYTE"),
        ],
        result_bindings=[("0", "User::WatermarkFrom"), ("1", "User::WatermarkTo")],
        is_stored_procedure=True,
    )


def set_watermark(object_name, name="Set Watermark"):
    return ExecuteSql(
        name,
        CONTROL_CONNECTION,
        "EXEC etl.usp_SetWatermark @SourceSystemCode = ?, @ObjectName = N'%s', @WatermarkTo = ?, "
        "@PackageExecutionId = ?;" % object_name,
        parameter_bindings=[
            ("$Package::SourceSystemCode", 0, "NVARCHAR"),
            ("User::WatermarkTo", 1, "NVARCHAR"),
            ("User::PackageExecutionId", 2, "LONG"),
        ],
        is_stored_procedure=True,
    )


def log_row_count(object_name, name="Log Row Counts"):
    return ExecuteSql(
        name,
        CONTROL_CONNECTION,
        "EXEC etl.usp_LogRowCount @PackageExecutionId = ?, @ObjectName = N'%s', @SourceRowCount = ?, "
        "@TargetRowCount = ?, @RejectRowCount = ?;" % object_name,
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("User::RowsRead", 1, "LONG"),
            ("User::RowsInserted", 2, "LONG"),
            ("User::RowsRejected", 3, "LONG"),
        ],
        is_stored_procedure=True,
    )


def truncate(table, connection=CONN_STAGING, name=None):
    return ExecuteSql(name or "Truncate %s" % table, connection, "TRUNCATE TABLE %s;" % table)


def exec_proc(name, proc_call, connection=CONN_STAGING, parameter_bindings=None):
    return ExecuteSql(name, connection, proc_call, parameter_bindings=parameter_bindings or [],
                      is_stored_procedure=True)


def attach_error_handler(pkg):
    """OnError handler: persist the error and mark the execution failed."""
    capture = ExecuteSql(
        "Log Error",
        CONTROL_CONNECTION,
        "EXEC etl.usp_LogError @PackageExecutionId = ?, @BatchId = ?, @ErrorCode = ?, "
        "@ErrorDescription = ?, @SourceName = ?;",
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("$Package::BatchId", 1, "LONG"),
            ("System::ErrorCode", 2, "LONG"),
            ("System::ErrorDescription", 3, "NVARCHAR"),
            ("System::SourceName", 4, "NVARCHAR"),
        ],
        is_stored_procedure=True,
    )
    fail = ExecuteSql(
        "Mark Execution Failed",
        CONTROL_CONNECTION,
        "EXEC etl.usp_LogPackageEnd @PackageExecutionId = ?, @Status = 'Failed', "
        "@RowsRead = ?, @RowsInserted = ?, @RowsUpdated = ?, @RowsRejected = ?;",
        parameter_bindings=[
            ("User::PackageExecutionId", 0, "LONG"),
            ("User::RowsRead", 1, "LONG"),
            ("User::RowsInserted", 2, "LONG"),
            ("User::RowsUpdated", 3, "LONG"),
            ("User::RowsRejected", 4, "LONG"),
        ],
        is_stored_procedure=True,
    )
    pkg.add_event_handler("OnError", [capture, fail], [(capture, fail, "Completion", None, True)])
    return pkg


def new_package(name, description, source_system=None, connections=(), extra_variables=None,
                log_to_sql=True):
    """Create a package pre-wired with the estate control framework."""
    pkg = Package(name, description=description)
    standard_parameters(pkg, source_system=source_system)
    standard_variables(pkg, extra=extra_variables)
    pkg.use_connection(CONTROL_CONNECTION, *connections)
    if log_to_sql:
        pkg.add_sql_log_provider(CONTROL_CONNECTION)
    attach_error_handler(pkg)
    return pkg
