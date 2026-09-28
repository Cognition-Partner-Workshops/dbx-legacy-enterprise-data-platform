"""Shared helpers for the WWI_ReferenceData migration (ssis/06_reference_data -> Databricks).

Modules:
    grids       steward-maintained reference grids ported from sqlserver/reference/ref.usp_Load*.sql
    schemas     Delta DDL for silver.ref_*, silver.err_* and the gold dimensions this project owns
    runtime     job-parameter handling, package lifecycle (dbx_etl_common control calls), reject routing
    delta_io    idempotent MERGE helpers (SCD1 / SCD2 / reference upserts) and surrogate-key assignment
    ref_loads   Spark ports of the ref.usp_Load* / ref.usp_ReportUnmappedCodes procedures
    transforms  Data Flow transformations of every REF_Load_* package as pure DataFrame functions
    calendar    Dimension.Date / Dimension.Fiscal Calendar generation (sequence()/explode())
"""

PROJECT_NAME = "WWI_ReferenceData"

PHASES = {
    "REF_Load_Geography": 10,
    "REF_Load_WarehouseSite": 10,
    "REF_Load_Currency": 20,
    "REF_Load_PaymentTerms": 20,
    "REF_Load_PaymentMethod": 20,
    "REF_Load_SalesChannel": 30,
    "REF_Load_TransactionType": 30,
    "REF_Load_ReturnReason": 30,
    "REF_Load_LoyaltyTier": 30,
    "REF_Load_Carrier": 30,
    "REF_Load_CostCenter": 30,
    "REF_Load_DateDimension": 40,
    "REF_Load_CodeTranslation": 50,
    "REF_Load_UnknownMembers": 50,
}
