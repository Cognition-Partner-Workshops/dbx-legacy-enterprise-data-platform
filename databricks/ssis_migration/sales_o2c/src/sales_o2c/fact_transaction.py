"""08_facts: FACT_Load_CustomerTransaction and FACT_Load_Transaction.

FACT_Load_CustomerTransaction = stg.usp_ConformCustomerTransactionForFact (raw -> stg_customer_transaction) +
the package data flow (aging buckets, debit/credit sign, lookups) -> gold_fact_customer_transaction.
FACT_Load_Transaction = stg.usp_ConformTransactionForFact (AR| customer + AP| supplier sub-ledgers ->
stg_transaction) + the package data flow -> gold_fact_transaction in the legacy Fact.Transaction shape.

Deliberate deviations (see README): the legacy conform procedure synthesises AR rows from stg.Sale because the
extract's destination was mis-declared as raw.SqlInvoice; we use the real EXT_SQL_CustomerTransactions landing.
Supplier transactions are another group's extract, so the AP side reads the legacy OLTP table via federation.
The balance screen (|excl + tax - total| > 0.01) is *recorded* in err_rejected_constraint_violation but the rows
are still loaded, because the populated legacy Fact.Transaction contains every source transaction.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import OLTP_CATALOG, SOURCE_SYSTEM_CODE, RunContext
from sales_o2c.dimensions import asOfLookup, dimension
from sales_o2c.tables import appendRows, mergeUpsert, withAudit
from sales_o2c.watermark import TIMESTAMP, getWatermark, setWatermark

TRANSACTION_TYPE_CODES = {
    "Customer Invoice": "INV", "Customer Credit Note": "CRN", "Customer Payment Received": "PAY", "Customer Refund": "REF",
    "Supplier Invoice": "INV", "Supplier Credit Note": "CRN", "Supplier Payment Issued": "PAY", "Supplier Refund": "REF",
}
DEBIT_CODES = ("INV", "DBN")

LEGACY_TRANSACTION_BUSINESS_COLUMNS = [
    "date_key", "customer_key", "bill_to_customer_key", "supplier_key", "transaction_type_key", "payment_method_key",
    "wwi_customer_transaction_id", "wwi_supplier_transaction_id", "wwi_invoice_id", "wwi_purchase_order_id",
    "supplier_invoice_number", "total_excluding_tax", "tax_amount", "total_including_tax", "outstanding_balance", "is_finalized",
]


def transactionTypeCode(nameCol):
    expr = F.lit("OTH")
    for name, code in TRANSACTION_TYPE_CODES.items():
        expr = F.when(nameCol == name, code).otherwise(expr)
    return expr


def dueDate(regionCol, dateCol):
    """stg.usp_ConformCustomerTransactionForFact: EU = month end + 30, APAC = +60, else +30."""
    return (
        F.when(regionCol == "EU", F.date_add(F.last_day(dateCol), 30))
        .when(regionCol == "APAC", F.date_add(dateCol, 60))
        .otherwise(F.date_add(dateCol, 30))
    )


def agingBucket(daysCol, regionCol):
    """FACT_Load_CustomerTransaction Derived Column AgingBucketCode (region-specific buckets)."""
    eu = F.when(daysCol <= 30, "1-30").when(daysCol <= 60, "31-60").otherwise("60+")
    apac = F.when(daysCol <= 60, "1-60").when(daysCol <= 120, "61-120").otherwise("120+")
    na = F.when(daysCol <= 30, "1-30").when(daysCol <= 60, "31-60").when(daysCol <= 90, "61-90").otherwise("90+")
    return F.when(daysCol <= 0, "CURRENT").when(regionCol == "EU", eu).when(regionCol == "APAC", apac).otherwise(na)


def conformCustomerTransactions(raw: DataFrame, customerRegion: DataFrame) -> DataFrame:
    """raw_sql_customer_transaction (+ customer region) -> stg_customer_transaction."""
    df = raw.join(customerRegion, "customer_id", "left")
    region = F.upper(F.coalesce(F.col("region_code"), F.lit("NA")))
    return df.select(
        F.col("customer_transaction_id"),
        F.concat(F.lit(SOURCE_SYSTEM_CODE), F.lit("|"), F.col("customer_transaction_id")).alias("customer_transaction_business_key"),
        F.col("customer_id").alias("customer_business_key"),
        F.col("customer_id"),
        transactionTypeCode(F.col("transaction_type_name")).alias("transaction_type_code"),
        F.col("transaction_type_id"), F.col("payment_method_id"),
        F.col("invoice_id").cast("string").alias("invoice_number"),
        F.col("invoice_id"),
        F.col("transaction_date"),
        dueDate(region, F.col("transaction_date")).alias("due_date"),
        F.col("amount_excluding_tax"), F.col("tax_amount"),
        F.col("transaction_amount"), F.col("outstanding_balance"), F.col("is_finalized"),
        F.lit("USD").alias("transaction_currency"),
        region.alias("region_code"),
        F.when(region == "EU", "VAT").when(region == "APAC", "GST").otherwise("SUT").alias("tax_regime_code"),
        F.col("last_edited_when").alias("last_modified_at"),
    ).withColumn("row_hash", F.md5(F.concat_ws("|", F.col("customer_transaction_id"), F.col("transaction_amount"), F.col("outstanding_balance"), F.col("is_finalized"))))


def computeCustomerTransactionMeasures(df: DataFrame, asOf=None) -> DataFrame:
    today = F.lit(asOf).cast("date") if asOf is not None else F.current_date()
    days = F.datediff(today, F.col("due_date"))
    isDebit = F.col("transaction_type_code").isin(*DEBIT_CODES)
    return (
        df.withColumn("days_past_due", days)
        .withColumn("aging_bucket_code", agingBucket(days, F.col("region_code")))
        .withColumn("is_debit_transaction", isDebit)
        .withColumn("signed_amount", F.when(isDebit, F.col("transaction_amount")).otherwise(-F.col("transaction_amount")).cast("decimal(18,2)"))
        .withColumn("is_past_due", (days > 0) & (F.coalesce(F.col("outstanding_balance"), F.lit(0)) != 0))
    )


def _customerRegion(spark: SparkSession) -> DataFrame:
    return spark.table(f"{OLTP_CATALOG}.Sales.Customers").select(F.col("CustomerID").alias("customer_id"), F.col("RegionCode").alias("region_code"))


def runFactLoadCustomerTransaction(spark: SparkSession, ctx: RunContext) -> dict:
    wmFrom = getWatermark(spark, ctx, "Fact.CustomerTransaction", TIMESTAMP)
    raw = spark.table(ctx.table("raw_sql_customer_transaction")).filter(F.col("delete_flag") == "N")
    if not ctx.reloadFullHistory:
        raw = raw.filter(F.col("last_edited_when") > F.lit(wmFrom).cast("timestamp"))
    staged = withAudit(conformCustomerTransactions(raw, _customerRegion(spark)), ctx)
    mergeUpsert(spark, ctx.table("stg_customer_transaction"), staged, ["customer_transaction_business_key"])

    df = staged.withColumn("last_modified_when", F.col("last_modified_at"))
    df = asOfLookup(df, dimension(spark, "Customer"), "customer_id", "last_modified_when", "customer_key")
    df = asOfLookup(df, dimension(spark, "TransactionType"), "transaction_type_id", "last_modified_when", "transaction_type_key")
    df = asOfLookup(df, dimension(spark, "PaymentMethod"), "payment_method_id", "last_modified_when", "payment_method_key")
    measured = computeCustomerTransactionMeasures(df)
    unmatched = measured.filter(F.col("customer_key") == 0)
    facts = measured.select(
        F.col("customer_transaction_id").alias("wwi_customer_transaction_id"),
        F.col("transaction_date").alias("transaction_date_key"), F.col("due_date").alias("due_date_key"),
        F.col("customer_key"), F.col("customer_key").alias("bill_to_customer_key"), F.col("transaction_type_key"), F.col("payment_method_key"),
        F.col("region_code"), F.col("invoice_number"), F.lit(SOURCE_SYSTEM_CODE).alias("source_system_code"),
        F.col("transaction_currency").alias("transaction_currency_code"), F.col("transaction_type_code"),
        F.col("amount_excluding_tax"), F.col("tax_amount"), F.col("transaction_amount"), F.col("outstanding_balance"),
        F.lit(1).cast("decimal(18,6)").alias("fx_rate_to_reporting"), F.col("transaction_amount").alias("transaction_amount_reporting"),
        F.col("tax_regime_code"), F.col("is_finalized"), F.col("aging_bucket_code"), F.col("days_past_due"),
        F.col("is_debit_transaction"), F.col("signed_amount"), F.col("is_past_due"),
        F.md5(F.concat_ws("|", F.col("customer_transaction_business_key"))).alias("natural_key_hash"),
        F.lit(int(ctx.packageExecutionId)).cast("bigint").alias("lineage_key"), F.col("last_modified_when"),
    )
    rowsInserted = mergeUpsert(spark, ctx.table("gold_fact_customer_transaction"), withAudit(facts, ctx), ["wwi_customer_transaction_id"])
    rowsRejected = appendRows(
        spark, ctx.table("err_rejected_lookup_failure"),
        withAudit(unmatched.select(
            F.lit("stg.CustomerTransaction").alias("source_object_name"), F.col("customer_transaction_business_key").alias("source_business_key"),
            F.lit("Customer").alias("lookup_name"), F.lit("WWI Customer ID").alias("lookup_column_name"), F.col("customer_id").cast("string").alias("lookup_value"),
            F.lit("LOOKUP_NO_MATCH").alias("reject_reason_code"), F.lit("Fact").alias("reject_stage"), F.lit(True).alias("routed_to_unknown_member"),
            F.lit(True).alias("queued_for_late_arrival"), F.current_timestamp().alias("rejected_at_utc"),
        ), ctx),
    )
    maxLm = measured.agg(F.max("last_modified_when")).first()[0]
    if maxLm is not None:
        setWatermark(spark, ctx, "Fact.CustomerTransaction", TIMESTAMP, maxLm.isoformat())
    return {"rowsRead": rowsInserted, "rowsInserted": rowsInserted, "rowsRejected": rowsRejected}


# ------------------------------------------------------------------ FACT_Load_Transaction
def conformTransactions(customer: DataFrame, supplier: DataFrame) -> DataFrame:
    """stg.usp_ConformTransactionForFact: AR| + AP| sub-ledgers into one stg_transaction shape."""
    ar = customer.select(
        F.concat(F.lit("AR|"), F.col("customer_transaction_business_key")).alias("transaction_business_key"),
        F.col("transaction_type_code"), F.col("transaction_type_id"), F.col("payment_method_id"),
        F.col("customer_business_key").cast("string").alias("party_business_key"), F.lit("CUSTOMER").alias("party_type_code"),
        F.col("customer_transaction_id").alias("wwi_customer_transaction_id"), F.lit(None).cast("int").alias("wwi_supplier_transaction_id"),
        F.col("invoice_id").alias("wwi_invoice_id"), F.lit(None).cast("int").alias("wwi_purchase_order_id"), F.lit(None).cast("string").alias("supplier_invoice_number"),
        F.col("invoice_customer_id").alias("wwi_customer_id"), F.col("customer_id").alias("wwi_bill_to_customer_id"), F.lit(None).cast("int").alias("wwi_supplier_id"),
        F.col("transaction_date"), F.col("amount_excluding_tax"), F.col("tax_amount"), F.col("transaction_amount"), F.col("outstanding_balance"), F.col("is_finalized"),
        F.col("transaction_currency"), F.col("invoice_number").alias("source_document_number"),
        F.date_format(F.col("transaction_date"), "yyyyMM").alias("accounting_period_code"), F.col("region_code"), F.col("last_modified_at"),
    )
    ap = supplier.select(
        F.concat(F.lit("AP|"), F.lit(SOURCE_SYSTEM_CODE), F.lit("|"), F.col("supplier_transaction_id")).alias("transaction_business_key"),
        transactionTypeCode(F.col("transaction_type_name")).alias("transaction_type_code"), F.col("transaction_type_id"), F.col("payment_method_id"),
        F.col("supplier_id").cast("string").alias("party_business_key"), F.lit("SUPPLIER").alias("party_type_code"),
        F.lit(None).cast("int").alias("wwi_customer_transaction_id"), F.col("supplier_transaction_id").alias("wwi_supplier_transaction_id"),
        F.lit(None).cast("int").alias("wwi_invoice_id"), F.col("purchase_order_id").alias("wwi_purchase_order_id"), F.col("supplier_invoice_number"),
        F.lit(None).cast("int").alias("wwi_customer_id"), F.lit(None).cast("int").alias("wwi_bill_to_customer_id"), F.col("supplier_id").alias("wwi_supplier_id"),
        F.col("transaction_date"), F.col("amount_excluding_tax"), F.col("tax_amount"), F.col("transaction_amount"), F.col("outstanding_balance"), F.col("is_finalized"),
        F.lit("USD").alias("transaction_currency"), F.col("supplier_invoice_number").alias("source_document_number"),
        F.date_format(F.col("transaction_date"), "yyyyMM").alias("accounting_period_code"), F.lit("NA").alias("region_code"), F.col("last_modified_at"),
    )
    out = ar.unionByName(ap)
    return out.withColumn(
        "dq_status_code",
        F.when(F.col("party_business_key").isNull() | F.col("transaction_business_key").isNull() | F.col("accounting_period_code").isNull(), "FAIL").otherwise("PASS"),
    ).withColumn("row_hash", F.md5(F.concat_ws("|", F.col("transaction_business_key"), F.col("transaction_amount"), F.col("outstanding_balance"), F.col("is_finalized"))))


def computeTransactionMeasures(df: DataFrame) -> DataFrame:
    variance = (F.coalesce(F.col("amount_excluding_tax"), F.lit(0)) + F.coalesce(F.col("tax_amount"), F.lit(0)) - F.coalesce(F.col("transaction_amount"), F.lit(0))).cast("decimal(18,2)")
    return (
        df.withColumn("balance_check_variance", variance)
        .withColumn("is_balanced", F.abs(variance) <= 0.01)
        .withColumn("ledger_side_code", F.when(F.col("party_type_code") == "SUPPLIER", "AP").otherwise("AR"))
        .withColumn("is_supplier_side", F.col("party_type_code") == "SUPPLIER")
    )


def _supplierTransactions(spark: SparkSession) -> DataFrame:
    return spark.sql(
        f"""
        SELECT st.SupplierTransactionID AS supplier_transaction_id, st.SupplierID AS supplier_id, st.TransactionTypeID AS transaction_type_id,
               tt.TransactionTypeName AS transaction_type_name, st.PurchaseOrderID AS purchase_order_id, st.PaymentMethodID AS payment_method_id,
               st.SupplierInvoiceNumber AS supplier_invoice_number, CAST(st.TransactionDate AS date) AS transaction_date,
               st.AmountExcludingTax AS amount_excluding_tax, st.TaxAmount AS tax_amount, st.TransactionAmount AS transaction_amount,
               st.OutstandingBalance AS outstanding_balance, st.IsFinalized AS is_finalized, st.LastEditedWhen AS last_modified_at
        FROM {OLTP_CATALOG}.Purchasing.SupplierTransactions st
        JOIN {OLTP_CATALOG}.Application.TransactionTypes tt ON tt.TransactionTypeID = st.TransactionTypeID
        """
    )


def runFactLoadTransaction(spark: SparkSession, ctx: RunContext) -> dict:
    wmFrom = getWatermark(spark, ctx, "Fact.Transaction", TIMESTAMP)
    customer = spark.table(ctx.table("stg_customer_transaction"))
    supplier = _supplierTransactions(spark)
    if not ctx.reloadFullHistory:
        customer = customer.filter(F.col("last_modified_at") > F.lit(wmFrom).cast("timestamp"))
        supplier = supplier.filter(F.col("last_modified_at") > F.lit(wmFrom).cast("timestamp"))
    # Legacy GetTransactionUpdates: WWI Customer ID = COALESCE(invoice.CustomerID, ct.CustomerID); bill-to = ct.CustomerID
    invoices = spark.table(f"{OLTP_CATALOG}.Sales.Invoices").select(F.col("InvoiceID").alias("invoice_id"), F.col("CustomerID").alias("inv_customer_id"))
    customer = customer.join(invoices, "invoice_id", "left").withColumn("invoice_customer_id", F.coalesce(F.col("inv_customer_id"), F.col("customer_id"))).drop("inv_customer_id")
    staged = withAudit(conformTransactions(customer, supplier), ctx)
    mergeUpsert(spark, ctx.table("stg_transaction"), staged, ["transaction_business_key"])

    df = staged.filter(F.col("dq_status_code") == "PASS").withColumn("last_modified_when", F.col("last_modified_at"))
    df = asOfLookup(df, dimension(spark, "Customer"), "wwi_customer_id", "last_modified_when", "customer_key")
    df = asOfLookup(df, dimension(spark, "Customer"), "wwi_bill_to_customer_id", "last_modified_when", "bill_to_customer_key")
    df = asOfLookup(df, dimension(spark, "Supplier"), "wwi_supplier_id", "last_modified_when", "supplier_key")
    df = asOfLookup(df, dimension(spark, "TransactionType"), "transaction_type_id", "last_modified_when", "transaction_type_key")
    df = asOfLookup(df, dimension(spark, "PaymentMethod"), "payment_method_id", "last_modified_when", "payment_method_key")
    measured = computeTransactionMeasures(df)
    facts = measured.select(
        F.col("transaction_business_key"),
        F.col("transaction_date").alias("date_key"), F.col("customer_key"), F.col("bill_to_customer_key"), F.col("supplier_key"),
        F.col("transaction_type_key"), F.col("payment_method_key"),
        F.col("wwi_customer_transaction_id"), F.col("wwi_supplier_transaction_id"), F.col("wwi_invoice_id"), F.col("wwi_purchase_order_id"), F.col("supplier_invoice_number"),
        F.col("amount_excluding_tax").alias("total_excluding_tax"), F.col("tax_amount"), F.col("transaction_amount").alias("total_including_tax"),
        F.col("outstanding_balance"), F.col("is_finalized"),
        F.lit(int(ctx.packageExecutionId)).cast("bigint").alias("lineage_key"),
        F.col("ledger_side_code"), F.col("region_code"), F.col("transaction_currency").alias("transaction_currency_code"),
        F.lit(1).cast("decimal(18,6)").alias("fx_rate_to_reporting"), F.col("outstanding_balance").alias("outstanding_balance_reporting"),
        F.col("accounting_period_code"), F.col("party_type_code"), F.col("transaction_type_code"), F.col("balance_check_variance"), F.col("is_balanced"),
        F.md5(F.col("transaction_business_key")).alias("natural_key_hash"), F.col("last_modified_when"),
    )
    rowsInserted = mergeUpsert(spark, ctx.table("gold_fact_transaction"), withAudit(facts, ctx), ["transaction_business_key"])
    unbalanced = appendRows(
        spark, ctx.table("err_rejected_constraint_violation"),
        withAudit(measured.filter(~F.col("is_balanced")).select(
            F.lit("Fact.Transaction").alias("target_object_name"), F.lit("CK_Transaction_Balanced").alias("constraint_name"),
            F.col("transaction_business_key").alias("violating_business_key"), F.lit("UNBALANCED_LOADED").alias("reject_reason_code"),
            F.lit("Fact").alias("reject_stage"), F.col("balance_check_variance").alias("variance_amount"), F.current_timestamp().alias("rejected_at_utc"),
        ), ctx),
    )
    maxLm = measured.agg(F.max("last_modified_when")).first()[0]
    if maxLm is not None:
        setWatermark(spark, ctx, "Fact.Transaction", TIMESTAMP, maxLm.isoformat())
    return {"rowsRead": rowsInserted, "rowsInserted": rowsInserted, "rowsRejected": unbalanced}
