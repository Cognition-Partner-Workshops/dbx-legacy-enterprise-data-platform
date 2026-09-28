"""Bronze column conformance.

The STG_Load_* packages were generated against Oracle/OLTP extract column names
(CC_CODE, VENDOR_CODE, PAY_AMT, ...) while the repository's raw.* DDL, which the
bronze tables mirror, scripts the extracts with different names (COST_CENTER_CD,
SUPP_ID, PAYMENT_AMT, ...). The notebooks and transforms keep the package
vocabulary so they can be read next to the .dtsx; `conformSource` adds the
package-named columns on top of the bronze frame from this one mapping table.
A None target means the estate has no equivalent column and the package input
is NULL (listed in docs/migration/04_staging-package-mapping.md).
"""

from pyspark.sql import functions as F


def _recoverable():
    return F.when(F.col("RECOVERABLE_PCT") > 0, F.lit("Y")).otherwise(F.lit("N"))


def _hazmat():
    return F.when(F.trim(F.coalesce(F.col("HAZMAT_CLASS_CD"), F.lit(""))) != "", F.lit("Y")).otherwise(F.lit("N"))


def _discontinued():
    return F.when(F.col("DISCONTINUED_DT").isNotNull(), F.lit("Y")).otherwise(F.lit("N"))


def _consent():
    return F.when(F.trim(F.coalesce(F.col("ConsentCategories"), F.lit(""))) != "", F.lit("Y")).otherwise(F.lit("N"))


def _duration():
    return (F.unix_timestamp(F.col("SessionEndedWhen")) - F.unix_timestamp(F.col("SessionStartedWhen"))).cast("int")


RAW_COLUMN_MAP = {
    "raw_oracle_ap_invoice_hdr": {"INVOICE_NBR": "INVOICE_NUM", "VENDOR_CODE": "SUPP_ID", "INV_CCY": "CURRENCY_CD", "GROSS_AMT": "INVOICE_AMT", "TERMS_CD": "PAYMENT_TERMS_CD", "HOLD_FLAG": "HOLD_FLG"},
    "raw_oracle_ap_invoice_line": {"INVOICE_NBR": "INVOICE_NUM", "INV_LINE_NBR": "LINE_NUM", "TAX_CODE": "TAX_CD", "PO_NBR": None},
    "raw_oracle_ap_payment": {
        "PAYMENT_NBR": "PAYMENT_NUM", "VENDOR_CODE": "SUPP_ID", "PAY_METHOD_CD": "PAYMENT_METHOD_CD", "BANK_ACCT_CD": "BANK_ACCOUNT_REF", "PAY_CCY": "CURRENCY_CD",
        "PAY_AMT": "PAYMENT_AMT", "PAY_DT": "PAYMENT_DT", "VALUE_DT": "CLEARED_DT", "PAY_STATUS_CD": "PAYMENT_STATUS_CD",
    },
    "raw_oracle_cost_center": {"CC_CODE": "COST_CENTER_CD", "CC_NAME": "COST_CENTER_NAME", "PARENT_CC_CODE": "PARENT_COST_CENTER_CD", "FUNCTION_CD": "FUNCTIONAL_AREA_CD", "VALID_FROM_DT": "EFFECTIVE_FROM_DT"},
    "raw_oracle_currency": {"CCY_CODE": "CURRENCY_CD", "CCY_NAME": "CURRENCY_NAME", "MINOR_UNITS": "MINOR_UNIT_DIGITS", "REGION_CD": None},
    "raw_oracle_customer_address": {
        "CUST_CODE": "CUST_ID", "ADDR_TYPE_CD": "ADDRESS_USAGE_CD", "ADDR_LINE_1": "ADDRESS_LINE_1", "ADDR_LINE_2": "ADDRESS_LINE_2", "STATE_PROV_CD": "STATE_PROVINCE_CD",
        "REGION_CD": None, "EFF_FROM_DT": "VALID_FROM_DT",
    },
    "raw_oracle_customer_master": {
        "CUST_CODE": "CUST_ID", "TRADING_NAME": "CUST_LEGAL_NAME", "CUST_CLASS_CD": "CUST_TYPE_CD", "CREDIT_STATUS_CD": "CREDIT_RATING_CD", "COUNTRY_CD": None,
        "TAX_REG_NBR": "TAX_REGISTRATION_NUM", "CONSENT_FLAG": "MARKETING_CONSENT_FLG", "CREDIT_CCY": "CURRENCY_CD", "LAST_UPD_DT": "LAST_UPDATE_DT",
    },
    "raw_oracle_fx_rate": {"FROM_CCY": "FROM_CURRENCY_CD", "TO_CCY": "TO_CURRENCY_CD", "EFF_FROM_DT": "RATE_DT", "EFF_TO_DT": None, "RATE": "CONVERSION_RATE", "SRC_SYSTEM_CD": "RATE_SOURCE_CD"},
    "raw_file_fx_override": {"FROM_CCY": "FromCurrencyCode", "TO_CCY": "ToCurrencyCode", "EFF_FROM_DT": "RateDate", "EFF_TO_DT": None, "RATE": "ConversionRate", "RATE_TYPE_CD": "RateTypeCode", "SRC_SYSTEM_CD": "RateSource"},
    "raw_oracle_geography": {"GEO_CODE": "GEOGRAPHY_ID", "STATE_PROV_CD": "STATE_PROVINCE_CD", "STATE_PROV_NAME": "STATE_PROVINCE_NAME", "SALES_TERR_CD": None, "POPULATION": "POPULATION_NUM"},
    "raw_oracle_gl_journal_line": {
        "JOURNAL_ID": "JOURNAL_HDR_ID", "JOURNAL_LINE_NBR": "LINE_NUM", "JRNL_CCY": "CURRENCY_CD", "DEBIT_AMT": "ENTERED_DR_AMT", "CREDIT_AMT": "ENTERED_CR_AMT",
        "ACCOUNTING_DT": "EFFECTIVE_DT", "SOURCE_CD": "JOURNAL_SOURCE_CD", "REGION_CD": None,
    },
    "raw_oracle_payment_terms": {"TERMS_CODE": "PAYMENT_TERMS_CD", "TERMS_DESC": "PAYMENT_TERMS_NAME", "DISC_PCT": "DISCOUNT_PCT", "DISC_DAYS": "DISCOUNT_DAYS"},
    "raw_oracle_product_master": {
        "PROD_CODE": "PRODUCT_CD", "PROD_DESC": "PRODUCT_DESC", "PROD_FAMILY_CD": "CATEGORY_CD", "PACK_QTY": "UOM_CONVERSION_FACTOR", "LIST_PRICE_CCY": "STANDARD_COST_CURR_CD",
        "NET_WEIGHT": None, "WEIGHT_UOM_CD": None, "HAZMAT_FLG": _hazmat, "DISCONTINUED_FLG": _discontinued, "LAST_UPD_DT": "LAST_UPDATE_DT",
    },
    "raw_oracle_purchase_order_hdr": {"PO_NBR": "PO_NUMBER", "VENDOR_CODE": "SUPP_ID", "BUY_ORG_CD": None, "PO_CCY": "CURRENCY_CD", "PO_DT": "ORDER_DT", "LAST_UPD_DT": "LAST_UPDATE_DT"},
    "raw_oracle_purchase_order_line": {"PO_NBR": "PO_NUMBER", "PO_LINE_NBR": "LINE_NUM", "ITEM_CODE": "PRODUCT_ID", "UOM_CD": "ORDER_UOM_CD", "TAX_CODE": "TAX_CD"},
    "raw_oracle_supplier_master": {"SUPP_CODE": "SUPP_ID", "TAX_ID": "TAX_ID_NUM", "PAY_TERMS_CD": "PAYMENT_TERMS_CD", "COUNTRY_CD": None, "DEFAULT_CCY": "CURRENCY_CD", "LAST_UPD_DT": "LAST_UPDATE_DT"},
    "raw_oracle_tax_rate": {
        "TAX_CODE": "TAX_CD", "TAX_TYPE_CD": "TAX_REGIME_CD", "REGION_CD": None, "JURISDICTION_CD": "TAX_JURISDICTION_CD", "EFF_FROM_DT": "EFFECTIVE_FROM_DT",
        "EFF_TO_DT": "EFFECTIVE_TO_DT", "RECOVERABLE_FLG": _recoverable,
    },
    "raw_oracle_vendor_contract": {"CONTRACT_NBR": "CONTRACT_NUM", "SUPP_CODE": "SUPP_ID", "COMMIT_AMT": "COMMITTED_AMT", "COMMIT_CCY": "CURRENCY_CD", "DISC_PCT": None, "STATUS_CD": "CONTRACT_STATUS_CD"},
    "raw_file_partner_sales": {
        "PartnerOrderRef": "TransactionReference", "SaleDateText": "TransactionDate", "CustomerRef": "CustomerReference", "ItemRef": "PartnerProductCode",
        "QuantityText": "QuantitySold", "AmountText": "GrossAmount", "CurrencyText": "CurrencyCode", "CountryText": "CountryCode",
    },
    "raw_sql_credit_note": {"InvoiceID": "OriginalInvoiceID", "CreditAmount": "GrossAmount", "IssuedWhen": "CreditNoteDate"},
    "raw_sql_invoice": {"DeliveryMethodCode": "DeliveryMethodID", "BillToRegionCode": None},
    "raw_sql_loyalty_ledger": {"LoyaltyEntryID": "LoyaltyLedgerID", "EntryDate": "EntryWhen"},
    "raw_sql_order_line": {"PackageTypeCode": "PackageTypeID"},
    "raw_sql_return_line": {"InvoiceID": None, "QuantityReturned": "ReturnedQuantity", "RegionCode": None},
    "raw_sql_shipment": {"TrackingNumber": "ShipmentReference", "DestinationCountryCode": "ShipToCountryCode", "DestinationPostalCode": "ShipToPostalCode", "GrossWeightKg": "TotalWeightKg", "DespatchedWhen": "ShippedWhen"},
    "raw_sql_shipment_line": {"QuantityShipped": "ShippedQuantity", "LineWeightKg": "WeightKg"},
    "raw_sql_stock_movement": {"TransactionTypeCode": "TransactionTypeName", "WarehouseSiteId": "WarehouseCode", "UomCode": None},
    "raw_sql_web_session": {
        "SessionGuid": "AnonymousVisitorKey", "ChannelCode": "CampaignCode", "DeviceTypeCode": "DeviceCategory", "UserAgentText": "BrowserFamily",
        "DurationSeconds": _duration, "ConsentFlag": _consent, "SessionStartWhen": "SessionStartedWhen",
    },
}


def conformSource(df, tableName):
    """Add the package-vocabulary columns to a bronze frame (no-op when the
    extract already carries them)."""
    mapping = RAW_COLUMN_MAP.get(tableName, {})
    cols = {}
    for packageColumn, source in mapping.items():
        if packageColumn in df.columns:
            continue
        if source is None:
            cols[packageColumn] = F.lit(None).cast("string")
        elif isinstance(source, str):
            cols[packageColumn] = F.col(source).cast("string") if source not in df.columns else F.col(source)
        else:
            cols[packageColumn] = source()
    return df.withColumns(cols) if cols else df
