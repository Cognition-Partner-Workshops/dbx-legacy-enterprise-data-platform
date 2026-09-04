"""How each Oracle producer binds to the table the ERP actually has.

Only the drift the mechanical aliases in :mod:`wwigen.conform` cannot resolve
is declared here: renames that need a human to know what the column means,
producer columns the ERP has no home for, and the surrogate keys the schema
uses where the producers speak in business codes.

``key`` names the producer column carrying the row's own business code - the
table's numeric primary key is derived from it - and ``parents`` does the same
for a foreign key, so a child row resolves its parent without the two tables
having to be generated together.
"""

from __future__ import annotations

from ..conform import (Rule, by_region, code_id, copy_of, line_id,
                       region_of_country)

# Regional divergence the reference tables carry. Same source as the rest of
# the estate: the region drives calendar, tax regime and formatting.
FISCAL_CALENDAR = {"NA": "NA_454", "EU": "EU_CAL", "APAC": "APAC_445"}
TAX_REGIME = {"NA": "SALES", "EU": "VAT", "APAC": "GST"}
ADDRESS_FORMAT = {"NA": "US_STD", "EU": "EU_STD", "APAC": "APAC_STD"}
POSTAL_FORMAT = {"NA": "NNNNN", "EU": "AN_MIX", "APAC": "NNNN"}
DATE_FORMAT = {"NA": "MM/DD/YYYY", "EU": "DD/MM/YYYY", "APAC": "YYYY-MM-DD"}
LANGUAGE = {"NA": "en-US", "EU": "en-GB", "APAC": "en-AU"}
CONSENT_REGIME = {"NA": "CANSPAM", "EU": "GDPR", "APAC": "APPI"}
FISCAL_START_MONTH = {"NA": 1, "EU": 4, "APAC": 7}

AUDIT_DROPS = ("ROW_VERSION_NO",)


def _iso3(cfg, values, index):
    """A three-letter country code derived from the two-letter one."""
    code = (values.get("COUNTRY_CD") or "XX").upper()
    return (code + code[-1]) if len(code) == 2 else code[:3]


def _upper(column):
    def resolve(cfg, values, index):
        value = values.get(column)
        return None if value is None else str(value).upper()
    return resolve


RULES = {

    # -- WWI_REF ---------------------------------------------------------

    "WWI_REF.CURRENCY_CODE": Rule(
        rename={"CURRENCY_NM": "CURR_NAME", "MINOR_UNIT_QTY": "MINOR_UNIT_DIGITS",
                "SYMBOL_TX": "CURR_SYMBOL"},
    ),
    "WWI_REF.FX_RATE_DAILY": Rule(
        rename={"SOURCE_CD": "RATE_SOURCE_CD", "OVERRIDE_FLG": "SUPERSEDED_FLG"},
    ),
    "WWI_REF.COUNTRY_REF": Rule(
        rename={"CURRENCY_CD": "DEFAULT_CURR_CD", "DIAL_PREFIX_TX": "PHONE_PREFIX_CD",
                "POSTAL_FORMAT_CD": "POSTAL_FORMAT_TXT"},
        drop=("ADDRESS_FORMAT_CD", "STD_TAX_RATE", "TAX_REGIME_CD",
              "FISCAL_START_MONTH_NO"),
        fill={"COUNTRY_CD_3": _iso3},
    ),
    "WWI_REF.REGION_REF": Rule(
        rename={"FISCAL_START_MONTH_NO": "FISCAL_YEAR_START_MONTH",
                "CONSENT_BASIS_CD": "CONSENT_REGIME_CD"},
        drop=("FX_CONVENTION_CD", "CLASS_SCHEME_CD"),
        fill={
            "FISCAL_CALENDAR_CD": by_region(FISCAL_CALENDAR, "NA_454"),
            "TAX_REGIME_CD": by_region(TAX_REGIME, "SALES"),
            "ADDRESS_FORMAT_CD": by_region(ADDRESS_FORMAT, "US_STD"),
            "POSTAL_FORMAT_CD": by_region(POSTAL_FORMAT, "NNNNN"),
            "DATE_FORMAT_MASK": by_region(DATE_FORMAT, "MM/DD/YYYY"),
            "DEFAULT_LANGUAGE_CD": by_region(LANGUAGE, "en-US"),
            "CONSENT_REGIME_CD": by_region(CONSENT_REGIME, "CANSPAM"),
        },
    ),
    "WWI_REF.CALENDAR_FISCAL": Rule(
        rename={"CAL_DT": "CALENDAR_DT", "FISCAL_PERIOD_CD": "PERIOD_CD",
                "BUSINESS_DAY_FLG": "WORKING_DAY_FLG"},
        drop=("PERIOD_END_FLG", "QUARTER_END_FLG"),
        fill={
            "CALENDAR_CD": by_region(FISCAL_CALENDAR, "NA_454"),
            "PERIOD_START_DT": copy_of("CAL_DT"),
            "PERIOD_END_DT": copy_of("CAL_DT"),
        },
    ),
    "WWI_REF.UOM_REF": Rule(
        rename={"BASE_FACTOR": "CONVERSION_FACTOR"},
        fill={"BASE_UOM_CD": copy_of("UOM_CD")},
    ),
    "WWI_REF.STATUS_CODE_REF": Rule(
        rename={"CODE_SET_CD": "ENTITY_CD", "CODE_VALUE": "STATUS_CD",
                "CODE_DESC": "STATUS_NAME", "SORT_ORDER_NO": "DISPLAY_SEQ_NBR"},
    ),
    "WWI_REF.CODE_TRANSLATION": Rule(
        rename={"FROM_CODE": "SOURCE_VALUE_TXT", "TO_CODE": "TARGET_VALUE_TXT",
                "FROM_REGION_CD": "REGION_CD", "NOTE_TX": "DESCRIPTION_TXT",
                "APPROVED_FLG": "ACTIVE_FLG"},
        drop=("TO_REGION_CD", "CANONICAL_CD"),
        fill={"SOURCE_SYS_CD": by_region({"NA": "ERPNA", "EU": "ERPEU",
                                          "APAC": "ERPAP"}, "ERPNA",
                                         column="FROM_REGION_CD")},
    ),
    "WWI_REF.SOURCE_SYSTEM_REF": Rule(
        rename={"SRC_SYSTEM_NM": "SYSTEM_NAME", "PLATFORM_TX": "VENDOR_TXT",
                "OWNER_TEAM_TX": "OWNING_TEAM_TXT",
                "EXTRACT_METHOD_CD": "INTERFACE_MODE_CD"},
    ),
    "WWI_REF.PAYMENT_METHOD_REF": Rule(
        rename={"CLEARING_DAYS": "SETTLEMENT_DAYS",
                "REQUIRES_BANK_FLG": "REQUIRES_ROUTING_FLG",
                "FEE_CURRENCY_CD": "METHOD_CURR_CD"},
        drop=("FEE_AMT",),
        fill={"FILE_FORMAT_CD": by_region({"NA": "NACHA", "EU": "SEPA_XML",
                                           "APAC": "ISO20022"}, "NACHA")},
    ),
    "WWI_REF.CITY_REF": Rule(
        key="CITY_ID",
        drop=("CITY_ID",),
        fill={"CITY_NAME_UPPER": _upper("CITY_NM")},
    ),

    # -- WWI_MDM ---------------------------------------------------------

    "WWI_MDM.CUST_MASTER": Rule(
        key="CUST_CODE",
        rename={"CUST_CODE": "CUST_NBR", "LEGACY_ACCT_NO": "LEGACY_CUST_CD",
                "STATUS_CD": "CUST_STATUS_CD", "CURRENCY_CD": "PRIMARY_CURR_CD",
                "TAX_REG_NO": "TAX_REG_NBR", "TAX_REG_FLG": "TAX_EXEMPT_FLG",
                "SALES_REP_ID": "ACCT_MANAGER_CD", "CONSENT_CD": "CONSENT_SOURCE_CD",
                "MARKETING_FLG": "CONSENT_MARKETING_FLG",
                "OPEN_DT": "FIRST_ORDER_DT", "MERGE_CAND_FLG": "MISC_FLAG_1",
                "CUST_CLASS_CD": "CUST_TYPE_CD"},
        drop=("CREDIT_LIMIT_AMT", "RETENTION_MONTHS") + AUDIT_DROPS,
    ),
    "WWI_MDM.CUST_ADDRESS": Rule(
        parents={"CUST_ID": "CUST_CODE"},
        rename={"GEOCODE_QLTY_CD": "ADDR_VERIFY_VENDOR_CD",
                "STD_STATUS_CD": "POSTAL_CD_NORM"},
        drop=("CUST_CODE",) + AUDIT_DROPS,
        fill={"REGION_CD": region_of_country()},
    ),
    "WWI_MDM.CUST_CONTACT": Rule(
        parents={"CUST_ID": "CUST_CODE"},
        rename={"FIRST_NM": "GIVEN_NAME", "LAST_NM": "FAMILY_NAME",
                "ROLE_CD": "CONTACT_ROLE_CD", "PREF_LANG_CD": "LANGUAGE_CD",
                "CONSENT_DT": "CONSENT_UPDATED_DT"},
        drop=("CUST_CODE", "CONTACT_SEQ_NO", "CONSENT_CD", "OPT_OUT_DT") + AUDIT_DROPS,
    ),
    "WWI_MDM.CUST_CLASSIFICATION": Rule(
        parents={"CUST_ID": "CUST_CODE"},
        rename={"CLASS_SEQ_NO": "CLASS_LEVEL_NBR", "REASON_CD": "ASSIGN_METHOD_CD",
                "APPROVED_BY": "ASSIGNED_BY_CD"},
        drop=("CUST_CODE",) + AUDIT_DROPS,
    ),
    "WWI_MDM.CUST_CREDIT_PROFILE": Rule(
        parents={"CUST_ID": "CUST_CODE"},
        rename={"CREDIT_CURRENCY_CD": "CREDIT_LIMIT_CURR_CD",
                "RISK_SCORE": "RISK_SCORE_NBR", "RISK_BAND_CD": "RISK_CLASS_CD",
                "COLLECTOR_ID": "REVIEWED_BY_CD"},
        drop=("CUST_CODE", "ON_HOLD_FLG") + AUDIT_DROPS,
    ),
    "WWI_MDM.SUPP_MASTER": Rule(
        key="SUPP_CODE",
        rename={"SUPP_CODE": "SUPP_NBR", "STATUS_CD": "SUPP_STATUS_CD",
                "CURRENCY_CD": "DEFAULT_CURR_CD", "TAX_REG_NO": "VAT_REG_NBR",
                "SCORECARD_BAND_CD": "STRATEGIC_TIER_CD",
                "ONBOARD_DT": "APPROVED_DT"},
        drop=("CERTIFICATION_CD", "APPROVED_FLG", "LAST_AUDIT_DT",
              "SINGLE_SOURCE_FLG") + AUDIT_DROPS,
    ),
    "WWI_MDM.SUPP_ADDRESS": Rule(
        parents={"SUPP_ID": "SUPP_CODE"},
        rename={"ADDR_TYPE_CD": "SITE_TYPE_CD"},
        drop=("SUPP_CODE", "ADDR_SEQ_NO", "VALID_FROM_DT") + AUDIT_DROPS,
        fill={"SITE_CD": lambda cfg, values, index: "SITE%03d" % (index % 999 + 1)},
    ),
    "WWI_MDM.SUPP_BANK_ACCOUNT": Rule(
        parents={"SUPP_ID": "SUPP_CODE"},
        rename={"BANK_SEQ_NO": "BANK_ACCT_SEQ_NBR", "ACCOUNT_MASK": "ACCOUNT_NBR_LAST4",
                "ROUTING_REF": "ROUTING_NBR", "SWIFT_BIC": "BIC_CD",
                "CURRENCY_CD": "ACCOUNT_CURR_CD", "VERIFIED_FLG": "VALIDATED_FLG",
                "VERIFIED_DT": "VALIDATED_DT"},
        drop=("SUPP_CODE",) + AUDIT_DROPS,
        fill={"BANK_COUNTRY_CD": copy_of("COUNTRY_CD", "US")},
    ),
    "WWI_MDM.PRODUCT_MASTER": Rule(
        key="ITEM_CODE",
        rename={"ITEM_CODE": "ITEM_NBR", "UOM_CD": "PRIMARY_UOM_CD",
                "STD_COST_AMT": "UNIT_COST_STD",
                "PRICE_CURRENCY_CD": "LIST_PRICE_CURR_CD",
                "DISCONTINUED_FLG": "DELETED_FLG",
                "NET_WEIGHT_KG": "UNIT_WEIGHT_KG", "TARIFF_CD": "HS_TARIFF_CD"},
        drop=("CATEGORY_CD", "PACK_SIZE_QTY", "PRIMARY_SUPP_CD",
              "LEAD_TIME_DAYS") + AUDIT_DROPS,
    ),
    "WWI_MDM.PRODUCT_CATEGORY": Rule(
        key="CATEGORY_CD",
        parents={"PARENT_CATEGORY_ID": "PARENT_CATEGORY_CD"},
        rename={"LEVEL_NO": "CATEGORY_LEVEL_NBR"},
        drop=("EFF_FROM_DT",) + AUDIT_DROPS,
    ),
    "WWI_MDM.PRODUCT_HIERARCHY": Rule(
        parents={"PRODUCT_ID": "ITEM_CODE"},
        drop=("ITEM_CODE",) + AUDIT_DROPS,
    ),
    "WWI_MDM.PARTY_XREF": Rule(
        key="XREF_ID",
        rename={"ERP_CODE": "SOURCE_KEY_TXT",
                "TARGET_SYSTEM_CD": "SOURCE_SYS_CD",
                "MATCH_CONFIDENCE": "MATCH_SCORE"},
        drop=("XREF_ID", "MATCH_STATUS_CD", "EFF_FROM_DT", "EFF_TO_DT",
              "TARGET_KEY") + AUDIT_DROPS,
    ),
    "WWI_MDM.MDM_MERGE_HISTORY": Rule(
        parents={"SURVIVOR_PARTY_ID": "SURVIVOR_CODE",
                 "MERGED_PARTY_ID": "MERGED_CODE"},
        rename={"MERGED_BY": "MERGED_BY_CD", "MERGED_DT": "MERGE_DT",
                "REVERSED_FLG": "UNMERGE_FLG"},
        drop=("SURVIVOR_CODE", "MERGED_CODE", "MATCH_SCORE") + AUDIT_DROPS,
    ),

    # -- WWI_PROC --------------------------------------------------------

    "WWI_PROC.REQUISITION_HDR": Rule(
        key="REQ_NO",
        rename={"REQUESTER_ID": "REQUESTOR_CD", "REQ_DT": "REQUEST_DT",
                "STATUS_CD": "REQ_STATUS_CD", "APPROVER_ID": "APPROVER_1_CD",
                "APPROVED_DT": "APPROVER_1_DT", "EST_TOTAL_AMT": "ESTIMATED_AMT",
                "CURRENCY_CD": "ESTIMATED_CURR_CD"},
        drop=("PO_NO",),
    ),
    "WWI_PROC.REQUISITION_LINE": Rule(
        parents={"REQ_ID": "REQ_NO", "PRODUCT_ID": "ITEM_CODE"},
        rename={"REQ_LINE_NO": "LINE_NBR", "EST_UNIT_PRICE": "ESTIMATED_UNIT_PRICE",
                "EST_LINE_AMT": "ESTIMATED_LINE_AMT",
                "SUGGESTED_SUPP_CD": "SUGGESTED_SUPP_TXT"},
        drop=("REQ_NO", "ITEM_CODE"),
        fill={"REQ_LINE_ID": line_id("REQ_NO", "REQ_LINE_NO"),
              "LINE_CURR_CD": copy_of("CURRENCY_CD", "USD")},
    ),
    "WWI_PROC.PURCHASE_ORDER_HDR": Rule(
        key="PO_NO",
        parents={"SUPP_ID": "SUPP_CODE", "CONTRACT_ID": "CONTRACT_NO"},
        rename={"BUYER_ID": "BUYER_CD", "CURRENCY_CD": "ORDER_CURR_CD",
                "PO_DT": "ORDER_DT", "STATUS_CD": "PO_STATUS_CD",
                "SHIP_TO_SITE_CD": "SHIP_TO_LOCATION_CD",
                "PO_TOTAL_AMT": "TOTAL_AMT", "PO_TAX_AMT": "TAX_AMT"},
        drop=("SUPP_CODE", "CONTRACT_NO", "PO_TOTAL_BASE_AMT", "BASE_CURRENCY_CD",
              "BLANKET_FLG", "APPROVED_DT"),
    ),
    "WWI_PROC.PURCHASE_ORDER_LINE": Rule(
        parents={"PO_ID": "PO_NO", "PRODUCT_ID": "ITEM_CODE"},
        rename={"PO_LINE_NO": "LINE_NBR",
                "LINE_NET_AMT": "LINE_AMT", "LINE_TAX_AMT": "TAX_AMT",
                "TAX_CODE": "TAX_CODE_CD", "CHARGE_ACCOUNT_CD": "GL_ACCOUNT_CD"},
        drop=("PO_NO", "ITEM_CODE"),
        fill={"PO_LINE_ID": line_id("PO_NO", "PO_LINE_NO"),
              "ORDER_DT": copy_of("CREATED_TS"),
              "LINE_CURR_CD": copy_of("CURRENCY_CD", "USD")},
    ),
    "WWI_PROC.PO_CHANGE_ORDER": Rule(
        parents={"PO_ID": "PO_NO"},
        rename={"REASON_CD": "CHANGE_REASON_CD", "CHANGED_BY": "REQUESTED_BY_CD",
                "CHANGED_DT": "REQUESTED_DT"},
        drop=("PO_NO", "PO_LINE_NO"),
        fill={"PO_LINE_ID": line_id("PO_NO", "PO_LINE_NO")},
    ),
    "WWI_PROC.PO_RECEIPT_HDR": Rule(
        key="RECEIPT_NO",
        parents={"SUPP_ID": "SUPP_CODE"},
        rename={"SITE_CD": "WAREHOUSE_CD",
                "RECEIVER_ID": "RECEIVED_BY_CD", "STATUS_CD": "RECEIPT_STATUS_CD"},
        drop=("PO_NO", "SUPP_CODE", "INSPECTION_REQ_FLG", "RECEIVED_TS"),
    ),
    "WWI_PROC.PO_RECEIPT_LINE": Rule(
        parents={"RECEIPT_ID": "RECEIPT_NO", "PO_ID": "PO_NO",
                 "PRODUCT_ID": "ITEM_CODE"},
        rename={"RECEIPT_LINE_NO": "LINE_NBR", "BIN_CD": "PUTAWAY_LOCATION_CD"},
        drop=("RECEIPT_NO", "PO_NO", "ITEM_CODE"),
        fill={"RECEIPT_LINE_ID": line_id("RECEIPT_NO", "RECEIPT_LINE_NO"),
              "PO_LINE_ID": line_id("PO_NO", "PO_LINE_NO")},
    ),
    "WWI_PROC.GOODS_RETURN_HDR": Rule(
        key="RETURN_NO",
        parents={"SUPP_ID": "SUPP_CODE", "RECEIPT_ID": "RECEIPT_NO"},
        rename={"REASON_CD": "RETURN_REASON_CD", "STATUS_CD": "RETURN_STATUS_CD",
                "CURRENCY_CD": "CREDIT_CURR_CD"},
        drop=("PO_NO", "SUPP_CODE", "CREDIT_RECEIVED_FLG", "ITEM_CODE",
              "RETURN_QTY"),
    ),
    "WWI_PROC.VENDOR_CONTRACT": Rule(
        key="CONTRACT_NO",
        parents={"SUPP_ID": "SUPP_CODE"},
        rename={"EFF_FROM_DT": "START_DT", "COMMIT_AMT": "COMMITTED_AMT",
                "CURRENCY_CD": "CONTRACT_CURR_CD", "OWNER_ID": "OWNER_CD",
                "STATUS_CD": "CONTRACT_STATUS_CD"},
        drop=("SUPP_CODE",),
    ),
    "WWI_PROC.SUPPLIER_SCORECARD": Rule(
        parents={"SUPP_ID": "SUPP_CODE"},
        rename={"PERIOD_CD": "SCORE_PERIOD_CD", "DEFECT_PPM": "QUALITY_REJECT_PCT",
                "RECEIPTS_QTY": "RECEIPT_LINE_CNT", "BAND_CD": "SCORE_BAND_CD",
                "REVIEWED_DT": "CALCULATED_DT"},
        drop=("SUPP_CODE", "REJECTS_QTY", "REVIEWER_ID"),
    ),

    # -- WWI_FIN ---------------------------------------------------------

    "WWI_FIN.AP_INVOICE_HDR": Rule(
        key="INVOICE_NO",
        parents={"SUPP_ID": "SUPP_CODE", "PO_ID": "PO_NO"},
        rename={"CURRENCY_CD": "INVOICE_CURR_CD", "GL_DT": "GL_DATE",
                "WITHHOLDING_AMT": "WITHHELD_AMT", "STATUS_CD": "INVOICE_STATUS_CD",
                "SUPPLIER_INVOICE_REF": "SCANNED_IMAGE_REF"},
        drop=("SUPP_CODE", "PO_NO", "BASE_AMT", "BASE_CURRENCY_CD", "FX_TYPE_CD",
              "MATCHED_FLG", "HOLD_CD"),
    ),
    "WWI_FIN.AP_INVOICE_LINE": Rule(
        parents={"INVOICE_ID": "INVOICE_NO", "PRODUCT_ID": "ITEM_CODE"},
        rename={"INVOICE_LINE_NO": "LINE_NBR",
                "QTY": "QUANTITY", "TAX_CODE": "TAX_CODE_CD",
                "GL_ACCOUNT_CD": "ACCOUNT_CD",
                "LINE_TAX_AMT": "RECOVERABLE_TAX_AMT"},
        drop=("INVOICE_NO", "PO_NO", "PO_LINE_NO", "VARIANCE_CD", "PERIOD_CD",
              "ITEM_CODE"),
        fill={"INVOICE_LINE_ID": line_id("INVOICE_NO", "INVOICE_LINE_NO"),
              "LINE_CURR_CD": copy_of("CURRENCY_CD", "USD")},
    ),
    "WWI_FIN.AP_INVOICE_HOLD": Rule(
        parents={"INVOICE_ID": "INVOICE_NO"},
        rename={"HOLD_CD": "HOLD_CODE_CD", "PLACED_BY": "PLACED_BY_CD",
                "RELEASED_BY": "RELEASED_BY_CD", "MANUAL_FLG": "ESCALATED_FLG"},
        drop=("INVOICE_NO", "PERIOD_CD"),
        fill={"HOLD_ID": line_id("INVOICE_NO", "HOLD_SEQ_NO")},
    ),
    "WWI_FIN.AP_PAYMENT": Rule(
        key="PAYMENT_NO",
        parents={"SUPP_ID": "SUPP_CODE"},
        rename={"BANK_ACCOUNT_MASK": "BANK_ACCOUNT_CD",
                "CURRENCY_CD": "PAYMENT_CURR_CD", "VALUE_DT": "CLEARED_DT",
                "BASE_AMT": "ACCOUNTED_AMT", "STATUS_CD": "PAYMENT_STATUS_CD",
                "REMITTANCE_REF": "REMITTANCE_EMAIL_TXT"},
        drop=("SUPP_CODE", "BASE_CURRENCY_CD", "PAYMENT_RUN_ID", "VOID_FLG"),
    ),
    "WWI_FIN.AP_PAYMENT_APPLY": Rule(
        parents={"PAYMENT_ID": "PAYMENT_NO", "INVOICE_ID": "INVOICE_NO"},
        rename={"WRITEOFF_AMT": "WITHHELD_AMT", "APPLIED_DT": "APPLY_DT",
                "APPLY_STATUS_CD": "APPLY_TYPE_CD", "PARTIAL_FLG": "POSTED_FLG"},
        drop=("PAYMENT_NO", "INVOICE_NO", "PERIOD_CD"),
        fill={"APPLIED_CURR_CD": copy_of("CURRENCY_CD", "USD")},
    ),
    "WWI_FIN.AP_AGING_SNAPSHOT": Rule(
        parents={"SUPP_ID": "SUPP_CODE"},
        rename={"CURRENCY_CD": "BALANCE_CURR_CD", "BUCKET_1_30_AMT": "BUCKET_1_AMT",
                "BUCKET_31_60_AMT": "BUCKET_2_AMT", "BUCKET_61_90_AMT": "BUCKET_3_AMT",
                "BUCKET_OVER_90_AMT": "BUCKET_4_AMT",
                "TOTAL_AMT": "TOTAL_OUTSTANDING_AMT",
                "AGING_BASIS_CD": "BUCKET_DEFINITION_CD"},
        drop=("SUPP_CODE",),
    ),
    "WWI_FIN.GL_ACCOUNT": Rule(
        key="GL_ACCOUNT_CD",
        rename={"GL_ACCOUNT_CD": "ACCOUNT_CD", "REGION_CD": "SEGMENT_1_CD"},
        drop=("ACTIVE_FLG",),
        fill={"SEGMENT_3_CD": lambda cfg, values, index:
              (values.get("GL_ACCOUNT_CD") or "000000")[-6:]},
    ),
    "WWI_FIN.GL_JOURNAL_HDR": Rule(
        key="JOURNAL_NO",
        rename={"JOURNAL_DT": "ACCOUNTING_DT", "STATUS_CD": "POSTING_STATUS_CD",
                "CONTROL_TOTAL_AMT": "TOTAL_DEBIT_AMT",
                "CURRENCY_CD": "JOURNAL_CURR_CD",
                "PREPARED_BY": "PREPARER_NOTES_TXT",
                "APPROVED_BY": "APPROVED_BY_CD"},
        parents={"REVERSAL_OF_JOURNAL_ID": "REVERSES_JOURNAL_NO"},
        drop=("PERIOD_CD", "REVERSES_JOURNAL_NO"),
        fill={"LEDGER_CD": by_region({"NA": "USD_PRI", "EU": "EUR_PRI",
                                      "APAC": "APAC_PRI"}, "USD_PRI")},
    ),
    "WWI_FIN.GL_JOURNAL_LINE": Rule(
        parents={"JOURNAL_ID": "JOURNAL_NO"},
        rename={"JOURNAL_LINE_NO": "LINE_NBR", "GL_ACCOUNT_CD": "ACCOUNT_CD",
                "DEBIT_AMT": "ENTERED_DEBIT_AMT", "CREDIT_AMT": "ENTERED_CREDIT_AMT",
                "CURRENCY_CD": "ENTERED_CURR_CD",
                "BASE_DEBIT_AMT": "ACCOUNTED_DEBIT_AMT",
                "BASE_CREDIT_AMT": "ACCOUNTED_CREDIT_AMT",
                "LINE_DESC_TX": "LINE_DESC", "REFERENCE_NO": "REFERENCE_1_TXT",
                "EFFECTIVE_DT": "ACCOUNTING_DT"},
        drop=("JOURNAL_NO", "REGION_CD", "PERIOD_CD"),
        fill={"JOURNAL_LINE_ID": line_id("JOURNAL_NO", "JOURNAL_LINE_NO"),
              "LEDGER_CURR_CD": copy_of("CURRENCY_CD", "USD")},
    ),
    "WWI_FIN.COST_CENTER": Rule(
        key="COST_CENTER_CD",
        rename={"MANAGER_ID": "MANAGER_CD", "PARENT_CC_CD": "PARENT_COST_CENTER_CD",
                "EFF_FROM_DT": "OPENED_DT", "EFF_TO_DT": "CLOSED_DT"},
    ),
    "WWI_FIN.PAYMENT_TERMS": Rule(
        key="TERMS_CD",
        rename={"TERMS_NM": "TERMS_DESC", "DISCOUNT_DAYS": "DISCOUNT_1_DAYS",
                "DISCOUNT_PCT": "DISCOUNT_1_PCT", "DUE_BASIS_CD": "TERM_BASIS_CD"},
    ),
    "WWI_FIN.TAX_RATE": Rule(
        key="TAX_CODE",
        rename={"TAX_CODE": "TAX_CODE_CD",
                "ROUNDING_RULE_CD": "REPORTING_BOX_CD"},
        drop=("RECOVERABLE_FLG", "COUNTRY_CD"),
    ),
}
