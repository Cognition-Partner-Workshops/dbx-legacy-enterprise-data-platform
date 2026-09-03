# Oracle ERP (WWIGERP) deploy order

Run every script with SQL*Plus or SQLcl from the repository root, in the order
below. Each script carries its own header block naming its dependencies.
Schema passwords are supplied as substitution variables at run time
(`ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE`
and the per-schema `&&WWI_*_SECRET` variables); no value is stored in this
repository.

Nothing here has been executed. The ordering is derived from the declared
dependencies in the file headers only.

## 1. Instance and schema objects

- `oracle/ddl/01_create_tablespaces.sql`
- `oracle/ddl/02_create_profiles.sql`
- `oracle/ddl/03_create_schemas.sql`
- `oracle/ddl/04_create_roles.sql`
- `oracle/ddl/05_grant_privileges.sql`
- `oracle/ddl/06_create_directories.sql`
- `oracle/ddl/07_create_synonyms.sql`
- `oracle/tables/ZZ_add_future_partitions.sql`

Scripts 01-07 run before any table script. `oracle/tables/ZZ_add_future_partitions.sql`
runs after the tables exist - it sorts last in the table stage - and is re-run
each year.

## 2. Tables, sequences, constraints and indexes

One file per table, in schema dependency order: reference first, then master
data, then procurement, then finance, then audit.

### WWI_REF (15 tables)

- `oracle/tables/WWI_REF.CALENDAR_FISCAL.sql`
- `oracle/tables/WWI_REF.CITY_REF.sql`
- `oracle/tables/WWI_REF.CODE_TRANSLATION.sql`
- `oracle/tables/WWI_REF.COUNTRY_REF.sql`
- `oracle/tables/WWI_REF.CURRENCY_CODE.sql`
- `oracle/tables/WWI_REF.FX_RATE_DAILY.sql`
- `oracle/tables/WWI_REF.INCOTERM_REF.sql`
- `oracle/tables/WWI_REF.LANGUAGE_REF.sql`
- `oracle/tables/WWI_REF.PAYMENT_METHOD_REF.sql`
- `oracle/tables/WWI_REF.POSTAL_REF.sql`
- `oracle/tables/WWI_REF.REASON_CODE_REF.sql`
- `oracle/tables/WWI_REF.REGION_REF.sql`
- `oracle/tables/WWI_REF.SOURCE_SYSTEM_REF.sql`
- `oracle/tables/WWI_REF.STATUS_CODE_REF.sql`
- `oracle/tables/WWI_REF.UOM_REF.sql`

### WWI_MDM (19 tables)

- `oracle/tables/WWI_MDM.CUST_ADDRESS.sql`
- `oracle/tables/WWI_MDM.CUST_CLASSIFICATION.sql`
- `oracle/tables/WWI_MDM.CUST_CONTACT.sql`
- `oracle/tables/WWI_MDM.CUST_CREDIT_PROFILE.sql`
- `oracle/tables/WWI_MDM.CUST_HIERARCHY.sql`
- `oracle/tables/WWI_MDM.CUST_MASTER.sql`
- `oracle/tables/WWI_MDM.CUST_SEGMENT_ASSIGN.sql`
- `oracle/tables/WWI_MDM.MDM_MERGE_HISTORY.sql`
- `oracle/tables/WWI_MDM.PARTY_XREF.sql`
- `oracle/tables/WWI_MDM.PRODUCT_CATEGORY.sql`
- `oracle/tables/WWI_MDM.PRODUCT_HIERARCHY.sql`
- `oracle/tables/WWI_MDM.PRODUCT_MASTER.sql`
- `oracle/tables/WWI_MDM.PRODUCT_SUBSTITUTE.sql`
- `oracle/tables/WWI_MDM.PRODUCT_UOM_CONV.sql`
- `oracle/tables/WWI_MDM.SUPP_ADDRESS.sql`
- `oracle/tables/WWI_MDM.SUPP_BANK_ACCOUNT.sql`
- `oracle/tables/WWI_MDM.SUPP_CERTIFICATION.sql`
- `oracle/tables/WWI_MDM.SUPP_CONTACT.sql`
- `oracle/tables/WWI_MDM.SUPP_MASTER.sql`

### WWI_PROC (15 tables)

- `oracle/tables/WWI_PROC.GOODS_RETURN_HDR.sql`
- `oracle/tables/WWI_PROC.GOODS_RETURN_LINE.sql`
- `oracle/tables/WWI_PROC.PO_CHANGE_ORDER.sql`
- `oracle/tables/WWI_PROC.PO_RECEIPT_HDR.sql`
- `oracle/tables/WWI_PROC.PO_RECEIPT_LINE.sql`
- `oracle/tables/WWI_PROC.PURCHASE_ORDER_HDR.sql`
- `oracle/tables/WWI_PROC.PURCHASE_ORDER_LINE.sql`
- `oracle/tables/WWI_PROC.REQUISITION_HDR.sql`
- `oracle/tables/WWI_PROC.REQUISITION_LINE.sql`
- `oracle/tables/WWI_PROC.SOURCING_EVENT.sql`
- `oracle/tables/WWI_PROC.SUPPLIER_QUOTE.sql`
- `oracle/tables/WWI_PROC.SUPPLIER_QUOTE_LINE.sql`
- `oracle/tables/WWI_PROC.SUPPLIER_SCORECARD.sql`
- `oracle/tables/WWI_PROC.VENDOR_CONTRACT.sql`
- `oracle/tables/WWI_PROC.VENDOR_CONTRACT_LINE.sql`

### WWI_FIN (16 tables)

- `oracle/tables/WWI_FIN.AP_AGING_SNAPSHOT.sql`
- `oracle/tables/WWI_FIN.AP_INVOICE_HDR.sql`
- `oracle/tables/WWI_FIN.AP_INVOICE_HOLD.sql`
- `oracle/tables/WWI_FIN.AP_INVOICE_LINE.sql`
- `oracle/tables/WWI_FIN.AP_PAYMENT.sql`
- `oracle/tables/WWI_FIN.AP_PAYMENT_APPLY.sql`
- `oracle/tables/WWI_FIN.COST_ALLOCATION_RULE.sql`
- `oracle/tables/WWI_FIN.COST_CENTER.sql`
- `oracle/tables/WWI_FIN.GL_ACCOUNT.sql`
- `oracle/tables/WWI_FIN.GL_JOURNAL_HDR.sql`
- `oracle/tables/WWI_FIN.GL_JOURNAL_LINE.sql`
- `oracle/tables/WWI_FIN.GL_PERIOD_STATUS.sql`
- `oracle/tables/WWI_FIN.PAYMENT_TERMS.sql`
- `oracle/tables/WWI_FIN.TAX_JURISDICTION.sql`
- `oracle/tables/WWI_FIN.TAX_RATE.sql`
- `oracle/tables/WWI_FIN.WITHHOLDING_RULE.sql`

### WWI_AUDIT (4 tables)

- `oracle/tables/WWI_AUDIT.CHANGE_LOG.sql`
- `oracle/tables/WWI_AUDIT.EXTRACT_CONTROL.sql`
- `oracle/tables/WWI_AUDIT.INTERFACE_ERROR.sql`
- `oracle/tables/WWI_AUDIT.PURGE_LOG.sql`

## 3. Reference content

- `oracle/reference/01_region_country_geography.sql`
- `oracle/reference/02_currency_and_fx_rates.sql`
- `oracle/reference/03_city_and_postal_geography.sql`
- `oracle/reference/04_uom_language_incoterm.sql`
- `oracle/reference/05_tax_na_sales_and_use.sql`
- `oracle/reference/06_tax_eu_vat.sql`
- `oracle/reference/07_tax_apac_gst.sql`
- `oracle/reference/08_payment_terms_and_methods.sql`
- `oracle/reference/09_status_and_reason_codes.sql`
- `oracle/reference/10_chart_of_accounts_and_cost_centers.sql`
- `oracle/reference/11_fiscal_calendars_and_periods.sql`
- `oracle/reference/12_withholding_rules.sql`

## 4. Seed and configuration data

- `oracle/seed/01_source_system_ref.sql`
- `oracle/seed/02_code_translation_external_codes.sql`
- `oracle/seed/03_code_translation_parameters.sql`
- `oracle/seed/04_extract_control_seed.sql`
- `oracle/seed/05_cost_allocation_rules.sql`

Seeded surrogate keys are chosen below the START WITH value of the matching
`SEQ_<TABLE>` sequence so a later sequence-driven insert cannot collide with them.
