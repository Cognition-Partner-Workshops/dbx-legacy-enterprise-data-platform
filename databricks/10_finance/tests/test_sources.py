import finance_sources as sources


def test_adapters_alias_staging_columns_to_package_contract():
    gl = sources.glJournalLineSql("wwi_dev")
    assert "wwi_dev.silver.stg_gl_journal_line" in gl
    assert "AccountedDebitAmount AS FunctionalDebitAmount" in gl
    assert "FiscalPeriodLabel AS AccountingPeriod" in gl
    assert "CASE WHEN IsPosted THEN 'POSTED' ELSE 'UNPOSTED' END AS JournalStatusCode" in gl
    ap = sources.apInvoiceSql("wwi_dev")
    assert "AmountPaid AS PaidAmount" in ap and "CAST(NULL AS STRING) AS SupplierSiteCode" in ap
    lines = sources.apInvoiceLineSql("wwi_dev")
    assert "l.TaxJurisdictionCode AS JurisdictionCode" in lines and "LEFT JOIN wwi_dev.silver.stg_ap_invoice AS i" in lines


def test_unmapped_columns_are_reported():
    unmapped = sources.unmappedColumns()
    assert unmapped["stg.ApInvoice"] == ["SupplierSiteCode"]
    assert set(unmapped["stg.ApInvoiceLine"]) == {"SupplierTaxRegistrationNumber", "ServiceCategoryCode"}
    assert "stg.GlJournalLine" not in unmapped
