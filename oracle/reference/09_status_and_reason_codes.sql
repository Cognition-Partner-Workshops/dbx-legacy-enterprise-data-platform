/* =====================================================================
 * Object       : Reference content - WWI_REF.STATUS_CODE_REF, WWI_REF.REASON_CODE_REF
 * Schema       : WWI_REF / WWI_FIN (Oracle ERP - WWIGERP)
 * Deploy order : 108
 * Depends on   : oracle/tables/WWI_REF.STATUS_CODE_REF.sql, oracle/tables/WWI_REF.REASON_CODE_REF.sql
 * Called by    : run once per environment after the table DDL
 *
 * The decode sets for the status and reason columns.
 *
 * 'OPEN' appears three times with three different meanings, once per entity,
 * which is exactly how the estate behaves: the code alone is meaningless
 * without knowing which table it came from.
 *
 * The reason codes are the clearest regional divergence in the reference data.
 * NA return reasons are quality-and-damage oriented and feed the supplier
 * scorecard; EU adds withdrawal and consumer-rights reasons that carry no
 * supplier fault; APAC uses a numeric-style code set inherited from the
 * Singapore distributor system and marks most reasons as excluded from KPIs.
 * ===================================================================== */

SET DEFINE OFF

INSERT ALL
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PURCHASE_ORDER', 'DRAFT', 'Draft', 'Entered but not submitted for approval.', 'PO_LIFE', 10,
            'N', 'N', 'Y', 'PENDAPP,CANC',
            NULL, 'D', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PURCHASE_ORDER', 'PENDAPP', 'Pending approval', 'Awaiting the approval hierarchy.', 'PO_LIFE', 20,
            'N', 'N', 'N', 'APPR,REJ,DRAFT',
            NULL, 'P', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PURCHASE_ORDER', 'APPR', 'Approved', 'Approved and released to the supplier.', 'PO_LIFE', 30,
            'N', 'N', 'N', 'OPEN,CANC',
            NULL, 'A', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PURCHASE_ORDER', 'OPEN', 'Open', 'Released, receipts expected.', 'PO_LIFE', 40,
            'N', 'N', 'N', 'PARTRCV,CLOSED,CANC',
            NULL, 'O', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PURCHASE_ORDER', 'PARTRCV', 'Partially received', 'Some lines received, others outstanding.', 'PO_LIFE', 50,
            'N', 'N', 'N', 'CLOSED,OPEN',
            NULL, 'R', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PURCHASE_ORDER', 'CLOSED', 'Closed', 'Fully received and invoiced.', 'PO_LIFE', 60,
            'Y', 'N', 'N', NULL,
            NULL, 'C', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PURCHASE_ORDER', 'CANC', 'Cancelled', 'Cancelled before or during fulfilment.', 'PO_LIFE', 70,
            'Y', 'N', 'N', NULL,
            NULL, 'X', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PO_RECEIPT', 'OPEN', 'Open', 'Received into the dock, not yet inspected.', 'RCV_LIFE', 10,
            'N', 'N', 'Y', 'INSP,ACCEPT',
            NULL, 'O', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PO_RECEIPT', 'INSP', 'In inspection', 'Held for incoming quality inspection.', 'RCV_LIFE', 20,
            'N', 'N', 'Y', 'ACCEPT,REJECT',
            NULL, 'I', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PO_RECEIPT', 'ACCEPT', 'Accepted', 'Accepted into inventory.', 'RCV_LIFE', 30,
            'Y', 'N', 'N', NULL,
            NULL, 'A', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('PO_RECEIPT', 'REJECT', 'Rejected', 'Rejected at inspection, return expected.', 'RCV_LIFE', 40,
            'N', 'Y', 'N', 'RETURN',
            NULL, 'J', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('AP_INVOICE', 'ENTERED', 'Entered', 'Keyed or interfaced, not yet matched.', 'AP_LIFE', 10,
            'N', 'N', 'Y', 'MATCHED,HOLD,CANC',
            NULL, 'E', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('AP_INVOICE', 'MATCHED', 'Matched', 'Three-way match passed within tolerance.', 'AP_LIFE', 20,
            'N', 'N', 'N', 'APPR,HOLD',
            NULL, 'M', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('AP_INVOICE', 'HOLD', 'On hold', 'One or more holds applied, payment blocked.', 'AP_LIFE', 30,
            'N', 'Y', 'Y', 'MATCHED,CANC',
            NULL, 'H', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('AP_INVOICE', 'APPR', 'Approved for payment', 'Cleared for the next payment run.', 'AP_LIFE', 40,
            'N', 'N', 'N', 'PAID,HOLD',
            NULL, 'A', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('AP_INVOICE', 'PAID', 'Paid', 'Fully applied by one or more payments.', 'AP_LIFE', 50,
            'Y', 'N', 'N', NULL,
            NULL, 'P', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('GL_PERIOD', 'OPEN', 'Open', 'Postings accepted.', 'GL_PERIOD', 10,
            'N', 'N', 'Y', 'CLSD',
            NULL, 'O', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('GL_PERIOD', 'CLSD', 'Closed', 'Closed, may be reopened by the controller.', 'GL_PERIOD', 20,
            'N', 'N', 'N', 'OPEN,PERM',
            NULL, 'C', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('GL_PERIOD', 'PERM', 'Permanently closed', 'Closed after statutory filing, never reopened.', 'GL_PERIOD', 30,
            'Y', 'N', 'N', NULL,
            NULL, 'Z', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('REQUISITION', 'SUBMIT', 'Submitted', 'Submitted to sourcing.', 'REQ_LIFE', 10,
            'N', 'N', 'N', 'SOURCED,REJ',
            NULL, 'S', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('REQUISITION', 'SOURCED', 'Sourced', 'Converted to a PO or a sourcing event.', 'REQ_LIFE', 20,
            'Y', 'N', 'N', NULL,
            NULL, 'G', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('VENDOR_CONTRACT', 'ACTIVE', 'Active', 'In force.', 'CTR_LIFE', 10,
            'N', 'N', 'Y', 'EXPIRED,TERM',
            NULL, 'A', 'Y')
    INTO WWI_REF.STATUS_CODE_REF
        (ENTITY_CD, STATUS_CD, STATUS_NAME, STATUS_DESC, STATUS_GROUP_CD, DISPLAY_SEQ_NBR,
         IS_TERMINAL_FLG, IS_ERROR_FLG, ALLOWS_UPDATE_FLG, NEXT_STATUS_LIST_TXT,
         REGION_CD, LEGACY_STATUS_CD, ACTIVE_FLG)
    VALUES ('VENDOR_CONTRACT', 'EXPIRED', 'Expired', 'Past the end date, EU contracts auto-renew instead.', 'CTR_LIFE', 20,
            'Y', 'N', 'N', NULL,
            'EU', 'E', 'Y')
SELECT * FROM DUAL
/

INSERT ALL
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'DMG', 'NA', 'Damaged in transit', 'Visible damage recorded at the dock.', 'QUALITY',
            'Y', 'N', NULL, 'Y',
            'Y', 'N', 10, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'QTY', 'NA', 'Quantity over shipment', 'More received than ordered beyond tolerance.', 'LOGISTIC',
            'Y', 'Y', 1, 'Y',
            'Y', 'N', 20, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'WRNG', 'NA', 'Wrong item shipped', 'Item does not match the PO line.', 'LOGISTIC',
            'Y', 'N', NULL, 'Y',
            'Y', 'N', 30, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'SPEC', 'NA', 'Fails specification', 'Rejected at incoming inspection.', 'QUALITY',
            'Y', 'Y', 2, 'Y',
            'Y', 'N', 40, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'RET14', 'EU', 'Withdrawal within 14 days', 'Statutory withdrawal, no supplier fault.', 'LEGAL',
            'N', 'N', NULL, 'Y',
            'N', 'Y', 10, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'CONF', 'EU', 'Non-conforming goods', 'Does not conform to the contract description.', 'QUALITY',
            'Y', 'Y', 1, 'Y',
            'Y', 'N', 20, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'CEMK', 'EU', 'Missing CE marking or documentation', 'Compliance documentation absent.', 'COMPLY',
            'Y', 'Y', 2, 'N',
            'Y', 'N', 30, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'R01', 'APAC', 'Return code 01 - damaged carton', 'Inherited numeric code from the SG distributor system.', 'QUALITY',
            'N', 'N', NULL, 'Y',
            'Y', 'Y', 10, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'R02', 'APAC', 'Return code 02 - short shipment', 'Inherited numeric code from the SG distributor system.', 'LOGISTIC',
            'N', 'N', NULL, 'Y',
            'Y', 'Y', 20, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('GOODS_RETURN', 'R07', 'APAC', 'Return code 07 - customer refusal', 'Refused at delivery, reason not recorded.', 'OTHER',
            'N', 'N', NULL, 'Y',
            'N', 'Y', 30, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('AP_HOLD', 'PRICE', 'ALL', 'Price variance beyond tolerance', 'Invoice unit price exceeds the PO price tolerance.', 'MATCH',
            'N', 'Y', 1, 'Y',
            'N', 'N', 10, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('AP_HOLD', 'QTY', 'ALL', 'Quantity billed exceeds received', 'Billed more than the receipt supports.', 'MATCH',
            'N', 'Y', 1, 'Y',
            'N', 'N', 20, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('AP_HOLD', 'TAX', 'EU', 'VAT number missing or invalid', 'Supplier VAT registration could not be resolved.', 'TAX',
            'Y', 'Y', 2, 'N',
            'N', 'N', 30, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('AP_HOLD', 'WHT', 'APAC', 'Withholding certificate not on file', 'Payment blocked until the certificate is lodged.', 'TAX',
            'Y', 'Y', 2, 'Y',
            'N', 'N', 40, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('AP_HOLD', 'DUP', 'ALL', 'Suspected duplicate invoice', 'Matches an existing supplier invoice number.', 'CONTROL',
            'Y', 'Y', 1, 'Y',
            'N', 'N', 50, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('JOURNAL_REVERSAL', 'ACCR', 'ALL', 'Accrual reversal', 'Standard period-end accrual reversed next period.', 'PERIOD',
            'N', 'N', NULL, 'Y',
            'N', 'Y', 10, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('JOURNAL_REVERSAL', 'ERR', 'ALL', 'Posting error', 'Corrects an incorrect posting.', 'CORRECT',
            'Y', 'Y', 2, 'Y',
            'N', 'N', 20, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('PO_CANCEL', 'BUDG', 'NA', 'Budget withdrawn', 'Cost centre budget pulled after approval.', 'FINANCE',
            'Y', 'Y', 2, 'Y',
            'N', 'Y', 10, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('PO_CANCEL', 'SUPP', 'ALL', 'Supplier unable to fulfil', 'Supplier declined or could not deliver.', 'SUPPLIER',
            'Y', 'N', NULL, 'N',
            'Y', 'N', 20, 'Y')
    INTO WWI_REF.REASON_CODE_REF
        (ENTITY_CD, REASON_CD, REGION_CD, REASON_NAME, REASON_DESC, REASON_CATEGORY_CD,
         REQUIRES_COMMENT_FLG, REQUIRES_APPROVAL_FLG, APPROVAL_LEVEL_NBR, FINANCIAL_IMPACT_FLG,
         SUPPLIER_FAULT_FLG, KPI_EXCLUDE_FLG, DISPLAY_SEQ_NBR, ACTIVE_FLG)
    VALUES ('PO_CANCEL', 'DUPE', 'ALL', 'Duplicate order', 'Ordered twice, one cancelled.', 'CONTROL',
            'N', 'N', NULL, 'N',
            'N', 'Y', 30, 'Y')
SELECT * FROM DUAL
/

COMMIT
/

SET DEFINE ON
