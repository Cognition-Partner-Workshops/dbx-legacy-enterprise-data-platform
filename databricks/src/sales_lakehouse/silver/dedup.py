"""Order-line deduplication - port of ``stg.usp_DeduplicateOrderLine``.

Legacy: sqlserver/staging/procedures/stg.usp_DeduplicateOrderLine.sql.
Two kinds of duplicate are handled in the legacy order:

1. Exact re-extraction copies - same ``OrderLineBusinessKey`` landed twice
   (lines 375-391): rows are ranked by ``StagingOrderLineId DESC`` and only the
   most recently staged copy survives.
2. Genuine re-keys - same order, stock item, quantity and unit price under a
   different line id (lines 393-467): rows share a ``DuplicateGroupId``, the
   highest ``LineNumber`` (then latest staging id) wins, and the losers stay in
   ``stg.OrderLine`` tagged ``DqStatusCode = 'WARN'`` while also being written
   to ``err.RejectedOrderLine`` with ``RejectReasonCode = 'DUPLICATE_LINE'``.

In the lakehouse the staging identity is replaced by the bronze load timestamp
and the source edit stamp; the losers are quarantined under ``DUP_ORDER_LINE``
and, for the re-key case, also kept in ``silver.order_line`` as WARN rows.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DUP_RULE_CODE = "DUP_ORDER_LINE"

EXACT_COPY_RANK_COL = "_exact_copy_rank"
REKEY_RANK_COL = "_rekey_rank"
REKEY_GROUP_SIZE_COL = "_rekey_group_size"


def rankExactCopies(
    df: DataFrame,
    keyCol: str = "order_line_business_key",
    recencyCols: tuple[str, ...] = ("source_modified_at_utc", "_load_ts"),
) -> DataFrame:
    """Rank identical business keys, 1 = the copy that survives.

    Legacy lines 375-391 keep ``MAX(StagingOrderLineId)``; the lakehouse
    equivalent is the latest source edit stamp, then the latest bronze load.
    """
    window = Window.partitionBy(keyCol).orderBy(*[F.col(c).desc_nulls_last() for c in recencyCols])
    return df.withColumn(EXACT_COPY_RANK_COL, F.row_number().over(window))


def flagRekeyDuplicates(
    df: DataFrame,
    groupCols: tuple[str, ...] = (
        "order_business_key",
        "stock_item_business_key",
        "ordered_quantity",
        "unit_price_amount_local",
    ),
    lineNumberCol: str = "line_number",
    recencyCols: tuple[str, ...] = ("source_modified_at_utc", "_load_ts"),
) -> DataFrame:
    """Add ``duplicate_group_id`` / ``is_duplicate_loser`` for genuine re-keys.

    Legacy lines 393-430: candidate groups are ``(OrderBusinessKey,
    StockItemBusinessKey, OrderedQuantity, UnitPriceAmount)`` with more than
    one row; within a group the survivor is ``LineNumber DESC, StagingOrderLineId DESC``.
    """
    groupWindow = Window.partitionBy(*groupCols)
    rankWindow = groupWindow.orderBy(
        F.col(lineNumberCol).desc_nulls_last(), *[F.col(c).desc_nulls_last() for c in recencyCols]
    )
    groupId = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in groupCols]), 256)
    out = (
        df.withColumn(REKEY_GROUP_SIZE_COL, F.count(F.lit(1)).over(groupWindow))
        .withColumn(REKEY_RANK_COL, F.row_number().over(rankWindow))
        .withColumn(
            "duplicate_group_id",
            F.when(F.col(REKEY_GROUP_SIZE_COL) > 1, F.substring(groupId, 1, 32)).otherwise(F.lit(None).cast("string")),
        )
        # LEGACY QUIRK: losers are not deleted - they stay in the conformed table
        # as WARN rows so the fact load can see them (lines 431-467).
        .withColumn("is_duplicate_loser", (F.col(REKEY_GROUP_SIZE_COL) > 1) & (F.col(REKEY_RANK_COL) > 1))
    )
    return out.drop(REKEY_GROUP_SIZE_COL, REKEY_RANK_COL)
