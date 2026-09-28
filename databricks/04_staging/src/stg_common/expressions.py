"""Column expressions mirroring the SSIS derived-column idioms and stg.ufn_*."""

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

EPOCH_TIMESTAMP = "1900-01-01"
END_OF_TIME_DATE = "9999-12-31"
NULL_TOKENS = ("NULL", "N/A", "NA", "?", "-", "UNKNOWN", ".")
REGION_CODES = ("NA", "EU", "APAC", "ROW")
DATE_SENTINELS = ("4712-12-31", "31-DEC-4712", "9999-12-31", "1900-01-01")


def trimUpper(col):
    """SSIS UPPER(TRIM(x))."""
    return F.upper(F.trim(_c(col)))


def trimCol(col):
    return F.trim(_c(col))


def nullIfBlank(col):
    """T-SQL NULLIF(LTRIM(RTRIM(x)), '')."""
    trimmed = F.trim(_c(col))
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)


def isBlank(col):
    """SSIS ISNULL(x) || TRIM(x) == ''."""
    c = _c(col)
    return c.isNull() | (F.trim(c) == "")


def defaultIfNull(col, default):
    """SSIS ISNULL(x) ? default : x."""
    return F.coalesce(_c(col), F.lit(default))


def defaultIfBlank(col, default):
    """SSIS ISNULL(x) || TRIM(x) == '' ? default : x (trimmed, upper-cased)."""
    c = _c(col)
    return F.when(isBlank(c), F.lit(default)).otherwise(F.upper(F.trim(c)))


def collapseSpaces(col):
    """SSIS TRIM(REPLACE(x, '  ', ' ')) - one pass, exactly like the package."""
    return F.trim(F.regexp_replace(_c(col), "  ", " "))


def cleanString(col, upper=False):
    """Port of stg.ufn_CleanString: control chars to spaces, collapse runs of
    whitespace, trim, and treat the estate's textual null tokens as NULL."""
    c = _c(col)
    c = F.regexp_replace(c, "[\\t\\n\\r\\u00A0]", " ")
    c = F.regexp_replace(c, "\\u0000", "")
    c = F.trim(F.regexp_replace(c, " {2,}", " "))
    c = F.when(c == "", F.lit(None)).when(F.upper(c).isin(*NULL_TOKENS), F.lit(None)).otherwise(c)
    return F.upper(c) if upper else c


def sourceSystemKey(sourceSystem, naturalKey, collapseRegionalInstances=True):
    """Port of stg.ufn_SourceSystemKey: '<SourceSystemCode>|<natural key>'."""
    system = F.upper(F.trim(F.coalesce(_c(sourceSystem), F.lit(""))))
    key = F.upper(F.trim(F.coalesce(_c(naturalKey), F.lit(""))))
    if collapseRegionalInstances:
        system = (
            F.when(system.isin("ORA_ERP_NA", "ORA_ERP_EU", "ORA_ERP_AP"), F.lit("ORA_ERP"))
            .when(system == "WWI_WEB", F.lit("WWI_OLTP"))
            .otherwise(system)
        )
    key = F.when((system == "ORA_ERP") & key.rlike("^[0-9]+$"), F.lpad(key, 10, "0")).otherwise(key)
    key = F.regexp_replace(key, "\\|", "/")
    return F.when((key == "") | (system == ""), F.lit(None).cast("string")).otherwise(
        F.concat(system, F.lit("|"), key)
    )


def safeDecimal(col, decimalSeparator=".", precision=19, scale=6):
    """Port of stg.ufn_SafeDecimal. Unparseable values yield NULL.

    Sign markers are detected with one regex on the original text and stripped
    with a linear chain of regexp_replace calls (each step references the
    previous one once) so the generated code stays small."""
    text = F.upper(F.trim(F.coalesce(_c(col).cast("string"), F.lit(""))))
    isNegative = text.rlike("^\\(.*\\)$|CR$|-$|^-")
    stripped = F.regexp_replace(text, "^\\((.*)\\)$", "$1")
    stripped = F.regexp_replace(stripped, "(CR|DR)$", "")
    stripped = F.regexp_replace(stripped, "^-|-$", "")
    stripped = F.regexp_replace(stripped, "[ \\u00A0+$%]", "")
    if decimalSeparator == ",":
        stripped = F.regexp_replace(F.regexp_replace(stripped, "\\.", ""), ",", ".")
    else:
        stripped = F.regexp_replace(stripped, ",", "")
    parsed = F.when(stripped.rlike("^[0-9]*\\.?[0-9]+$|^[0-9]+\\.?$"), stripped.cast(T.DecimalType(precision, scale)))
    return F.when(isNegative, -parsed).otherwise(parsed)


def safeDecimalBySeparator(col, separatorCol):
    """stg.ufn_SafeDecimal driven by ref.Region.DecimalSeparator per row."""
    sep = F.coalesce(_c(separatorCol), F.lit("."))
    return F.when(sep == ",", safeDecimal(col, ",")).otherwise(safeDecimal(col, "."))


def safeDate(col, regionCode):
    """Port of stg.ufn_SafeDate: region decides DD/MM vs MM/DD; sentinels are NULL."""
    raw = _c(col).cast("string")
    text = F.trim(F.coalesce(raw, F.lit("")))
    region = F.lit(regionCode) if isinstance(regionCode, str) and regionCode in REGION_CODES else _c(regionCode)
    isNullToken = (text == "") | F.upper(text).isin("NULL", "N/A", "0", "00000000") | text.isin(*DATE_SENTINELS)
    norm = F.regexp_replace(F.regexp_replace(text, "\\.", "/"), "-", "/")
    compact = F.when(norm.rlike("^[0-9]{8}$"), F.to_timestamp(norm, "yyyyMMdd"))
    regional = (
        F.when(region == "NA", _tryTs(norm, "M/d/yyyy"))
        .when(region == "EU", _tryTs(norm, "d/M/yyyy"))
        .when(region == "APAC", _tryTs(norm, "yyyy/M/d"))
        .otherwise(_tryTs(norm, "yyyy/M/d H:m:s"))
    )
    fallbacks = F.coalesce(
        compact,
        regional,
        _tryTs(norm, "yyyy/M/d H:m:s"),
        _tryTs(norm, "yyyy/M/d"),
        _tryTs(text, "dd MMM yyyy"),
        _tryTs(text, "yyyy-MM-dd'T'HH:mm:ss"),
        _tryTs(text, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(F.when(text.rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}"), text)),
    )
    result = F.when(isNullToken, F.lit(None).cast("timestamp")).otherwise(fallbacks)
    outOfRange = (result < F.lit("1980-01-01").cast("timestamp")) | (
        result > F.add_months(F.current_timestamp(), 60)
    )
    return F.when(outOfRange, F.lit(None).cast("timestamp")).otherwise(result)


def standardizePostalCode(postal, country):
    """Port of stg.ufn_StandardizePostalCode for the countries the estate ships."""
    text = F.upper(F.trim(F.coalesce(_c(postal), F.lit(""))))
    text = F.regexp_replace(F.regexp_replace(F.regexp_replace(text, "\\.", ""), "  ", " "), "\\u00A0", "")
    digits = F.regexp_replace(text, "[^0-9]", "")
    nospace = F.regexp_replace(text, " ", "")
    cc = F.upper(F.trim(_c(country)))
    empty = (text == "") | text.isin("NULL", "N/A", "00000", "0", "-")
    dlen = F.length(digits)
    nlen = F.length(nospace)
    return F.when(empty, F.lit(None).cast("string")).otherwise(
        F.when(cc == "US", F.when(dlen >= 5, F.substring(digits, 1, 5)))
        .when(cc == "CA", F.when(nlen == 6, F.concat(F.substring(nospace, 1, 3), F.lit(" "), F.substring(nospace, 4, 3))))
        .when(cc == "GB", F.when(nlen.between(5, 7), F.concat(nospace.substr(F.lit(1), F.length(nospace) - 3), F.lit(" "), F.substring(nospace, -3, 3))))
        .when(cc == "NL", F.when(nlen == 6, F.concat(F.substring(nospace, 1, 4), F.lit(" "), F.substring(nospace, 5, 2))))
        .when(cc == "PL", F.when(dlen == 5, F.concat(F.substring(digits, 1, 2), F.lit("-"), F.substring(digits, 3, 3))))
        .when(cc.isin("DE", "FR", "ES", "IT", "FI"), F.when(dlen == 5, digits).when(dlen == 4, F.concat(F.lit("0"), digits)))
        .when(cc == "JP", F.when(dlen == 7, F.concat(F.substring(digits, 1, 3), F.lit("-"), F.substring(digits, 4, 4))))
        .when(cc.isin("AU", "NZ"), F.when(dlen == 4, digits))
        .when(cc == "SG", F.when(dlen == 6, digits))
        .otherwise(F.when(text.rlike("^[A-Z ]*$") & (dlen == 0), F.when(text == "", F.lit(None)).otherwise(text)).otherwise(F.when(dlen > 0, digits)))
    )


def changeHash(*cols):
    """The house SSIS change hash: UPPER(TRIM((DT_WSTR,60)[col])) joined by '|'.
    NULL propagates exactly like the SSIS string concatenation."""
    parts = []
    for col in cols:
        parts.append(F.upper(F.trim(F.substring(_c(col).cast("string"), 1, 60))))
    out = parts[0]
    for part in parts[1:]:
        out = F.concat(out, F.lit("|"), part)
    return out


def rowHash(*cols):
    """SHA2_256 over the pipe-delimited values (T-SQL HASHBYTES in the procedures)."""
    return F.sha2(F.concat_ws("|", *[F.coalesce(_c(c).cast("string"), F.lit("")) for c in cols]), 256)


def regionCase(regionCol, mapping, default):
    """CASE RegionCode WHEN ... END with a default branch."""
    expr = None
    for code, value in mapping.items():
        cond = _c(regionCol) == code
        expr = F.when(cond, F.lit(value)) if expr is None else expr.when(cond, F.lit(value))
    return expr.otherwise(F.lit(default))


def jsonPayload(*cols):
    """RecordPayload: the source columns as a JSON document."""
    return F.to_json(F.struct(*[_c(c) for c in cols]))


def _c(col):
    return F.col(col) if isinstance(col, str) else col


def _tryTs(col, fmt):
    return F.to_timestamp(F.when(col.rlike(_fmtRegex(fmt)), col), fmt)


def _fmtRegex(fmt):
    """A coarse guard so to_timestamp never sees a value it cannot parse
    (Spark would otherwise raise instead of returning NULL in ANSI mode)."""
    if fmt == "M/d/yyyy" or fmt == "d/M/yyyy":
        return "^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$"
    if fmt == "yyyy/M/d":
        return "^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$"
    if fmt == "yyyy/M/d H:m:s":
        return "^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}"
    if fmt == "dd MMM yyyy":
        return "^[0-9]{2} [A-Za-z]{3} [0-9]{4}$"
    if fmt == "yyyy-MM-dd'T'HH:mm:ss":
        return "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    if fmt == "yyyy-MM-dd HH:mm:ss.SSS":
        return "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}"
    return ".*"
