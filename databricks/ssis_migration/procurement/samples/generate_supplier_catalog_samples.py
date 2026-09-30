"""Generates the representative supplier_catalog_*.psv landing files committed under samples/.
Layout follows config/landing-zone.yaml + ssis/03_file_ingestion (HDR / DTL / TRL, pipe separated,
ISO-8859-1, TRL carries row count and SUM(NetPrice*100) % 1000000)."""

import os
import random
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "supplier_catalog")

SUPPLIERS = ["SUP004000", "SUP004002", "SUP004005", "SUP004010", "SUP004017", "SUP004036"]
UOMS = ["EA", "BOX", "CTN", "KG"]
CURRENCIES = {"SUP004000": "USD", "SUP004002": "SGD", "SUP004005": "USD", "SUP004010": "EUR", "SUP004017": "GBP", "SUP004036": "AUD"}


def detailRows(rng, count, supplierPool):
    rows = []
    for i in range(count):
        supplier = rng.choice(supplierPool)
        net = Decimal(rng.randint(120, 90000)) / 100
        listPrice = (net * Decimal(rng.choice(["1.05", "1.10", "1.25"]))).quantize(Decimal("0.01"))
        hazard = rng.choice(["", "", "", "UN3077", "UN1263"])
        rows.append([
            "DTL", supplier, f"{supplier[-4:]}-ITM-{i + 1:04d}", f"MPN{rng.randint(10000, 99999)}",
            f"Catalogue item {i + 1} für {supplier}", rng.choice(UOMS), str(rng.choice([1, 6, 12, 24])),
            f"{listPrice}", f"{net}", CURRENCIES[supplier], str(rng.choice([1, 5, 10])), str(rng.randint(2, 45)),
            "20240101", rng.choice(["", "20241231", "20251231"]), hazard,
        ])
    return rows


def checksum(rows):
    return sum(int(Decimal(r[8]) * 100) for r in rows) % 1000000


def writeFile(name, rows, rowCount=None, footerChecksum=None):
    rowCount = len(rows) if rowCount is None else rowCount
    footerChecksum = checksum(rows) if footerChecksum is None else footerChecksum
    lines = ["|".join(["HDR", "supplier_catalog", name[17:25], "1", "", "", "", "", "", "", "", "", "", "", ""])]
    lines += ["|".join(r) for r in rows]
    lines.append("|".join(["TRL", str(rowCount), str(footerChecksum), "", "", "", "", "", "", "", "", "", "", "", ""]))
    with open(os.path.join(OUT, name), "w", encoding="iso-8859-1", newline="\n") as fh:
        fh.write("\n".join(lines) + "\n")


def main():
    os.makedirs(OUT, exist_ok=True)
    rng = random.Random(20260930)
    # 1. clean quarterly price list
    clean = detailRows(rng, 40, SUPPLIERS[:4])
    writeFile("supplier_catalog_20260101_001.psv", clean)
    # 2. price list with two malformed rows (missing item code, list < net); footer counts only valid rows
    second = detailRows(rng, 25, SUPPLIERS[2:])
    bad1 = list(second[3]); bad1[2] = ""
    bad2 = list(second[7]); bad2[7] = "1.00"
    withBad = second + [bad1, bad2]
    writeFile("supplier_catalog_20260401_001.psv", withBad, rowCount=len(second), footerChecksum=checksum(second))
    # 3. footer checksum mismatch -> quarantined
    third = detailRows(rng, 12, SUPPLIERS[:2])
    writeFile("supplier_catalog_20260401_002.psv", third, footerChecksum=(checksum(third) + 1) % 1000000)


if __name__ == "__main__":
    main()
