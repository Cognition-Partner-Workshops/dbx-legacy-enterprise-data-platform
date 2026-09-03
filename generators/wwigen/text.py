"""Fictional name, address and product-word corpora, plus the variant machinery.

All names are invented. The corpora are small on purpose: repetition across a
large customer base is exactly what produces the collisions and near-matches
that the match/merge and dedup logic exists to resolve.

``name_variant`` is the important function here. It produces the kind of
difference a twenty-year-old estate accumulates when the same company was
keyed three times by three different clerks: a suffix dropped, an ampersand
spelled out, an extra full stop, a transposition, a trailing branch marker.
"""

from __future__ import annotations

from . import rng

COMPANY_HEADS = (
    "Aldbrook", "Marrowfield", "Kestrel", "Penhallow", "Vantry", "Oakcombe",
    "Halbrand", "Torrent", "Ferrowick", "Lindmere", "Quillhaven", "Brackwell",
    "Sorrelton", "Dunmarch", "Everly", "Northgale", "Saltbridge", "Wrenfield",
    "Calderon", "Merristowe", "Ashvale", "Cobbett", "Draymoor", "Fennimore",
    "Glaslyn", "Hartsmere", "Ilverston", "Jarrowdale", "Kingsmoor", "Lowbridge",
)

COMPANY_TAILS = (
    "Trading", "Supply", "Distribution", "Provisions", "Wholesale", "Imports",
    "Exports", "Logistics", "Merchants", "Foods", "Novelties", "Group",
    "Retail", "Partners", "Holdings", "Sourcing",
)

COMPANY_SUFFIX = {
    "NA": ("Inc.", "LLC", "Corp.", "Co."),
    "EU": ("Ltd", "GmbH", "S.A.R.L.", "B.V.", "PLC"),
    "APAC": ("Pty Ltd", "Pte Ltd", "K.K.", "Ltd"),
}

GIVEN_NAMES = (
    "Alina", "Bertrand", "Corine", "Desmond", "Elke", "Farid", "Greta", "Hideo",
    "Imani", "Joris", "Kirra", "Lorcan", "Mireille", "Nikolai", "Orla", "Piers",
    "Quenby", "Rosalind", "Soren", "Tamsin", "Ulf", "Verity", "Wendell", "Xanthe",
    "Yusuf", "Zora",
)

FAMILY_NAMES = (
    "Achterberg", "Balfour", "Castellan", "Dunmore", "Eskildsen", "Fontaine",
    "Grimsby", "Halvorsen", "Ibarra", "Jelinek", "Kowalczyk", "Lindqvist",
    "Marchetti", "Nakagawa", "Oyelaran", "Pettigrew", "Quintero", "Rasmussen",
    "Stavros", "Thorogood", "Ueda", "Vasquez", "Whitcombe", "Yamashita",
)

STREETS = (
    "Cobbler", "Harrow", "Tannery", "Millrace", "Foundry", "Kiln", "Lantern",
    "Quarry", "Saltwater", "Threshing", "Vinegar", "Windlass", "Copperbeech",
    "Drovers", "Elmshaw", "Ferryman", "Granary", "Hollybush",
)

STREET_TYPES = {
    "us": ("St", "Ave", "Blvd", "Rd", "Way", "Pkwy"),
    "eu": ("straat", "gasse", "rue", "lane", "walk"),
    "apac": ("St", "Rd", "Cres", "Pde", "Tce"),
}

LOCALITIES = {
    "NA": ("Fairhaven", "Kingsport", "Redbank", "Ashford Falls", "Milbury",
           "Cedar Junction", "Portsmith", "Grand Rapids Heights"),
    "EU": ("Alderstadt", "Vieux-Pont", "Kirkhaven", "Bergendaal", "Saint-Elme",
           "Nordhafen", "Ballyveagh", "Oosterwijk"),
    "APAC": ("Wattle Bay", "Kurrajong", "Port Meridian", "Tanjong Ridge",
             "Higashi Nakamura", "Coral Flats", "Te Awamaru North", "Kembla Vale"),
}

SUBDIVISIONS = {
    "NA": ("IL", "TX", "NJ", "ON", "CA", "OH", "GA", "QC"),
    "EU": ("BW", "IDF", "NH", "LEI", "MUN", "DUB", "NRW", "ZH"),
    "APAC": ("NSW", "VIC", "QLD", "WA", "SG", "OSK", "AKL", "TKY"),
}

PRODUCT_ADJECTIVES = (
    "Novelty", "Vintage", "Miniature", "Oversized", "Reinforced", "Recycled",
    "Chilled", "Gift-boxed", "Bulk", "Refill", "Seasonal", "Premium",
    "Economy", "Heavy-duty", "Collapsible", "Stackable",
)

PRODUCT_NOUNS = (
    "Packing Tape", "Bubble Wrap", "Shipping Carton", "Postal Tube", "USB Cable",
    "Chef Hat", "Animal Mug", "Chocolate Frog", "Ride-on Toy", "Desk Lamp",
    "Pen Set", "Sticky Notes", "Table Cloth", "Ceramic Bowl", "Steel Flask",
    "Coin Purse", "Tote Bag", "Umbrella", "Photo Frame", "Snow Globe",
)

PRODUCT_COLOURS = ("Red", "Blue", "Green", "Black", "White", "Amber", "Slate", "Ivory")
PRODUCT_SIZES = ("XS", "S", "M", "L", "XL", "10 pack", "50 pack", "200 pack")

CATEGORY_CODES = (
    "PKG", "TOY", "CONF", "APP", "HOME", "STAT", "TEXT", "ELEC", "GIFT", "SEAS",
)


def company_name(seed: int, ordinal: int, region: str) -> str:
    head = rng.pick(seed, COMPANY_HEADS, "co-head", ordinal)
    tail = rng.pick(seed, COMPANY_TAILS, "co-tail", ordinal)
    suffix = rng.pick(seed, COMPANY_SUFFIX[region], "co-suffix", ordinal)
    if rng.chance(seed, 0.18, "co-amp", ordinal):
        second = rng.pick(seed, COMPANY_HEADS, "co-head2", ordinal)
        return "%s & %s %s %s" % (head, second, tail, suffix)
    return "%s %s %s" % (head, tail, suffix)


def person_name(seed: int, ordinal: int) -> tuple:
    return (rng.pick(seed, GIVEN_NAMES, "given", ordinal),
            rng.pick(seed, FAMILY_NAMES, "family", ordinal))


def product_name(seed: int, ordinal: int) -> str:
    return "%s %s (%s %s)" % (
        rng.pick(seed, PRODUCT_ADJECTIVES, "prod-adj", ordinal),
        rng.pick(seed, PRODUCT_NOUNS, "prod-noun", ordinal),
        rng.pick(seed, PRODUCT_COLOURS, "prod-col", ordinal),
        rng.pick(seed, PRODUCT_SIZES, "prod-size", ordinal),
    )


_SUFFIX_WORDS = ("Inc.", "LLC", "Corp.", "Co.", "Ltd", "GmbH", "PLC", "B.V.",
                 "Pty Ltd", "Pte Ltd", "K.K.", "S.A.R.L.")

_VARIANT_KINDS = (
    "drop_suffix", "spell_ampersand", "upper", "no_punct", "abbreviate",
    "trailing_branch", "double_space", "transpose", "trailing_dot",
)


def name_variant(seed: int, name: str, ordinal: int, attempt: int = 0) -> str:
    """A plausible clerical variant of ``name`` for near-duplicate seeding."""
    kind = rng.pick(seed, _VARIANT_KINDS, "variant-kind", ordinal, attempt)
    if kind == "drop_suffix":
        for suffix in _SUFFIX_WORDS:
            if name.endswith(" " + suffix):
                return name[: -(len(suffix) + 1)]
        return name.rstrip(".")
    if kind == "spell_ampersand":
        return name.replace(" & ", " and ")
    if kind == "upper":
        return name.upper()
    if kind == "no_punct":
        return name.replace(".", "").replace(",", "")
    if kind == "abbreviate":
        parts = name.split(" ")
        if len(parts) > 2:
            parts[1] = parts[1][:4] + "."
        return " ".join(parts)
    if kind == "trailing_branch":
        branch = rng.pick(seed, ("- Branch 2", "(Head Office)", "- DC", "- Acct 2"),
                          "variant-branch", ordinal, attempt)
        return "%s %s" % (name, branch)
    if kind == "double_space":
        return name.replace(" ", "  ", 1)
    if kind == "transpose":
        if len(name) > 6:
            cut = 3 + rng.stable_hash(seed, "variant-cut", ordinal, attempt) % (len(name) - 5)
            return name[:cut] + name[cut + 1] + name[cut] + name[cut + 2:]
        return name
    return name + "."


def address_variant(seed: int, line: str, ordinal: int) -> str:
    """Street-level variants: abbreviation swaps and unit-number drift."""
    swaps = (("Street", "St"), ("St", "Street"), ("Avenue", "Ave"), ("Ave", "Avenue"),
             ("Road", "Rd"), ("Rd", "Road"), ("Suite", "Ste"), ("Level", "L"))
    old, new = rng.pick(seed, swaps, "addr-variant", ordinal)
    if old in line:
        return line.replace(old, new, 1)
    return line + (" Unit %d" % (1 + rng.stable_hash(seed, "addr-unit", ordinal) % 40))


def email_for(first: str, last: str, company: str, ordinal: int) -> str:
    domain = "".join(ch for ch in company.split(" ")[0].lower() if ch.isalpha())
    return "%s.%s%d@%s.example" % (first.lower(), last.lower(), ordinal % 100, domain or "wwi")
