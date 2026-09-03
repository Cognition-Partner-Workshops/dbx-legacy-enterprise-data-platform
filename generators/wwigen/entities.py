"""Virtual master-data entities, derived on demand from an ordinal.

Nothing about a customer, supplier, product or employee is stored. Every
attribute is a pure function of the run seed and the entity's ordinal, so any
table generator - in any order, in a separate process, resuming a half-finished
run - recomputes exactly the same entity. That is what makes the suite both
referentially coherent across the ERP and the OLTP and bounded in memory at
the ``large`` scale.

Entities carry history, not just a current state: address moves, category
reassignments, price and cost changes and salesperson reassignments are
returned as effective-dated lists so the SCD Type 2 dimension loads have real
history to build, and so a transaction dated 2019 resolves to the attributes
that were true in 2019.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from functools import lru_cache

from . import keys, regions, rng, skew, text

CUSTOMER = "customer"
SUPPLIER = "supplier"
PRODUCT = "product"
EMPLOYEE = "employee"


@dataclass(frozen=True)
class Address:
    valid_from: datetime.date
    line1: str
    line2: str
    locality: str
    subdivision: str
    postal_code: str
    country_code: str


@dataclass(frozen=True)
class Customer:
    ordinal: int
    erp_code: str
    oltp_id: int
    partner_code: str
    region: str
    country: regions.CountryProfile
    name: str
    trading_name: str
    class_code: str
    status_code: str
    opened_date: datetime.date
    credit_limit: float
    payment_terms_days: int
    tax_registered: bool
    tax_registration: str
    consent_code: str
    marketing_flag: str
    retention_months: int
    buying_group: str
    addresses: tuple
    class_history: tuple
    salesperson_history: tuple
    crosswalk: str
    duplicate_of: int
    is_dominant: bool

    def address_on(self, when: datetime.date) -> Address:
        chosen = self.addresses[0]
        for address in self.addresses:
            if address.valid_from <= when:
                chosen = address
            else:
                break
        return chosen

    def class_on(self, when: datetime.date) -> str:
        chosen = self.class_history[0][1]
        for valid_from, value in self.class_history:
            if valid_from <= when:
                chosen = value
            else:
                break
        return chosen

    def salesperson_on(self, when: datetime.date) -> int:
        chosen = self.salesperson_history[0][1]
        for valid_from, value in self.salesperson_history:
            if valid_from <= when:
                chosen = value
            else:
                break
        return chosen


@dataclass(frozen=True)
class Supplier:
    ordinal: int
    erp_code: str
    oltp_id: int
    region: str
    country: regions.CountryProfile
    name: str
    status_code: str
    onboarded_date: datetime.date
    payment_terms_days: int
    payment_method: str
    tax_registration: str
    bank_account_masked: str
    certification_code: str
    scorecard_band: str
    addresses: tuple
    crosswalk: str
    is_dominant: bool


@dataclass(frozen=True)
class Product:
    ordinal: int
    erp_code: str
    oltp_id: int
    name: str
    category_code: str
    category_history: tuple
    uom: str
    pack_size: int
    is_chiller: bool
    lead_time_days: int
    supplier_ordinal: int
    price_history: tuple
    discontinued_on: datetime.date
    crosswalk: str
    is_dominant: bool

    def category_on(self, when: datetime.date) -> str:
        chosen = self.category_history[0][1]
        for valid_from, value in self.category_history:
            if valid_from <= when:
                chosen = value
            else:
                break
        return chosen

    def price_on(self, when: datetime.date) -> tuple:
        chosen = self.price_history[0]
        for entry in self.price_history:
            if entry[0] <= when:
                chosen = entry
            else:
                break
        return (chosen[1], chosen[2])


@dataclass(frozen=True)
class Employee:
    ordinal: int
    oltp_id: int
    full_name: str
    region: str
    role_code: str
    territory_code: str
    hired_date: datetime.date
    is_salesperson: bool
    commission_plan: str


def region_for(seed: int, kind: str, ordinal: int, region_mix: dict) -> str:
    names = tuple(sorted(region_mix))
    weights = tuple(region_mix[name] for name in names)
    return rng.weighted_pick(seed, names, weights, "region", kind, ordinal)


def _addresses(seed: int, kind: str, ordinal: int, region: str,
               country: regions.CountryProfile, opened: datetime.date,
               end: datetime.date) -> tuple:
    move_dates = [opened]
    from . import timeline
    move_dates.extend(timeline.attribute_change_dates(seed, kind + "-addr", ordinal,
                                                      opened, end, 3))
    built = []
    for index, valid_from in enumerate(move_dates):
        draw = rng.stable_hash(seed, "addr", kind, ordinal, index)
        street = rng.pick(seed, text.STREETS, "street", kind, ordinal, index)
        street_type = rng.pick(seed, text.STREET_TYPES[country.address_style],
                               "street-type", kind, ordinal, index)
        locality = rng.pick(seed, text.LOCALITIES[region], "locality", kind, ordinal, index)
        subdivision = rng.pick(seed, text.SUBDIVISIONS[region], "subdiv", kind, ordinal, index)
        postal = regions.postal_code(country.postal_style, draw)
        line1, line2 = regions.format_address_lines(
            country.address_style, 1 + draw % 9800,
            "%s %s" % (street, street_type), locality, subdivision, postal)
        built.append(Address(valid_from, line1, line2, locality, subdivision,
                             postal, country.code))
    return tuple(built)


@lru_cache(maxsize=8192)
def _customer_cached(seed: int, ordinal: int, population: int, mix_key: tuple,
                     start_ordinal: int, end_ordinal: int) -> Customer:
    start = datetime.date.fromordinal(start_ordinal)
    end = datetime.date.fromordinal(end_ordinal)
    region_mix = dict(mix_key)
    region = region_for(seed, CUSTOMER, ordinal, region_mix)
    country = rng.pick(seed, regions.countries(region), "country", CUSTOMER, ordinal)
    name = text.company_name(seed, ordinal, region)
    dominant = skew.is_dominant(population, ordinal)

    opened_offset = rng.stable_hash(seed, "opened", ordinal) % max((end - start).days - 120, 1)
    opened = start + datetime.timedelta(days=opened_offset)
    if dominant:
        # House accounts predate the history window; they were migrated in.
        opened = start

    consent = regions.CONSENT_MODEL[region]
    class_codes = regions.CUSTOMER_CLASS_CODES[region]
    class_history = [(opened, rng.weighted_pick(seed, class_codes, (0.18, 0.38, 0.36, 0.08),
                                                "class", ordinal))]
    from . import timeline
    for index, changed in enumerate(timeline.attribute_change_dates(seed, "cust-class", ordinal,
                                                                    opened, end, 2)):
        class_history.append((changed, rng.pick(seed, class_codes, "class-chg", ordinal, index)))

    salesperson_history = [(opened, rng.stable_hash(seed, "sp", ordinal) % 97)]
    for index, changed in enumerate(timeline.attribute_change_dates(seed, "cust-sp", ordinal,
                                                                    opened, end, 3)):
        salesperson_history.append((changed, rng.stable_hash(seed, "sp-chg", ordinal, index) % 97))

    tax_registered = region == "EU" and rng.chance(seed, 0.62, "vatreg", ordinal)
    tax_registration = ""
    if region == "EU":
        tax_registration = "%s%09d" % (country.code, rng.stable_hash(seed, "vatno", ordinal) % 10 ** 9)
    elif region == "NA":
        tax_registration = "%02d-%07d" % (rng.stable_hash(seed, "ein", ordinal) % 100,
                                          rng.stable_hash(seed, "ein2", ordinal) % 10 ** 7)
    else:
        tax_registration = "%011d" % (rng.stable_hash(seed, "abn", ordinal) % 10 ** 11)

    duplicate_of = -1
    if ordinal > 20 and rng.chance(seed, 0.011, "dupflag", ordinal):
        duplicate_of = rng.stable_hash(seed, "dupsrc", ordinal) % (ordinal - 1)

    return Customer(
        ordinal=ordinal,
        erp_code=keys.erp_customer_code(ordinal),
        oltp_id=keys.oltp_customer_id(ordinal),
        partner_code=keys.partner_customer_code(ordinal, region),
        region=region,
        country=country,
        name=name,
        trading_name=text.name_variant(seed, name, ordinal) if rng.chance(seed, 0.22, "trade", ordinal) else name,
        class_code=class_history[-1][1],
        status_code=rng.weighted_pick(seed, regions.CUSTOMER_STATUS_CODES[region],
                                      (0.86, 0.07, 0.04, 0.03), "status", ordinal),
        opened_date=opened,
        credit_limit=float(500 * (1 + rng.stable_hash(seed, "credit", ordinal) % 400)
                           * (12 if dominant else 1)),
        payment_terms_days=rng.pick(seed, (7, 14, 30, 30, 45, 60, 90), "terms", ordinal),
        tax_registered=tax_registered,
        tax_registration=tax_registration,
        consent_code=rng.pick(seed, consent["consent_code_set"], "consent", ordinal),
        marketing_flag=consent["marketing_default"],
        retention_months=consent["retention_months"],
        buying_group=rng.pick(seed, ("", "", "", "BG-WHOLE", "BG-INDEP", "BG-CHAIN"),
                              "bgroup", ordinal),
        addresses=_addresses(seed, CUSTOMER, ordinal, region, country, opened, end),
        class_history=tuple(class_history),
        salesperson_history=tuple(salesperson_history),
        crosswalk=keys.CROSSWALK_CLEAN,
        duplicate_of=duplicate_of,
        is_dominant=dominant,
    )


def customer(cfg, ordinal: int) -> Customer:
    population = cfg.count("customers")
    base = _customer_cached(cfg.seed, ordinal, population,
                            tuple(sorted(cfg.region_mix.items())),
                            cfg.history_start.toordinal(), cfg.history_end.toordinal())
    state = keys.crosswalk_state(cfg.seed, CUSTOMER, ordinal, cfg.defect("crosswalk_mismatch_rate"))
    if state == base.crosswalk:
        return base
    return Customer(**{**base.__dict__, "crosswalk": state})


@lru_cache(maxsize=4096)
def _supplier_cached(seed: int, ordinal: int, population: int, mix_key: tuple,
                     start_ordinal: int, end_ordinal: int) -> Supplier:
    start = datetime.date.fromordinal(start_ordinal)
    end = datetime.date.fromordinal(end_ordinal)
    region = region_for(seed, SUPPLIER, ordinal, dict(mix_key))
    country = rng.pick(seed, regions.countries(region), "country", SUPPLIER, ordinal)
    name = text.company_name(seed, ordinal + 90000, region)
    onboarded = start + datetime.timedelta(
        days=rng.stable_hash(seed, "onboard", ordinal) % max((end - start).days - 200, 1))
    account_digits = rng.stable_hash(seed, "bank", ordinal) % 10 ** 8
    return Supplier(
        ordinal=ordinal,
        erp_code=keys.erp_supplier_code(ordinal),
        oltp_id=keys.oltp_supplier_id(ordinal),
        region=region,
        country=country,
        name=name,
        status_code=rng.weighted_pick(seed, ("ACTIVE", "HOLD", "PENDING", "BLOCKED"),
                                      (0.88, 0.05, 0.04, 0.03), "sup-status", ordinal),
        onboarded_date=onboarded,
        payment_terms_days=rng.pick(seed, (14, 30, 45, 60, 60, 90), "sup-terms", ordinal),
        payment_method=rng.pick(seed, regions.PAYMENT_METHOD_CODES[region], "sup-pm", ordinal),
        tax_registration="%s%09d" % (country.code, rng.stable_hash(seed, "sup-tax", ordinal) % 10 ** 9),
        bank_account_masked="****%04d" % (account_digits % 10000),
        certification_code=rng.pick(seed, ("ISO9001", "ISO14001", "BRC", "HACCP", "NONE", "NONE"),
                                    "sup-cert", ordinal),
        scorecard_band=rng.weighted_pick(seed, ("A", "B", "C", "D"), (0.22, 0.44, 0.26, 0.08),
                                         "sup-band", ordinal),
        addresses=_addresses(seed, SUPPLIER, ordinal, region, country, onboarded, end),
        crosswalk=keys.CROSSWALK_CLEAN,
        is_dominant=skew.is_dominant(population, ordinal),
    )


def supplier(cfg, ordinal: int) -> Supplier:
    base = _supplier_cached(cfg.seed, ordinal, cfg.count("suppliers"),
                            tuple(sorted(cfg.region_mix.items())),
                            cfg.history_start.toordinal(), cfg.history_end.toordinal())
    state = keys.crosswalk_state(cfg.seed, SUPPLIER, ordinal, cfg.defect("crosswalk_mismatch_rate"))
    if state == base.crosswalk:
        return base
    return Supplier(**{**base.__dict__, "crosswalk": state})


@lru_cache(maxsize=16384)
def _product_cached(seed: int, ordinal: int, population: int, supplier_population: int,
                    start_ordinal: int, end_ordinal: int) -> Product:
    start = datetime.date.fromordinal(start_ordinal)
    end = datetime.date.fromordinal(end_ordinal)
    from . import timeline
    category = rng.pick(seed, text.CATEGORY_CODES, "cat", ordinal)
    category_history = [(start, category)]
    for index, changed in enumerate(timeline.attribute_change_dates(seed, "prod-cat", ordinal,
                                                                    start, end, 2)):
        category_history.append((changed, rng.pick(seed, text.CATEGORY_CODES,
                                                   "cat-chg", ordinal, index)))

    base_cost = 0.45 + (rng.stable_hash(seed, "cost", ordinal) % 46000) / 100.0
    margin = 1.18 + (rng.stable_hash(seed, "margin", ordinal) % 90) / 100.0
    price_history = [(start, round(base_cost * margin, 2), round(base_cost, 2))]
    for index, changed in enumerate(timeline.attribute_change_dates(seed, "prod-price", ordinal,
                                                                    start, end, 5)):
        uplift = 1.0 + (rng.stable_hash(seed, "uplift", ordinal, index) % 22 - 4) / 100.0
        previous_price, previous_cost = price_history[-1][1], price_history[-1][2]
        price_history.append((changed, round(previous_price * uplift, 2),
                              round(previous_cost * (1.0 + (uplift - 1.0) * 0.7), 2)))

    discontinued = None
    if rng.chance(seed, 0.06, "disc", ordinal):
        discontinued = start + datetime.timedelta(
            days=(end - start).days // 2 + rng.stable_hash(seed, "disc-at", ordinal) % max(
                (end - start).days // 2, 1))

    return Product(
        ordinal=ordinal,
        erp_code=keys.erp_product_code(ordinal),
        oltp_id=keys.oltp_stock_item_id(ordinal),
        name=text.product_name(seed, ordinal),
        category_code=category_history[-1][1],
        category_history=tuple(category_history),
        uom=rng.weighted_pick(seed, ("EA", "BOX", "CTN", "PLT", "KG"),
                              (0.58, 0.21, 0.13, 0.04, 0.04), "uom", ordinal),
        pack_size=rng.pick(seed, (1, 1, 1, 6, 10, 12, 24, 48, 100), "pack", ordinal),
        is_chiller=rng.chance(seed, 0.07, "chiller", ordinal),
        lead_time_days=rng.pick(seed, (2, 3, 5, 7, 10, 14, 21, 45), "lead", ordinal),
        supplier_ordinal=skew.pareto_ordinal(seed, max(supplier_population, 1), "prod-sup", ordinal),
        price_history=tuple(price_history),
        discontinued_on=discontinued,
        crosswalk=keys.CROSSWALK_CLEAN,
        is_dominant=skew.is_dominant(population, ordinal),
    )


def product(cfg, ordinal: int) -> Product:
    base = _product_cached(cfg.seed, ordinal, cfg.count("products"), cfg.count("suppliers"),
                           cfg.history_start.toordinal(), cfg.history_end.toordinal())
    state = keys.crosswalk_state(cfg.seed, PRODUCT, ordinal, cfg.defect("crosswalk_mismatch_rate"))
    if state == base.crosswalk:
        return base
    return Product(**{**base.__dict__, "crosswalk": state})


@lru_cache(maxsize=4096)
def _employee_cached(seed: int, ordinal: int, mix_key: tuple, start_ordinal: int) -> Employee:
    start = datetime.date.fromordinal(start_ordinal)
    region = region_for(seed, EMPLOYEE, ordinal, dict(mix_key))
    first, last = text.person_name(seed, ordinal)
    is_salesperson = rng.chance(seed, 0.42, "is-sp", ordinal)
    return Employee(
        ordinal=ordinal,
        oltp_id=3000 + ordinal,
        full_name="%s %s" % (first, last),
        region=region,
        role_code=rng.pick(seed, ("SALES", "SALES", "WHSE", "FIN", "PROC", "CS"),
                           "role", ordinal),
        territory_code="%s-%02d" % (region, 1 + rng.stable_hash(seed, "terr", ordinal) % 12),
        hired_date=start - datetime.timedelta(days=rng.stable_hash(seed, "hired", ordinal) % 4000),
        is_salesperson=is_salesperson,
        commission_plan=rng.pick(seed, ("PLAN-STD", "PLAN-KEY", "PLAN-NEW", "PLAN-NONE"),
                                 "comm", ordinal),
    )


def employee(cfg, ordinal: int) -> Employee:
    return _employee_cached(cfg.seed, ordinal, tuple(sorted(cfg.region_mix.items())),
                            cfg.history_start.toordinal())


def salesperson_ordinal(cfg, raw: int) -> int:
    """Map a customer's stored salesperson slot onto a real employee ordinal."""
    population = cfg.count("employees")
    return raw % max(population, 1)


def clear_caches() -> None:
    """Drop the derivation caches. Used by the determinism self-check."""
    _customer_cached.cache_clear()
    _supplier_cached.cache_clear()
    _product_cached.cache_clear()
    _employee_cached.cache_clear()
