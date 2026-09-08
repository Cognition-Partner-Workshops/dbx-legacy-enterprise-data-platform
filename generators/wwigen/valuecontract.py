"""Hold every generated Oracle row to the contract the deployed table declares.

Projecting an extract onto the deployed columns is only half a loadable file.
The first SMALL load proved the other half: 41 of 46 tables rejected rows the
column list fitted perfectly - a status code outside its CHECK domain
(ORA-02290), a surrogate key wider than ``NUMBER(12)`` (ORA-01438), a
reference key the deployment had already seeded (ORA-00001), a child pointing
at a parent that was itself rejected (ORA-02291).

None of those need a database to find. This module derives the row-level
contract from the same DDL the deployment runs, repairs a generated row into
it deterministically, and then re-checks the repaired row - so SQL*Loader
executes a load rather than discovering that the data was never valid:

* **types** - character values are cut to the declared width, numbers are
  quantised to the declared scale and folded into the declared precision;
* **CHECK domains** - a violated constraint is satisfied by steering the row
  onto one of the constraint's own branches, which is what makes a regional
  or status-dependent predicate repairable rather than merely detectable;
* **keys** - every unique constraint is a key space, single-column or
  composite: a tuple that repeats one an earlier row emitted or one the
  deployment seeds is moved off it deterministically, and the children that
  derived the moved key follow the same remapping. A generated tuple that
  repeats an earlier generated one and cannot be moved is a contract error;
  the one case that is not an error is a reference catalogue's natural key
  the deployment already seeds, where the generated row is the seeded row;
* **foreign keys** - a child value is resolved against the keys its parent
  extract actually emits plus the keys the deployment seeds, never against an
  independently invented space;
* **partitions** - a range-partitioned table only accepts a key a declared
  partition covers.

What cannot be repaired is raised as :class:`~wwigen.conform.ContractError`
during generation. A file this module has written is a file whose rows the
engine's own predicates accept.
"""

from __future__ import annotations

import datetime
import weakref

from . import canon, oracheck, rng

# Generated surrogate keys that have to be moved out of the way of an earlier
# row keep their identity band: high enough not to meet a seeded key, low
# enough to stay inside NUMBER(12).
REKEY_BAND = 700000000
REKEY_ATTEMPTS = 64

MAX_REPAIR_ROUNDS = 6


class ContractError(RuntimeError):
    """A generated extract cannot satisfy its target table."""


class SeededRow(Exception):
    """The row is the business row the deployment already seeded."""


class Violation:
    """One way a generated row fails the table's own contract."""

    def __init__(self, kind, table, column, detail):
        self.kind = kind
        self.table = table
        self.column = column
        self.detail = detail

    def __str__(self):
        where = "%s.%s" % (self.table, self.column) if self.column else self.table
        return "%s: %s (%s)" % (where, self.detail, self.kind)

    def __repr__(self):
        return "Violation(%r)" % str(self)


def numeric_limit(column):
    """The largest magnitude ``column`` can hold, or None when unbounded."""
    if not column.is_numeric or not column.precision:
        return None
    return 10 ** (column.precision - column.scale) - 10 ** -column.scale \
        if column.scale else 10 ** column.precision - 1


def fit_number(value, column):
    """``value`` as the column's NUMBER(p,s) can hold it.

    A scaled value is quantised and then clamped - an amount that overflows is
    an amount, and the row is still about the same business event. An integer
    key is folded into the precision instead, so distinct keys stay distinct
    for as long as the precision allows and the key guard settles the rest.
    """
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return value
    limit = numeric_limit(column)
    if column.scale:
        value = round(float(value), column.scale)
    elif isinstance(value, float):
        value = int(round(value))
    if limit is None or abs(value) <= limit:
        return value
    if column.scale:
        return limit if value > 0 else -limit
    folded = int(abs(value)) % (limit + 1)
    return -folded if value < 0 else folded


def fit_text(value, column):
    if value is None or not column.is_character:
        return value
    text = value if isinstance(value, str) else str(value)
    width = column.width
    return text[:width] if width else text


class ValueContract:
    """The deployed contract of one table, applied to the rows it is given.

    One instance serves one extract for one run: it holds the keys the extract
    has already emitted, so a duplicate is visible, and the remapping those
    duplicates caused, so a child can follow its parent.
    """

    def __init__(self, table, spec_key, written, cfg, ctx=None):
        self.table = table
        self.spec_key = spec_key
        self.written = tuple(written)
        self.writable = {column.name for column in self.written}
        self.cfg = cfg
        self.ctx = ctx
        self.seed = canon.oracle_seed()
        # A column the extract does not write takes the table's DEFAULT on
        # load. A constant default is as much part of the row the engine will
        # hold as anything written; only a default the load evaluates -
        # SYSDATE, USER - leaves the column without a verdict.
        self.unwritten = {column.name: column.default_value
                          for column in table.columns
                          if column.name not in self.writable and column.has_default}
        self.constant = {name: value for name, value in self.unwritten.items()
                         if not isinstance(value, oracheck.Unknowable)}
        self.unique_keys = tuple(
            key for key in table.unique_keys
            if key and all(name in self.writable or name in self.constant
                           for name in key))
        self.emitted = {key: set() for key in self.unique_keys}
        self.seeded = {key: self.seed.keys_of(table.key, key)
                       for key in self.unique_keys}
        self.rekeyed = 0
        self.seed_reconciled = 0

    # -- the pipeline ----------------------------------------------------

    def apply(self, values, index):
        """One generated row, repaired into the contract or refused."""
        values = self._fit(values)
        values = self._repair_checks(values, index)
        values = self._resolve_foreign_keys(values, index)
        values = self._fit_partition(values)
        values = self._settle_keys(values, index)
        self._enforce(values)
        return values

    def _fit(self, values):
        for column in self.written:
            value = values.get(column.name)
            if value is None:
                continue
            if column.is_character:
                values[column.name] = fit_text(value, column)
            elif column.is_numeric:
                values[column.name] = fit_number(value, column)
        return values

    # -- CHECK constraints ------------------------------------------------

    def _view(self, values):
        """The row as the engine will see it once the defaults have applied."""
        if not self.unwritten:
            return values
        view = dict(self.unwritten)
        view.update(values)
        return view

    def _broken(self, values):
        return self.table.violations(self._view(values))

    def _repair_checks(self, values, index):
        for _round in range(MAX_REPAIR_ROUNDS):
            broken = self._broken(values)
            if not broken:
                return values
            progressed = False
            for check in broken:
                repaired = self._satisfy(check, values, index)
                if repaired is not None:
                    values = repaired
                    progressed = True
            if not progressed:
                break
        return values

    def _satisfy(self, check, values, index):
        """The row steered onto one of the constraint's own branches."""
        before = len(self._broken(values))
        candidates = []
        for position, branch in enumerate(oracheck.branches(check.node)):
            already = sum(1 for part in branch
                          if part.evaluate(self._view(values)) is True)
            candidates.append((-already, position, branch))
        for _score, position, branch in sorted(candidates, key=lambda item: item[:2]):
            proposal = self._enforce_branch(branch, values, index, position)
            if proposal is None:
                continue
            if check.evaluate(self._view(proposal)) is False:
                continue
            if len(self._broken(proposal)) <= before:
                return proposal
        return None

    def _enforce_branch(self, branch, values, index, position):
        proposal = dict(values)
        for part in branch:
            if part.evaluate(self._view(proposal)) is not False:
                continue
            if not self._enforce_part(part, proposal, index, position):
                return None
        return self._fit(proposal)

    def _enforce_part(self, part, proposal, index, position):
        """Make one predicate hold, or report that it cannot be made to."""
        if isinstance(part, oracheck.In) and not part.negated:
            return self._set_from(part.operand, part.literals(), proposal, index, position)
        if isinstance(part, oracheck.IsNull):
            column = self._target(part.operand)
            if column is None:
                return False
            if part.negated:
                return self._set(column, self._filler(column, index, position), proposal)
            if column.required or column.name in self.table.primary_key:
                return False
            return self._set(column, None, proposal)
        if isinstance(part, oracheck.Between) and not part.negated:
            return self._enforce_between(part, proposal, index, position)
        if isinstance(part, oracheck.Compare):
            return self._enforce_compare(part, proposal, index, position)
        return False

    def _enforce_between(self, part, proposal, index, position):
        column = self._target(part.operand)
        low = part.low.evaluate(proposal)
        high = part.high.evaluate(proposal)
        if column is None or not isinstance(low, (int, float)) \
                or not isinstance(high, (int, float)):
            return False
        value = proposal.get(column.name)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            value = low
        value = min(max(value, low), high)
        return self._set(column, value, proposal)

    def _enforce_compare(self, part, proposal, index, position):
        column = self._target(part.left)
        if column is None:
            return False
        wanted = part.right.evaluate(proposal)
        if isinstance(wanted, oracheck.Unknowable):
            return False
        operator = part.operator
        if operator == "=":
            return self._set(column, wanted, proposal)
        if operator in ("<>", "!="):
            return self._set_unlike(column, wanted, proposal, index, position)
        if wanted is None:
            return False
        step = self._step(column, wanted)
        if operator == ">=":
            value = wanted
        elif operator == ">":
            value = wanted + step
        elif operator == "<=":
            value = wanted
        elif operator == "<":
            value = wanted - step
        else:
            return False
        return self._set(column, value, proposal)

    @staticmethod
    def _step(column, wanted):
        if isinstance(wanted, (datetime.date, datetime.datetime)):
            return datetime.timedelta(days=1)
        if column.scale:
            return 10 ** -column.scale
        return 1

    def _set_from(self, operand, literals, proposal, index, position):
        column = self._target(operand)
        if column is None:
            return False
        allowed = self._feasible(column, literals)
        if not allowed:
            return False
        value = rng.pick(self.cfg.seed, allowed, self.spec_key,
                         column.name, position, index)
        return self._set(column, value, proposal)

    @staticmethod
    def _feasible(column, values):
        """The values of a domain the column can actually hold.

        A CHECK may name a value the column is too narrow for -
        ``VARCHAR2(10)`` against ``'EU_30_90_ST'`` - and writing it would only
        move the rejection from ORA-02290 to ORA-12899.
        """
        keep = []
        for value in values:
            if column.is_character and isinstance(value, str) \
                    and column.width and len(value) > column.width:
                continue
            if column.is_numeric and isinstance(value, (int, float)) \
                    and not isinstance(value, bool):
                limit = numeric_limit(column)
                if limit is not None and abs(value) > limit:
                    continue
            keep.append(value)
        return tuple(keep)

    def _set_unlike(self, column, unwanted, proposal, index, position):
        """A value the column may hold that is not ``unwanted``."""
        allowed = [value for value in self._feasible(
            column, self.table.allowed_values.get(column.name, ()))
            if value != unwanted]
        if allowed:
            value = rng.pick(self.cfg.seed, tuple(allowed), self.spec_key,
                             column.name, position, index)
            return self._set(column, value, proposal)
        current = proposal.get(column.name)
        if isinstance(current, (int, float)) and not isinstance(current, bool):
            return self._set(column, current + self._step(column, current), proposal)
        if not column.required and column.name not in self.table.primary_key:
            return self._set(column, None, proposal)
        return False

    def _set(self, column, value, proposal):
        if column.name not in self.writable:
            return False
        if value is None and column.required:
            return False
        if column.is_character and value is not None:
            value = fit_text(value, column)
        elif column.is_numeric:
            value = fit_number(value, column)
        proposal[column.name] = value
        return True

    def _target(self, operand):
        """The writable column a predicate is about, if it is about one."""
        if not isinstance(operand, oracheck.ColumnRef):
            return None
        column = self.table.column(operand.name)
        if column is None or column.name not in self.writable:
            return None
        return column

    def _filler(self, column, index, position):
        """A value for a column a constraint requires to be present."""
        allowed = self._feasible(column, self.table.allowed_values.get(column.name, ()))
        if allowed:
            return rng.pick(self.cfg.seed, allowed, self.spec_key,
                            column.name, position, index)
        parent = self.table.foreign_keys.get(column.name)
        if parent is not None:
            space = self._parent_space(parent[0], parent[1])
            if space:
                ordered = self._ordered(space)
                return ordered[rng.stable_hash(self.cfg.seed, self.spec_key,
                                               column.name, index) % len(ordered)]
        if column.is_numeric:
            limit = numeric_limit(column)
            value = 1 + rng.stable_hash(self.cfg.seed, self.spec_key,
                                        column.name, index) % 100
            return value if limit is None else min(value, limit)
        if column.is_date:
            return self.cfg.history_end
        width = column.width or 1
        if width == 1:
            return "Y" if column.name.endswith("_FLG") else "X"
        token = "%s%04d" % (column.name.split("_")[0][:3].upper(),
                            rng.stable_hash(self.cfg.seed, self.spec_key,
                                            column.name, index) % 10000)
        return token[:width]

    # -- foreign keys ------------------------------------------------------

    def _resolve_foreign_keys(self, values, index):
        for name, (parent_key, parent_column) in sorted(self.table.foreign_keys.items()):
            if name not in self.writable:
                continue
            column = self.table.column(name)
            value = values.get(name)
            if value is None:
                continue
            space = self._parent_space(parent_key, parent_column)
            remap = self._parent_remap(parent_key, parent_column)
            if value in remap:
                value = remap[value]
            if space is None or value in space:
                values[name] = value
                continue
            if not space or not value:
                # Nothing to point at, or a value standing for "no parent": an
                # optional relationship is left unset rather than pointed at
                # an invented row.
                if column.required:
                    continue                     # reported by _enforce
                values[name] = None
                continue
            ordered = self._ordered(space)
            values[name] = ordered[rng.stable_hash(
                self.cfg.seed, self.spec_key, name, value) % len(ordered)]
        return values

    @staticmethod
    def _ordered(space):
        return sorted(space, key=lambda value: (str(type(value)), str(value)))

    def _parent_space(self, parent_key, parent_column):
        """Every key the parent will hold: its extract's plus the seeded ones.

        ``None`` means the parent is outside what this run can know about, and
        the child's value is left alone rather than remapped onto a guess.
        """
        if self.ctx is None:
            return None
        return keyspace(self.ctx, self.cfg, parent_key, parent_column)

    def _parent_remap(self, parent_key, parent_column):
        if self.ctx is None:
            return {}
        return remaps(self.ctx).get("%s.%s" % (parent_key, parent_column), {})

    # -- partitions --------------------------------------------------------

    def _fit_partition(self, values):
        partition = self.table.partition
        if partition is None or partition.column not in self.writable:
            return values
        value = values.get(partition.column)
        if partition.accepts(value):
            return values
        column = self.table.column(partition.column)
        if value is None:
            values[partition.column] = self._latest_date(values) or self.cfg.history_end
            return values
        highest = partition.highest
        if highest is not None and column.is_date:
            values[partition.column] = highest - datetime.timedelta(days=1)
        return values

    def _latest_date(self, values):
        dates = [value for value in values.values()
                 if isinstance(value, (datetime.date, datetime.datetime))]
        return max(dates) if dates else None

    # -- keys --------------------------------------------------------------

    def key_tuple(self, key, values):
        """The tuple the engine will index for ``key``, or None if it is null.

        A column the extract leaves to a constant DEFAULT is part of the
        indexed tuple exactly as a written one is - ``UK_TAX_RATE_EFF`` is
        four columns whether or not the extract writes ``RATE_CATEGORY_CD``.
        Oracle does not enforce a unique key whose tuple carries a null.
        """
        tuple_values = tuple(values.get(name) if name in self.writable
                             else self.constant.get(name) for name in key)
        return None if any(part is None for part in tuple_values) else tuple_values

    def _settle_keys(self, values, index):
        for key in self.unique_keys:
            candidate = self.key_tuple(key, values)
            if candidate is None:
                continue
            seeded = candidate in self.seeded[key]
            if not seeded and candidate not in self.emitted[key]:
                self.emitted[key].add(candidate)
                continue
            # A tuple the deployment seeds may only be moved by its surrogate
            # key: moving a business part of it would invent a currency the
            # estate does not have, where moving a surrogate leaves the same
            # row under a free identity.
            positions = self._movable_positions(key, surrogate_only=seeded)
            replacement = self._rekey(key, candidate, positions, values)
            if replacement is not None:
                self.emitted[key].add(replacement)
                self.rekeyed += 1
                continue
            if seeded:
                # Not a defect and not a silent loss: a reference catalogue's
                # natural key is the business identity of the row, so a
                # generated 'SG' *is* the deployment's seeded 'SG'. Moving it
                # would invent a country; emitting it would be ORA-00001. The
                # seeded row stands for it, and the run counts it.
                self.seed_reconciled += 1
                if self.ctx is not None:
                    store = reconciled(self.ctx)
                    store[self.spec_key] = store.get(self.spec_key, 0) + 1
                raise SeededRow("%s already seeds %s=%s"
                                % (self.table.key, ", ".join(key),
                                   ", ".join(str(part) for part in candidate)))
            raise ContractError(
                "%s cannot produce a unique %s: %s=%s repeats an earlier "
                "generated row and no part of the key may be moved"
                % (self.spec_key, ", ".join(key), ", ".join(key),
                   ", ".join(str(part) for part in candidate)))
        return values

    def _rekey(self, key, candidate, positions, values):
        """Move a repeated tuple off the occupied one, and record the move."""
        for position in positions:
            name = key[position]
            column = self.table.column(name)
            for attempt in range(REKEY_ATTEMPTS):
                moved = self._moved_value(column, candidate[position], attempt)
                if moved is None:
                    break
                replacement = tuple(moved if slot == position else part
                                    for slot, part in enumerate(candidate))
                if replacement in self.emitted[key] or replacement in self.seeded[key]:
                    continue
                values[name] = moved
                if self.ctx is not None and name in self.table.primary_key:
                    remaps(self.ctx).setdefault(
                        "%s.%s" % (self.table.key, name), {})[candidate[position]] = moved
                return replacement
        return None

    def _moved_value(self, column, value, attempt):
        """A deterministic alternative value for one part of a repeated tuple."""
        offset = rng.stable_hash(self.cfg.seed, self.spec_key, column.name,
                                 value, attempt)
        if column.is_numeric and not column.scale:
            limit = numeric_limit(column)
            moved = REKEY_BAND + offset % 90000000
            return moved if limit is None or moved <= limit else moved % (limit + 1)
        if column.is_date and isinstance(value, (datetime.date, datetime.datetime)):
            moved = value + datetime.timedelta(days=1 + attempt)
            partition = self.table.partition
            if partition is not None and partition.column == column.name \
                    and not partition.accepts(moved):
                return None
            return moved
        if column.is_character and isinstance(value, str):
            suffix = "-%03d" % (offset % 1000)
            width = column.width or (len(value) + len(suffix))
            return (value[:max(width - len(suffix), 0)] + suffix)[:width]
        return None

    def _movable_positions(self, key, surrogate_only=False):
        """The parts of a unique key that may be moved to break a collision.

        A foreign key may not: moving it points the row at a different parent,
        which makes it a different row rather than the same row re-keyed. Nor
        may a column the extract does not write - its value is the table's
        DEFAULT and the file cannot say otherwise. The table's own surrogate
        key is moved first, then any other free number, then an effective
        date, and only then a text part of the tuple. ``surrogate_only``
        keeps the move to the table's own surrogate key, which is what a
        collision with a deployed row allows.
        """
        ranked = []
        for position, name in enumerate(key):
            column = self.table.column(name)
            if column is None or name not in self.writable \
                    or name in self.table.foreign_keys:
                continue
            if column.is_numeric and not column.scale:
                rank = 0 if name in self.table.primary_key else 1
            elif column.is_date:
                rank = 2
            elif column.is_character:
                rank = 3
            else:
                continue
            if surrogate_only and rank:
                continue
            ranked.append((rank, position))
        return [position for _rank, position in sorted(ranked)]

    # -- the verdict -------------------------------------------------------

    def violations(self, values):
        """Everything the deployed table would reject this row for."""
        found = []
        for column in self.written:
            value = values.get(column.name)
            if value is None:
                if column.required:
                    found.append(Violation("nullability", self.table.key, column.name,
                                           "NOT NULL with no default (ORA-01400)"))
                continue
            if column.is_numeric and isinstance(value, (int, float)) \
                    and not isinstance(value, bool):
                limit = numeric_limit(column)
                if limit is not None and abs(value) > limit:
                    found.append(Violation(
                        "precision", self.table.key, column.name,
                        "%s does not fit NUMBER(%d,%d) (ORA-01438)"
                        % (value, column.precision, column.scale)))
                if column.scale and round(float(value), column.scale) != float(value):
                    found.append(Violation(
                        "scale", self.table.key, column.name,
                        "%s carries more than %d decimals" % (value, column.scale)))
            if column.is_character and isinstance(value, str) and column.width \
                    and len(value) > column.width:
                found.append(Violation(
                    "width", self.table.key, column.name,
                    "%d characters do not fit VARCHAR2(%d) (ORA-12899)"
                    % (len(value), column.width)))
        for check in self._broken(values):
            found.append(Violation("check", self.table.key,
                                   ", ".join(check.columns()),
                                   "%s violated (ORA-02290)"
                                   % (check.name or check.text)))
        for name, (parent_key, parent_column) in sorted(self.table.foreign_keys.items()):
            if name not in self.writable:
                continue
            value = values.get(name)
            if value is None:
                continue
            space = self._parent_space(parent_key, parent_column)
            if space is not None and value not in space:
                found.append(Violation(
                    "foreign-key", self.table.key, name,
                    "%s is not a key of %s.%s (ORA-02291)"
                    % (value, parent_key, parent_column)))
        partition = self.table.partition
        if partition is not None and partition.column in self.writable \
                and not partition.accepts(values.get(partition.column)):
            found.append(Violation(
                "partition", self.table.key, partition.column,
                "%s falls outside the declared partitions (ORA-14400)"
                % (values.get(partition.column),)))
        return found

    def _enforce(self, values):
        found = self.violations(values)
        if found:
            raise ContractError("%s cannot produce a loadable row: %s"
                                % (self.spec_key, "; ".join(str(item) for item in found)))


# -- the run's key spaces --------------------------------------------------

_RUN_STATE = weakref.WeakKeyDictionary()


def _store(ctx, name):
    """One named dictionary per run context, created on first use."""
    return _RUN_STATE.setdefault(ctx, {}).setdefault(name, {})


def remaps(ctx):
    """{TABLE.COLUMN: {generated key: the key it was moved to}} for the run."""
    return _store(ctx, "key_remaps")


def emitted(ctx):
    """{TABLE.COLUMN: {key, ...}} for every Oracle key written this run."""
    return _store(ctx, "emitted_keys")


def reconciled(ctx):
    """{TABLE: rows the deployment already seeds} for the run.

    A row here is not lost: the deployed catalogue holds its business
    identity. The count is kept so the run can say so out loud rather than
    leave a table short of its extract with no explanation.
    """
    return _store(ctx, "seed_reconciled")


def record(ctx, table_key, column, value):
    if value is None:
        return
    emitted(ctx).setdefault("%s.%s" % (table_key, column), set()).add(value)


def keyspace(ctx, cfg, table_key, column):
    """Every value ``table_key.column`` will hold once the estate is loaded.

    The seeded rows are known from the DDL; the generated ones are known by
    running the parent's own extract, once per run, and remembering the keys
    it emits. A parent that is being produced at this very moment - a
    self-reference - contributes what it has emitted so far.
    """
    name = "%s.%s" % (table_key, column)
    known = emitted(ctx)
    harvested = _store(ctx, "harvested_keys")
    if table_key not in harvested:
        harvested[table_key] = True
        _harvest(ctx, cfg, table_key)
    seeded = canon.oracle_seed().values_of(table_key, column)
    return frozenset(known.get(name, frozenset())) | seeded


def _harvest(ctx, cfg, table_key):
    """Run the parent's extract for its keys alone."""
    from . import tables
    running = _store(ctx, "harvesting")
    if running.get(table_key):
        return
    spec = None
    for candidate in tables.all_specs():
        if "%s.%s" % (candidate.schema, candidate.name) == table_key:
            spec = candidate
            break
    if spec is None:
        return
    running[table_key] = True
    try:
        for _row in spec.produce(cfg, ctx):
            pass
    finally:
        running[table_key] = False
