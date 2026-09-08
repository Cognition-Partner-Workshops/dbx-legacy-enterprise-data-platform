"""The CHECK constraints the Oracle estate declares, as something executable.

A generated row is only loadable if the engine's own predicates accept it, and
those predicates are not decoration: the estate declares 263 of them, and the
first SMALL load was rejected table after table on ORA-02290. This module
reads a constraint's text out of the DDL and turns it into a small expression
tree the generator can both evaluate against a row and read back - which
values a column may take, which columns a predicate is about, which branch of
an OR a row should be steered onto.

The grammar is the one the estate writes: ``IN`` lists, comparisons against
literals and other columns, ``IS [NOT] NULL``, ``BETWEEN``, ``LENGTH()``, and
``AND``/``OR``/``NOT`` over those. Anything outside it parses to
:class:`Unknown`, which evaluates to unknown rather than to a verdict - the
generator never claims a row satisfies a constraint it could not read.

Evaluation follows SQL's three-valued logic, so a constraint is violated only
when it evaluates to ``False``; ``None`` means the engine would accept the row.
"""

from __future__ import annotations

import datetime
import re

TOKEN_RE = re.compile(r"""
    (?P<space>\s+)
  | (?P<string>'(?:[^']|'')*')
  | (?P<number>\d+(?:\.\d+)?)
  | (?P<name>[A-Za-z][A-Za-z0-9_$#]*)
  | (?P<op><>|!=|>=|<=|=|<|>)
  | (?P<punct>[(),])
""", re.X)

KEYWORDS = frozenset({"AND", "OR", "NOT", "IN", "IS", "NULL", "BETWEEN", "LIKE"})


class Node:
    """One node of a parsed CHECK expression."""

    def evaluate(self, row):
        raise NotImplementedError

    def columns(self):
        return ()


class Unknown(Node):
    """A construct the parser does not model. Never a verdict."""

    def __init__(self, text):
        self.text = text

    def evaluate(self, row):
        return None

    def __repr__(self):
        return "Unknown(%r)" % self.text


class Literal(Node):
    def __init__(self, value):
        self.value = value

    def evaluate(self, row):
        return self.value

    def __repr__(self):
        return "Literal(%r)" % (self.value,)


class ColumnRef(Node):
    def __init__(self, name):
        self.name = name.upper()

    def evaluate(self, row):
        return row.get(self.name)

    def columns(self):
        return (self.name,)

    def __repr__(self):
        return "Column(%s)" % self.name


class Func(Node):
    """A function call. Only the ones the estate's constraints use compute."""

    def __init__(self, name, arguments):
        self.name = name.upper()
        self.arguments = tuple(arguments)

    def evaluate(self, row):
        values = [argument.evaluate(row) for argument in self.arguments]
        if any(value is None for value in values):
            return None
        if self.name == "LENGTH" and len(values) == 1:
            return len(str(values[0]))
        if self.name in ("UPPER", "LOWER") and len(values) == 1:
            text = str(values[0])
            return text.upper() if self.name == "UPPER" else text.lower()
        if self.name == "TRUNC" and len(values) == 1:
            value = values[0]
            if isinstance(value, datetime.datetime):
                return value.date()
            return value
        if self.name in ("NVL", "COALESCE"):
            for value in values:
                if value is not None:
                    return value
            return None
        return UNKNOWN_VALUE

    def columns(self):
        return tuple(name for argument in self.arguments for name in argument.columns())

    def __repr__(self):
        return "%s(%s)" % (self.name, ", ".join(repr(a) for a in self.arguments))


class Unknowable:
    """A value the module cannot compute; any comparison with it is unknown."""


UNKNOWN_VALUE = Unknowable()


def _comparable(left, right):
    """The pair as values that can be ordered, or None if they cannot."""
    if isinstance(left, Unknowable) or isinstance(right, Unknowable):
        return None
    if isinstance(left, bool) or isinstance(right, bool):
        return None
    numbers = (int, float)
    if isinstance(left, numbers) and isinstance(right, numbers):
        return left, right
    if isinstance(left, str) and isinstance(right, str):
        return left, right
    dates = (datetime.date, datetime.datetime)
    if isinstance(left, dates) and isinstance(right, dates):
        return _as_datetime(left), _as_datetime(right)
    if isinstance(left, str) and isinstance(right, numbers):
        return _numeric(left), right
    if isinstance(left, numbers) and isinstance(right, str):
        return left, _numeric(right)
    return None


def _as_datetime(value):
    if isinstance(value, datetime.datetime):
        return value
    return datetime.datetime(value.year, value.month, value.day)


def _numeric(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


class Compare(Node):
    OPERATORS = {
        "=": lambda a, b: a == b,
        "<>": lambda a, b: a != b,
        "!=": lambda a, b: a != b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
    }

    def __init__(self, left, operator, right):
        self.left = left
        self.operator = operator
        self.right = right

    def evaluate(self, row):
        left = self.left.evaluate(row)
        right = self.right.evaluate(row)
        if left is None or right is None:
            return None
        pair = _comparable(left, right)
        if pair is None or pair[0] is None or pair[1] is None:
            return None
        return self.OPERATORS[self.operator](pair[0], pair[1])

    def columns(self):
        return self.left.columns() + self.right.columns()

    def __repr__(self):
        return "(%r %s %r)" % (self.left, self.operator, self.right)


class In(Node):
    def __init__(self, operand, values, negated=False):
        self.operand = operand
        self.values = tuple(values)
        self.negated = negated

    def evaluate(self, row):
        value = self.operand.evaluate(row)
        if value is None or isinstance(value, Unknowable):
            return None
        members = [item.evaluate(row) for item in self.values]
        if any(isinstance(member, Unknowable) for member in members):
            return None
        found = any(_equal(value, member) for member in members)
        return not found if self.negated else found

    def columns(self):
        return self.operand.columns() + tuple(
            name for item in self.values for name in item.columns())

    def literals(self):
        return tuple(item.value for item in self.values if isinstance(item, Literal))

    def __repr__(self):
        return "(%r %sIN %r)" % (self.operand, "NOT " if self.negated else "",
                                 [item for item in self.values])


def _equal(left, right):
    if right is None:
        return False
    pair = _comparable(left, right)
    if pair is None or pair[0] is None or pair[1] is None:
        return False
    return pair[0] == pair[1]


class Between(Node):
    def __init__(self, operand, low, high, negated=False):
        self.operand = operand
        self.low = low
        self.high = high
        self.negated = negated

    def evaluate(self, row):
        lower = Compare(self.operand, ">=", self.low).evaluate(row)
        upper = Compare(self.operand, "<=", self.high).evaluate(row)
        if lower is None or upper is None:
            return None
        inside = lower and upper
        return not inside if self.negated else inside

    def columns(self):
        return self.operand.columns() + self.low.columns() + self.high.columns()

    def __repr__(self):
        return "(%r BETWEEN %r AND %r)" % (self.operand, self.low, self.high)


class IsNull(Node):
    def __init__(self, operand, negated=False):
        self.operand = operand
        self.negated = negated

    def evaluate(self, row):
        value = self.operand.evaluate(row)
        if isinstance(value, Unknowable):
            return None
        return (value is not None) if self.negated else (value is None)

    def columns(self):
        return self.operand.columns()

    def __repr__(self):
        return "(%r IS %sNULL)" % (self.operand, "NOT " if self.negated else "")


class And(Node):
    def __init__(self, parts):
        self.parts = tuple(parts)

    def evaluate(self, row):
        result = True
        for part in self.parts:
            value = part.evaluate(row)
            if value is False:
                return False
            if value is None:
                result = None
        return result

    def columns(self):
        return tuple(name for part in self.parts for name in part.columns())

    def __repr__(self):
        return "(%s)" % " AND ".join(repr(part) for part in self.parts)


class Or(Node):
    def __init__(self, parts):
        self.parts = tuple(parts)

    def evaluate(self, row):
        result = False
        for part in self.parts:
            value = part.evaluate(row)
            if value is True:
                return True
            if value is None:
                result = None
        return result

    def columns(self):
        return tuple(name for part in self.parts for name in part.columns())

    def __repr__(self):
        return "(%s)" % " OR ".join(repr(part) for part in self.parts)


class Not(Node):
    def __init__(self, operand):
        self.operand = operand

    def evaluate(self, row):
        value = self.operand.evaluate(row)
        return None if value is None else (not value)

    def columns(self):
        return self.operand.columns()

    def __repr__(self):
        return "(NOT %r)" % (self.operand,)


class _Parser:
    def __init__(self, text):
        self.text = text
        self.tokens = self._tokenise(text)
        self.position = 0

    @staticmethod
    def _tokenise(text):
        tokens, position = [], 0
        while position < len(text):
            match = TOKEN_RE.match(text, position)
            if match is None:
                tokens.append(("bad", text[position]))
                position += 1
                continue
            position = match.end()
            kind = match.lastgroup
            if kind == "space":
                continue
            value = match.group()
            if kind == "name" and value.upper() in KEYWORDS:
                kind = "keyword"
                value = value.upper()
            tokens.append((kind, value))
        return tokens

    # -- token helpers ---------------------------------------------------
    def peek(self, offset=0):
        index = self.position + offset
        return self.tokens[index] if index < len(self.tokens) else (None, None)

    def next(self):
        token = self.peek()
        self.position += 1
        return token

    def accept(self, kind, value=None):
        token = self.peek()
        if token[0] == kind and (value is None or token[1].upper() == value):
            self.position += 1
            return True
        return False

    def expect(self, kind, value=None):
        if not self.accept(kind, value):
            raise _ParseError("expected %s %s at %r" % (kind, value or "", self.peek()))

    # -- grammar ---------------------------------------------------------
    def parse(self):
        node = self.disjunction()
        if self.position != len(self.tokens):
            raise _ParseError("trailing input at %r" % (self.peek(),))
        return node

    def disjunction(self):
        parts = [self.conjunction()]
        while self.accept("keyword", "OR"):
            parts.append(self.conjunction())
        return parts[0] if len(parts) == 1 else Or(parts)

    def conjunction(self):
        parts = [self.factor()]
        while self.accept("keyword", "AND"):
            parts.append(self.factor())
        return parts[0] if len(parts) == 1 else And(parts)

    def factor(self):
        if self.accept("keyword", "NOT"):
            return Not(self.factor())
        if self.peek()[0] == "punct" and self.peek()[1] == "(" and self._group_is_predicate():
            self.expect("punct", "(")
            node = self.disjunction()
            self.expect("punct", ")")
            return node
        return self.predicate()

    def _group_is_predicate(self):
        """Whether a '(' opens a nested predicate rather than an operand.

        ``(A = 1 AND B = 2)`` is a predicate; ``(A + B) > 1`` is an operand.
        The estate never writes arithmetic in a CHECK, so a group holding a
        comparison, IN, IS or a connective is a predicate.
        """
        depth = 0
        index = self.position
        while index < len(self.tokens):
            kind, value = self.tokens[index]
            if kind == "punct" and value == "(":
                depth += 1
            elif kind == "punct" and value == ")":
                depth -= 1
                if depth == 0:
                    return False
            elif depth == 1 and (kind == "op" or (kind == "keyword" and value in
                                                  ("AND", "OR", "IS", "IN", "BETWEEN"))):
                return True
            index += 1
        return False

    def predicate(self):
        operand = self.operand()
        negated = self.accept("keyword", "NOT")
        if self.accept("keyword", "IN"):
            self.expect("punct", "(")
            values = [self.operand()]
            while self.accept("punct", ","):
                values.append(self.operand())
            self.expect("punct", ")")
            return In(operand, values, negated)
        if self.accept("keyword", "BETWEEN"):
            low = self.operand()
            self.expect("keyword", "AND")
            return Between(operand, low, self.operand(), negated)
        if negated:
            raise _ParseError("NOT before %r" % (self.peek(),))
        if self.accept("keyword", "IS"):
            null_negated = self.accept("keyword", "NOT")
            self.expect("keyword", "NULL")
            return IsNull(operand, null_negated)
        if self.peek()[0] == "op":
            operator = self.next()[1]
            return Compare(operand, operator, self.operand())
        if self.accept("keyword", "LIKE"):
            self.operand()
            return Unknown("LIKE")
        raise _ParseError("not a predicate at %r" % (self.peek(),))

    def operand(self):
        kind, value = self.next()
        if kind == "string":
            return Literal(value[1:-1].replace("''", "'"))
        if kind == "number":
            return Literal(float(value) if "." in value else int(value))
        if kind == "keyword" and value == "NULL":
            return Literal(None)
        if kind == "name":
            if self.peek() == ("punct", "("):
                self.expect("punct", "(")
                arguments = []
                if not self.accept("punct", ")"):
                    arguments.append(self.operand())
                    while self.accept("punct", ","):
                        arguments.append(self.operand())
                    self.expect("punct", ")")
                return Func(value, arguments)
            return ColumnRef(value)
        raise _ParseError("not an operand: %r" % ((kind, value),))


class _ParseError(ValueError):
    pass


def parse(text):
    """The expression tree for one CHECK body, or :class:`Unknown`."""
    try:
        return _Parser(text).parse()
    except (_ParseError, IndexError):
        return Unknown(" ".join(text.split()))


def branches(node):
    """The top-level OR branches of a constraint, each as a list of conjuncts.

    A row satisfies the constraint by satisfying every conjunct of one branch,
    which is what makes a violated regional or status-dependent constraint
    repairable instead of merely detectable.
    """
    if isinstance(node, Or):
        return [conjuncts(part) for part in node.parts]
    return [conjuncts(node)]


def conjuncts(node):
    if isinstance(node, And):
        result = []
        for part in node.parts:
            result.extend(conjuncts(part))
        return result
    return [node]


def domain_of(node, column):
    """The values ``column`` may take, if the constraint pins it to a set.

    Only unconditional restrictions count: a value set that holds whatever
    else the row says. ``STATUS_CD IN ('A','B')`` gives a domain,
    ``REGION_CD = 'EU' AND METHOD_CD IN (...)`` does not.
    """
    values = None
    for part in conjuncts(node):
        found = _unconditional_domain(part, column)
        if found is None:
            continue
        values = found if values is None else tuple(
            value for value in values if value in found)
    return values


def _unconditional_domain(node, column):
    if isinstance(node, In) and not node.negated \
            and isinstance(node.operand, ColumnRef) and node.operand.name == column:
        literals = node.literals()
        return literals or None
    if isinstance(node, Compare) and node.operator == "=" \
            and isinstance(node.left, ColumnRef) and node.left.name == column \
            and isinstance(node.right, Literal):
        return (node.right.value,)
    if isinstance(node, Or):
        # (X IS NULL OR X IN (...)) leaves the same value domain for X.
        domains = []
        for part in node.parts:
            found = _unconditional_domain(part, column)
            if found is None:
                if _is_null_test(part, column):
                    continue
                return None
            domains.append(found)
        if not domains:
            return None
        merged = []
        for domain in domains:
            for value in domain:
                if value not in merged:
                    merged.append(value)
        return tuple(merged)
    return None


def _is_null_test(node, column):
    return isinstance(node, IsNull) and not node.negated \
        and isinstance(node.operand, ColumnRef) and node.operand.name == column
