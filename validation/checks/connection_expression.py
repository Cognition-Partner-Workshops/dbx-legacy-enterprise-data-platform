#!/usr/bin/env python3
"""Evaluate the property expressions a generated connection manager carries.

An OLE DB connection manager is retargeted property by property - ServerName,
InitialCatalog, UserName - on top of the design-time ``DTS:ConnectionString``
literal, and the password arrives separately on ``CM.<connection>.Password``.
The string the provider finally receives is therefore the literal with those
properties folded back into it, which is what ``effective_connection_string``
builds. Anything that wants to prove the generated contract actually reaches
OraOLEDB or MSOLEDBSQL19 - the runtime acceptance test in validation/runtime -
has to compose the same string the runtime composes, not a second one
maintained beside it.

No OLE DB connection manager may carry a ``ConnectionString`` expression:
assigning that property rebuilds the connection from the string alone and
discards the password already applied to it, which is the defect behind
'Login failed for user' / DTS_E_CANNOTACQUIRECONNECTIONFROMCONNECTIONMANAGER.
``assert_no_connection_string_expression`` is the guard.

Only the constructs the generator emits are supported: string literals,
concatenation, project parameter references, the ``(DT_WSTR, n)`` cast, LEN(),
comparison against a number and the conditional operator.

Usage:
    python3 validation/checks/connection_expression.py <path.conmgr> [--unmasked]

Without --unmasked every password parameter is rendered as ``********``, so the
default output is safe to log.

The module is also the static guard for 0xC0017010: ``assert_no_sensitive``
refuses an expression that names a sensitive project parameter at all. The SSIS
expression evaluator cannot read a sensitive parameter, so such an expression
fails the package at validation, long before the credential would be used. The
supported carrier for a password is the connection manager's own
``CM.<connection>.Password`` project parameter, which the runtime applies to the
connection manager object rather than evaluating in an expression.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET

DTS_NS = "www.microsoft.com/SqlServer/Dts"
REDACTED = "********"

# project parameter -> environment variable the runner injects it from
PARAMETER_ENVIRONMENT = {
    "OracleHost": "ORACLE_HOST",
    "OraclePort": "ORACLE_PORT",
    "OracleService": "ORACLE_SERVICE",
    "OracleUser": "ORACLE_USER",
    "SqlServerHost": "SQLSERVER_HOST",
    "SqlServerPort": "SQLSERVER_PORT",
    "SqlServerUser": "SQLSERVER_USER",
    "SqlServerOltpDb": "SQLSERVER_OLTP_DB",
    "SqlServerStagingDb": "SQLSERVER_STAGING_DB",
    "SqlServerDwDb": "SQLSERVER_DW_DB",
    "InboundFileRoot": "ETL_INBOUND_FILE_ROOT",
    "ArchiveFileRoot": "ETL_ARCHIVE_FILE_ROOT",
    "QuarantineFileRoot": "ETL_QUARANTINE_FILE_ROOT",
}
# Project parameters that are Sensitive=1 in the project manifest, plus the
# connection-manager parameters that carry the credentials. None of them may be
# named by an expression.
SENSITIVE_PARAMETERS = ("OraclePassword", "SqlServerPassword")
SENSITIVE_PARAMETER_RE = re.compile(
    r"@\[\$Project::(?P<name>\w*(?:Password|Secret|Pwd)\w*|CM\.[\w.]*Password)\]",
    re.IGNORECASE)
PARAMETER_TOKEN_RE = re.compile(r"@\[\$Project::(?P<name>[\w.]+)\]")

# connection manager property -> the OLE DB keyword it writes into the
# connection string, as MSOLEDBSQL/OraOLEDB spell it.
PROPERTY_KEYWORD = {
    "ServerName": "Data Source",
    "InitialCatalog": "Initial Catalog",
    "UserName": "User ID",
    "Password": "Password",
}

TOKEN_RE = re.compile(r"""
    (?P<space>\s+)
  | (?P<string>"(?:[^"\\]|\\.)*")
  | (?P<parameter>@\[\$Project::\w+\])
  | (?P<cast>\(\s*DT_WSTR\s*,\s*\d+\s*\))
  | (?P<number>\d+)
  | (?P<name>[A-Za-z_]\w*)
  | (?P<operator><=|>=|==|!=|[+()?:<>,])
""", re.VERBOSE)


class ExpressionError(Exception):
    """The expression uses something this evaluator does not model."""


def tokenize(text):
    tokens = []
    position = 0
    while position < len(text):
        match = TOKEN_RE.match(text, position)
        if not match:
            raise ExpressionError("cannot tokenize at %r" % text[position:position + 30])
        position = match.end()
        kind = match.lastgroup
        if kind != "space":
            tokens.append((kind, match.group()))
    return tokens


class Parser:
    """Recursive descent over the subset the generator emits."""

    def __init__(self, tokens, parameters):
        self.tokens = tokens
        self.index = 0
        self.parameters = parameters

    def peek(self):
        return self.tokens[self.index] if self.index < len(self.tokens) else (None, None)

    def take(self, value=None):
        kind, text = self.peek()
        if kind is None or (value is not None and text != value):
            raise ExpressionError("expected %r, found %r" % (value, text))
        self.index += 1
        return text

    def parse(self):
        value = self.conditional()
        if self.index != len(self.tokens):
            raise ExpressionError("trailing tokens from %r" % (self.peek()[1],))
        return value

    def conditional(self):
        condition = self.comparison()
        if self.peek()[1] != "?":
            return condition
        self.take("?")
        when_true = self.conditional()
        self.take(":")
        when_false = self.conditional()
        return when_true if truthy(condition) else when_false

    def comparison(self):
        left = self.concatenation()
        kind, text = self.peek()
        if kind == "operator" and text in ("<", ">", "<=", ">=", "==", "!="):
            self.take()
            right = self.concatenation()
            return compare(text, left, right)
        return left

    def concatenation(self):
        value = self.unary()
        while self.peek()[1] == "+":
            self.take("+")
            value = "%s%s" % (value, self.unary())
        return value

    def unary(self):
        kind, text = self.peek()
        if kind == "cast":
            self.take()
            return str(self.unary())
        if kind == "string":
            self.take()
            return text[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        if kind == "number":
            self.take()
            return int(text)
        if kind == "parameter":
            self.take()
            name = text[len("@[$Project::"):-1]
            if name not in self.parameters:
                raise ExpressionError("no value supplied for $Project::%s" % name)
            return self.parameters[name]
        if kind == "name" and text.upper() == "LEN":
            self.take()
            self.take("(")
            inner = self.conditional()
            self.take(")")
            return len(str(inner))
        if text == "(":
            self.take("(")
            value = self.conditional()
            self.take(")")
            return value
        raise ExpressionError("unexpected token %r" % (text,))


def truthy(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1")
    return bool(value)


def compare(operator, left, right):
    if isinstance(left, str) != isinstance(right, str):
        left, right = coerce_number(left), coerce_number(right)
    return {
        "<": left < right, ">": left > right,
        "<=": left <= right, ">=": left >= right,
        "==": left == right, "!=": left != right,
    }[operator]


def coerce_number(value):
    return value if isinstance(value, int) else int(str(value) or 0)


def evaluate(expression, parameters):
    return Parser(tokenize(expression), parameters).parse()


def expression_parameters(expression):
    """Every project parameter an expression names, in order of appearance."""
    return [match.group("name") for match in PARAMETER_TOKEN_RE.finditer(expression)]


def sensitive_references(expression):
    """The sensitive project parameters an expression names.

    Both the known sensitive parameter names and anything shaped like a
    credential are reported, so a parameter added later cannot slip into an
    expression just because this module has not heard of it.
    """
    found = []
    for name in expression_parameters(expression):
        if name in SENSITIVE_PARAMETERS or SENSITIVE_PARAMETER_RE.search(
                "@[$Project::%s]" % name):
            if name not in found:
                found.append(name)
    return found


def assert_no_sensitive(expression, origin="expression"):
    """Raise if the expression names a sensitive parameter (0xC0017010 guard)."""
    found = sensitive_references(expression)
    if found:
        raise ExpressionError(
            "%s references the sensitive project parameter(s) %s; a sensitive "
            "parameter cannot be read by the SSIS expression evaluator "
            "(0xC0017010). Bind the credential through the connection manager's "
            "CM.<connection>.Password parameter instead."
            % (origin, ", ".join(found)))


def connection_expressions(conmgr_path):
    """The property expressions held in a .conmgr file, in document order."""
    root = ET.parse(conmgr_path).getroot()
    found = []
    for node in root.iter("{%s}PropertyExpression" % DTS_NS):
        found.append((node.get("{%s}Name" % DTS_NS), node.text or ""))
    if not found:
        raise ExpressionError("%s has no property expression at all; nothing "
                             "retargets it at run time" % conmgr_path)
    return found


def connection_kind(conmgr_path):
    """The CreationName of a .conmgr file: OLEDB, FILE, ..."""
    root = ET.parse(conmgr_path).getroot()
    return root.get("{%s}CreationName" % DTS_NS) or ""


def connection_literal(conmgr_path):
    """The design-time DTS:ConnectionString attribute of a .conmgr file."""
    root = ET.parse(conmgr_path).getroot()
    for data in root.iter("{%s}ObjectData" % DTS_NS):
        for inner in data.iter("{%s}ConnectionManager" % DTS_NS):
            value = inner.get("{%s}ConnectionString" % DTS_NS)
            if value is not None:
                return value
    raise ExpressionError("%s has no design-time connection string" % conmgr_path)


def connection_expression(conmgr_path):
    """The ConnectionString property expression of a FILE connection manager."""
    for name, text in connection_expressions(conmgr_path):
        if name == "ConnectionString":
            return text
    raise ExpressionError("%s has no ConnectionString property expression" % conmgr_path)


def assert_no_connection_string_expression(conmgr_path):
    """Raise if an OLE DB connection manager expresses its whole ConnectionString.

    Setting ConnectionString rebuilds the connection manager from that string,
    dropping the password the catalog applied through CM.<connection>.Password;
    the provider then answers 0x80040E4D 'Login failed for user' and the task
    fails with DTS_E_CANNOTACQUIRECONNECTIONFROMCONNECTIONMANAGER.
    """
    if connection_kind(conmgr_path) != "OLEDB":
        return
    for name, _text in connection_expressions(conmgr_path):
        if name == "ConnectionString":
            raise ExpressionError(
                "%s expresses ConnectionString; evaluating it rebuilds the "
                "connection and discards the password applied through "
                "CM.<connection>.Password. Retarget ServerName, InitialCatalog "
                "and UserName instead." % conmgr_path)


def parse_connection_string(text):
    """An OLE DB connection string as an ordered list of (keyword, value)."""
    pairs = []
    for fragment in text.split(";"):
        if not fragment.strip():
            continue
        keyword, _, value = fragment.partition("=")
        pairs.append((keyword.strip(), value.strip()))
    return pairs


def effective_connection_string(conmgr_path, parameters, credential=None):
    """The string the provider receives: literal + evaluated property expressions.

    This is the runtime's own composition order - the catalog and the expression
    evaluator write connection manager properties, and the connection manager
    rewrites its connection string from them - so a check that opens this string
    is testing what the package will actually open.
    """
    pairs = parse_connection_string(connection_literal(conmgr_path))
    for name, expression in connection_expressions(conmgr_path):
        if name == "ConnectionString":
            pairs = parse_connection_string(str(evaluate(expression, parameters)))
            continue
        keyword = PROPERTY_KEYWORD.get(name)
        if keyword is None:
            raise ExpressionError("%s expresses the unmodelled property %s"
                                  % (conmgr_path, name))
        pairs = _assign(pairs, keyword, str(evaluate(expression, parameters)))
    if credential is not None:
        pairs = _assign(pairs, "Password", credential)
    return "".join("%s=%s;" % (keyword, value) for keyword, value in pairs)


def _assign(pairs, keyword, value):
    """Set one keyword, dropping it entirely when the value is empty.

    An empty UserName is how the connection manager selects Windows
    authentication: the keyword is not written at all.
    """
    out = [(existing, held) for existing, held in pairs if existing.lower() != keyword.lower()]
    if value == "":
        return out
    out.append((keyword, value))
    return out


def parameters_from_environment(defaults=None, redact=False):
    """Runtime parameter values, taken from the environment by name."""
    values = dict(defaults or {})
    for parameter, variable in PARAMETER_ENVIRONMENT.items():
        if variable in os.environ:
            values[parameter] = os.environ[variable]
    if redact:
        for parameter in SENSITIVE_PARAMETERS:
            if parameter in values:
                values[parameter] = REDACTED
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("conmgr")
    parser.add_argument("--unmasked", action="store_true",
                        help="render real password values (caller must not log the result)")
    parser.add_argument("--password-env", default="", metavar="NAME",
                        help="append the password held by this environment "
                             "variable, the way CM.<connection>.Password does")
    parser.add_argument("--set", action="append", default=[], metavar="NAME=VALUE",
                        help="override one project parameter")
    args = parser.parse_args()

    values = parameters_from_environment(redact=not args.unmasked)
    credential = None
    if args.password_env:
        credential = os.environ.get(args.password_env, "")
        if not args.unmasked:
            credential = REDACTED
    for override in args.set:
        name, _, value = override.partition("=")
        if name in SENSITIVE_PARAMETERS:
            credential = value
            continue
        values[name] = value
    try:
        print(effective_connection_string(args.conmgr, values, credential))
    except ExpressionError as error:
        sys.stderr.write("connection expression: %s\n" % error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
