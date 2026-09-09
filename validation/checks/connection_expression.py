#!/usr/bin/env python3
"""Evaluate a generated connection-manager ConnectionString expression.

The provider never sees the literal ``DTS:ConnectionString`` attribute: the
package re-evaluates the ConnectionString property expression from the project
parameters when the connection is opened. Anything that wants to prove the
generated contract actually reaches OraOLEDB or MSOLEDBSQL19 - the runtime
acceptance test in validation/runtime - therefore has to evaluate the same
expression the runtime does, not a second string maintained beside it.

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


def connection_expression(conmgr_path):
    """The ConnectionString property expression held in a .conmgr file."""
    root = ET.parse(conmgr_path).getroot()
    for node in root.iter("{%s}PropertyExpression" % DTS_NS):
        if node.get("{%s}Name" % DTS_NS) == "ConnectionString":
            return node.text or ""
    raise ExpressionError("%s has no ConnectionString property expression" % conmgr_path)


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
    parser.add_argument("--provider", default="", help="value for the provider parameter")
    parser.add_argument("--trust-server-certificate", default="true")
    parser.add_argument("--set", action="append", default=[], metavar="NAME=VALUE",
                        help="override one project parameter")
    args = parser.parse_args()

    defaults = {
        "OracleProvider": args.provider or "OraOLEDB.Oracle.1",
        "SqlServerProvider": args.provider or "MSOLEDBSQL19.1",
        "SqlServerTrustServerCertificate": args.trust_server_certificate,
    }
    values = parameters_from_environment(defaults, redact=not args.unmasked)
    for override in args.set:
        name, _, value = override.partition("=")
        values[name] = value
    try:
        print(evaluate(connection_expression(args.conmgr), values))
    except ExpressionError as error:
        sys.stderr.write("connection expression: %s\n" % error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
