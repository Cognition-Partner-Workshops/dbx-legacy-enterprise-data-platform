"""The SSIS expression subset the orchestration plan's precedence edges use.

Port of ``deployment/lib/PlanExpression.ps1``: ``|| && ! == != < <= > >= + - * / %`` and
parentheses over ``@[User::X]`` / ``@[$Package::X]`` references, numeric literals, string
literals and ``True`` / ``False``. Anything else raises rather than silently taking an edge.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

_TOKEN = re.compile(
    r"(?P<space>\s+)"
    r"|(?P<variable>@\[[$]?[A-Za-z0-9_]+::[A-Za-z0-9_]+\])"
    r"|(?P<number>\d+(\.\d+)?)"
    r"|(?P<string>\"([^\"\\]|\\.)*\")"
    r"|(?P<operator><=|>=|==|!=|&&|\|\||[!<>+\-*/%()])"
    r"|(?P<word>[A-Za-z_][A-Za-z0-9_]*)"
)
_ASSIGNMENT = re.compile(r"^\s*@\[(?P<name>[$]?[A-Za-z0-9_]+::[A-Za-z0-9_]+)\]\s*=\s*(?P<value>.+?)\s*$")
_VARIABLE = re.compile(r"@\[([$]?[A-Za-z0-9_]+::[A-Za-z0-9_]+)\]")


class PlanExpressionError(ValueError):
    """The expression uses syntax or variables the plan does not support."""


def planValue(value: Any) -> Any:
    """Typed value of a plan literal: ``"True"``/``"False"`` are booleans (SSIS writes them so)."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value == "True":
            return True
        if value == "False":
            return False
        return value
    if isinstance(value, (int, float)):
        return value
    return str(value)


def variableTable(root: Dict[str, Any]) -> Dict[str, Any]:
    """``{"$Package::Name": default, "User::Name": default}`` for one plan root."""
    table: Dict[str, Any] = {}
    for name, value in (root.get("parameters") or {}).items():
        table["$Package::" + name] = planValue(value)
    for name, value in (root.get("variables") or {}).items():
        table["User::" + name] = planValue(value)
    return table


def referencedVariables(expression: str) -> List[str]:
    """Variable names (``User::X`` / ``$Package::X``) an expression or assignment reads."""
    return list(dict.fromkeys(_VARIABLE.findall(expression)))


def tokenize(expression: str) -> List[Tuple[str, str]]:
    tokens: List[Tuple[str, str]] = []
    position = 0
    while position < len(expression):
        m = _TOKEN.match(expression, position)
        if m is None:
            raise PlanExpressionError(
                f"cannot evaluate the SSIS expression '{expression}': unsupported syntax at offset {position}")
        position = m.end()
        kind = m.lastgroup or ""
        if kind == "space":
            continue
        tokens.append((kind, m.group(0)))
    return tokens


def evaluate(expression: str, variables: Dict[str, Any]) -> Any:
    """Evaluate one SSIS expression against a variable table. Unknown variables raise."""
    tokens = tokenize(expression)
    cursor = [0]

    def fail(message: str) -> None:
        raise PlanExpressionError(f"cannot evaluate the SSIS expression '{expression}': {message}")

    def peek() -> Tuple[str, str] | None:
        return tokens[cursor[0]] if cursor[0] < len(tokens) else None

    def nxt() -> Tuple[str, str] | None:
        token = peek()
        cursor[0] += 1
        return token

    def readOperator(*text: str) -> str | None:
        token = peek()
        if token and token[0] == "operator" and token[1] in text:
            cursor[0] += 1
            return token[1]
        return None

    def readPrimary() -> Any:
        token = nxt()
        if token is None:
            fail("the expression ends where a value was expected")
        kind, text = token  # type: ignore[misc]
        if kind == "variable":
            name = text[2:-1]
            if name not in variables:
                fail(f"it reads {text}, which this root's plan does not declare")
            return variables[name]
        if kind == "number":
            return float(text) if "." in text else int(text)
        if kind == "string":
            return re.sub(r"\\(.)", r"\1", text[1:-1])
        if kind == "word":
            if text.lower() == "true":
                return True
            if text.lower() == "false":
                return False
            fail(f"it uses the identifier '{text}', which this evaluator does not model")
        if text == "(":
            value = readOr()
            if not readOperator(")"):
                fail("an opening parenthesis is never closed")
            return value
        fail(f"it starts a value with '{text}'")

    def readUnary() -> Any:
        if readOperator("!"):
            value = readUnary()
            if not isinstance(value, bool):
                fail("! was applied to something that is not a boolean")
            return not value
        if readOperator("-"):
            return 0 - readUnary()
        return readPrimary()

    def readMultiplicative() -> Any:
        left = readUnary()
        while True:
            op = readOperator("*", "/", "%")
            if op is None:
                return left
            right = readUnary()
            left = left * right if op == "*" else left / right if op == "/" else left % right

    def readAdditive() -> Any:
        left = readMultiplicative()
        while True:
            op = readOperator("+", "-")
            if op is None:
                return left
            right = readMultiplicative()
            left = left + right if op == "+" else left - right

    def readComparison() -> Any:
        left = readAdditive()
        op = readOperator("==", "!=", "<=", ">=", "<", ">")
        if op is None:
            return left
        right = readAdditive()
        return {"==": left == right, "!=": left != right, "<": left < right,
                "<=": left <= right, ">": left > right, ">=": left >= right}[op]

    def readAnd() -> Any:
        left = readComparison()
        while readOperator("&&"):
            right = readComparison()
            if not isinstance(left, bool) or not isinstance(right, bool):
                fail("&& was applied to something that is not a boolean")
            left = left and right
        return left

    def readOr() -> Any:
        left = readAnd()
        while readOperator("||"):
            right = readAnd()
            if not isinstance(left, bool) or not isinstance(right, bool):
                fail("|| was applied to something that is not a boolean")
            left = left or right
        return left

    value = readOr()
    if cursor[0] != len(tokens):
        fail(f"it has trailing input from '{tokens[cursor[0]][1]}'")
    return value


def test(expression: str, variables: Dict[str, Any]) -> bool:
    """Boolean verdict of a precedence expression; a non-boolean result is a plan defect."""
    value = evaluate(expression, variables)
    if not isinstance(value, bool):
        raise PlanExpressionError(
            f"the precedence expression '{expression}' evaluated to '{value}', which is not a boolean")
    return value


def parseAssignment(assignment: str) -> Tuple[str, str]:
    """``"@[User::X] = <expr>" -> ("User::X", "<expr>")``."""
    m = _ASSIGNMENT.match(assignment)
    if m is None:
        raise PlanExpressionError(f"'{assignment}' is not an SSIS variable assignment")
    return m.group("name"), m.group("value")


def assign(assignment: str, variables: Dict[str, Any]) -> Tuple[str, Any]:
    """Apply an Expression Task assignment to the table; returns ``(name, newValue)``."""
    name, expression = parseAssignment(assignment)
    if name not in variables:
        raise PlanExpressionError(f"'{assignment}' assigns {name}, which this root's plan does not declare")
    value = evaluate(expression, variables)
    variables[name] = value
    return name, value


def coerce(text: Any, default: Any) -> Any:
    """Coerce a job/task parameter string to the type of the plan default it replaces.

    Unresolved dynamic value references (``{{tasks.x.values.y}}`` of a task that did not run) and
    empty strings fall back to the default.
    """
    if text is None:
        return default
    if not isinstance(text, str):
        return text
    if text == "" or text.startswith("{{"):
        return default
    if isinstance(default, bool):
        return text.strip().lower() in ("true", "1", "yes", "y")
    if isinstance(default, int):
        try:
            return int(float(text))
        except ValueError:
            return default
    if isinstance(default, float):
        try:
            return float(text)
        except ValueError:
            return default
    return text
