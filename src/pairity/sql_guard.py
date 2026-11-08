"""Static, conservative validation for user-authored read-only SQL sources.

This is not a SQL parser. It accepts a documented subset of Spark SQL:

* exactly one ``SELECT``/``WITH`` statement (one trailing ``;`` is ignored);
* relations only through ``{{input}}`` placeholders, subqueries, ``VALUES`` and
  CTE names defined in the same query, so every read goes through a declared,
  pinned and authorized input;
* scalar values only through named ``:parameter`` markers bound by the engine;
* no write/DDL/session commands, table-valued or external-reading functions,
  time travel, variable substitution or nondeterministic functions.

Anything outside the subset is rejected with a reason rather than guessed at.
Comparison results stay reproducible because the query is a pure function of its
pinned inputs and recorded parameter values.
"""

import hashlib
import re
from dataclasses import dataclass

MAX_QUERY_LENGTH = 20_000

_TOKEN = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<line_comment>--[^\n]*)
  | (?P<block_comment>/\*.*?\*/)
  | (?P<placeholder>\{\{\s*(?P<input>[A-Za-z_][A-Za-z0-9_]*)\s*\}\})
  | (?P<string>[rR]?'(?:\\.|''|[^'\\])*'|[rR]?"(?:\\.|""|[^"\\])*")
  | (?P<quoted>`(?:``|[^`])*`)
  | (?P<number>\d+(?:\.\d*)?(?:[eE][+-]?\d+)?[a-zA-Z]*|\.\d+(?:[eE][+-]?\d+)?[a-zA-Z]*)
  | (?P<word>[A-Za-z_][A-Za-z0-9_]*)
  | (?P<punct>::|<=>|<=|>=|<>|!=|==|\|\||->|=>|[(),.;:+\-*/%<>=!~&|^\[\]{}?$@\#])
    """,
    re.VERBOSE | re.DOTALL,
)

FORBIDDEN_WORDS = {
    # Writes, DDL, session state and procedures.
    "insert",
    "update",
    "delete",
    "merge",
    "create",
    "drop",
    "alter",
    "truncate",
    "grant",
    "revoke",
    "deny",
    "copy",
    "optimize",
    "vacuum",
    "refresh",
    "cache",
    "uncache",
    "set",
    "reset",
    "use",
    "call",
    "execute",
    "exec",
    "msck",
    "repair",
    "load",
    "analyze",
    "declare",
    "add",
    "export",
    "restore",
    # Dynamic or sampled relations.
    "identifier",
    "tablesample",
    "stream",
    # The TABLE query term reads a catalog table directly.
    "table",
}

FORBIDDEN_FUNCTIONS = {
    # Read external data, secrets or metadata outside the declared inputs.
    "read_files",
    "read_kafka",
    "read_kinesis",
    "read_pubsub",
    "read_pulsar",
    "read_state_metadata",
    "read_statestore",
    "table_changes",
    "cloud_files_state",
    "event_log",
    "secret",
    "list_secrets",
    "http_request",
    "ai_query",
    "vector_search",
    "java_method",
    "reflect",
    "try_reflect",
    "input_file_name",
    "input_file_block_start",
    "input_file_block_length",
    "current_catalog",
    "current_schema",
    "current_database",
    "current_metastore",
    "current_version",
    "is_member",
    "current_timezone",
    "is_account_group_member",
    # Nondeterministic.
    "rand",
    "randn",
    "random",
    "uniform",
    "randstr",
    "uuid",
    "shuffle",
    "now",
    "curdate",
    "monotonically_increasing_id",
    "spark_partition_id",
}

_CLAUSE_END = {
    "where",
    "group",
    "having",
    "order",
    "limit",
    "offset",
    "union",
    "intersect",
    "except",
    "minus",
    "window",
    "qualify",
    "cluster",
    "distribute",
    "sort",
    "select",
}
_RELATION_KEYWORDS = {"lateral", "values"}
_QUERY_START = {"select", "with", "values", "from"}
_ARGUMENT_FROM = {"extract", "trim", "substring", "substr", "position", "overlay"}
# Spark resolves these single-part names to session values, quoted or not, when no
# column has that name.
LITERAL_FUNCTIONS = {
    "current_date",
    "current_timestamp",
    "current_time",
    "current_user",
    "user",
    "session_user",
    "localtimestamp",
}
_CREDENTIAL_NAME = re.compile(r"(?i)(password|passwd|secret|token|credential|api_?key)")


class UnsafeQuery(ValueError):
    """The query is outside the supported read-only subset."""


@dataclass(frozen=True)
class Token:
    kind: str
    text: str
    start: int
    end: int

    @property
    def lower(self):
        return self.text.lower()


@dataclass(frozen=True)
class CheckedQuery:
    inputs: tuple[str, ...]
    parameters: tuple[str, ...]


def tokenize(query: str) -> list[Token]:
    tokens, position = [], 0
    while position < len(query):
        match = _TOKEN.match(query, position)
        if not match:
            snippet = query[position : position + 20]
            raise UnsafeQuery(f"Unsupported or unterminated SQL near {snippet!r}")
        kind = match.lastgroup if match.lastgroup != "input" else "placeholder"
        tokens.append(Token(kind, match.group(0), match.start(), match.end()))
        position = match.end()
    return tokens


def _significant(tokens):
    return [t for t in tokens if t.kind not in {"ws", "line_comment", "block_comment"}]


def _check_words(tokens):
    for index, token in enumerate(tokens):
        if token.kind == "punct" and token.text in {"$", "?", "{", "}", "@", "#"}:
            raise UnsafeQuery(
                f"{token.text!r} is not supported: use {{{{input}}}} placeholders for inputs and "
                ":name markers for parameters"
            )
        if token.kind == "punct" and token.text == ";":
            raise UnsafeQuery("A SQL source must contain exactly one statement")
        if token.kind not in {"word", "quoted"}:
            continue
        following = tokens[index + 1] if index + 1 < len(tokens) else None
        previous = tokens[index - 1] if index else None
        name = _identifier(token)
        qualified = previous is not None and previous.text == "."
        if following is not None and following.text == "(":
            if qualified:
                # A catalog function can read any table its owner can, outside the inputs.
                raise UnsafeQuery(f"Qualified function call ...{token.text}() is not allowed")
            if name in FORBIDDEN_FUNCTIONS or name in LITERAL_FUNCTIONS:
                raise UnsafeQuery(f"Function {token.text}() is not allowed in a SQL source")
        if qualified:
            continue  # A struct field or qualified column name, never a keyword.
        if name in LITERAL_FUNCTIONS:
            # Spark resolves these single-part names to session values even when quoted.
            raise UnsafeQuery(
                f"{token.text} resolves to a session value; qualify a column with this name "
                "by its relation alias (for example o.user)"
            )
        if token.kind == "quoted":
            continue  # Quoting makes a keyword an ordinary identifier.
        if token.lower in FORBIDDEN_WORDS:
            raise UnsafeQuery(f"{token.text.upper()} is not allowed in a read-only SQL source")
        if (
            token.lower == "as"
            and following is not None
            and following.lower == "of"
            and previous is not None
            and previous.lower in {"version", "timestamp", "system_time", "system_version"}
        ):
            raise UnsafeQuery("Time travel is not allowed; pin versions on the declared inputs")


def function_calls(query: str) -> list[str]:
    """Unqualified names written as calls, for runtime checks against the function catalog."""
    tokens = _significant(tokenize(query))
    return sorted(
        {
            _identifier(token)
            for index, token in enumerate(tokens[:-1])
            if token.kind in {"word", "quoted"}
            and tokens[index + 1].text == "("
            and not (index and tokens[index - 1].text == ".")
        }
    )


def _groups(tokens):
    """Paren-group chain for every token, and each '(' index's matching ')' index."""
    chains, matches, stack, opened, next_id = [], {}, [0], [], 1
    for index, token in enumerate(tokens):
        if token.text == ")" and len(stack) > 1:
            stack.pop()
            matches[opened.pop()] = index
        chains.append(tuple(stack))
        if token.text == "(":
            stack.append(next_id)
            opened.append(index)
            next_id += 1
    return chains, matches


def _ctes(tokens, chains, matches):
    """CTE definitions as (name, scope group, index where the body closes)."""
    ctes = []
    for index, token in enumerate(tokens[:-2]):
        previous = tokens[index - 1].lower if index else ""
        if (
            token.kind in {"word", "quoted"}
            and previous in {"with", "recursive", ","}
            and tokens[index + 1].lower == "as"
            and tokens[index + 2].text == "("
        ):
            ctes.append(
                (_identifier(token), chains[index][-1], matches.get(index + 2, len(tokens)))
            )
    return ctes


def _cte_visible(name, index, chains, ctes):
    # Spark 3.5 resolves a CTE name used before its definition, inside its own body or
    # outside the defining scope as a catalog table, so those references are rejected.
    return any(
        cte == name and scope in chains[index] and index > closes for cte, scope, closes in ctes
    )


def _identifier(token):
    if token.kind == "quoted":
        return token.text[1:-1].replace("``", "`").lower()
    return token.lower


def _check_relations(tokens):
    """Check every relation position.

    FROM starts a relation clause in every parenthesis context (Spark accepts FROM-first
    subqueries such as ``(FROM t SELECT ...)``) except directly inside the functions whose
    syntax uses FROM for an argument. A parenthesis in a relation position that does not
    open a query is a parenthesized join, and its contents are relation positions too.
    """
    chains, matches = _groups(tokens)
    ctes = _ctes(tokens, chains, matches)
    argument_from = [False]  # Innermost paren belongs to EXTRACT/TRIM/SUBSTRING/...
    in_from = [False]
    expect_relation = False
    for index, token in enumerate(tokens):
        previous = tokens[index - 1] if index else None
        following = tokens[index + 1] if index + 1 < len(tokens) else None
        if expect_relation:
            expect_relation = False
            if token.text == "(":
                opens_query = following is not None and following.lower in _QUERY_START
                argument_from.append(False)
                in_from.append(not opens_query)
                expect_relation = not opens_query
                continue
            if token.kind == "placeholder" or token.lower in _RELATION_KEYWORDS:
                pass
            elif token.kind in {"word", "quoted"}:
                if following is not None and following.text == "(":
                    raise UnsafeQuery(
                        f"Table-valued function {token.text}() is not allowed; "
                        "read data only through declared inputs"
                    )
                if following is not None and following.text == ".":
                    raise UnsafeQuery(
                        f"Relation {token.text}... must be a declared input placeholder"
                    )
                if not _cte_visible(_identifier(token), index, chains, ctes):
                    raise UnsafeQuery(
                        f"Relation {token.text} is not a declared input or a CTE defined earlier "
                        "in scope; reference inputs as {{name}}"
                    )
            else:
                raise UnsafeQuery(f"Unsupported relation syntax near {token.text!r}")
        if token.text == "(":
            argument_from.append(previous is not None and _identifier(previous) in _ARGUMENT_FROM)
            in_from.append(False)
            continue
        if token.text == ")":
            if len(in_from) == 1:
                raise UnsafeQuery("Unbalanced parentheses")
            argument_from.pop()
            in_from.pop()
            continue
        if argument_from[-1]:
            continue  # EXTRACT(YEAR FROM d), TRIM(BOTH ' ' FROM s): FROM is an argument.
        if token.lower in {"from", "join"}:
            in_from[-1] = True
            expect_relation = True
        elif token.lower in _CLAUSE_END:
            in_from[-1] = False
        elif token.lower == "view" and previous is not None and previous.lower == "lateral":
            # Spark's grammar places LATERAL VIEW after every relation of the clause, so the
            # commas that follow separate column aliases, never relations.
            in_from[-1] = False
        elif token.text == "," and in_from[-1]:
            expect_relation = True
    if len(in_from) != 1:
        raise UnsafeQuery("Unbalanced parentheses")
    if expect_relation:
        raise UnsafeQuery("Query ends where a relation is expected")


def _parameters(tokens):
    names = []
    for index, token in enumerate(tokens):
        if token.text != ":":
            continue
        previous = tokens[index - 1] if index else None
        following = tokens[index + 1] if index + 1 < len(tokens) else None
        attached = (
            previous is not None
            and previous.end == token.start
            and (
                previous.kind in {"word", "quoted", "number", "placeholder"}
                or previous.text in {")", "]"}
            )
        )
        if attached:
            continue  # Semi-structured path access such as raw:field (no whitespace).
        if following is None or following.kind != "word" or following.start != token.end:
            raise UnsafeQuery("A parameter marker must be :name")
        names.append(following.text)
    return names


def check_query(query: str) -> CheckedQuery:
    if not isinstance(query, str) or not query.strip():
        raise UnsafeQuery("A SQL source requires a query")
    if len(query) > MAX_QUERY_LENGTH:
        raise UnsafeQuery(f"A SQL source query is limited to {MAX_QUERY_LENGTH} characters")
    if "\x00" in query:
        raise UnsafeQuery("NUL characters are not allowed")
    if "${" in query:
        # Spark variable substitution rewrites the raw text, including string literals.
        raise UnsafeQuery("${...} variable substitution is not allowed anywhere in a SQL source")
    tokens = _significant(tokenize(query))
    if tokens and tokens[-1].text == ";":
        tokens = tokens[:-1]
    first = next((t for t in tokens if t.text != "("), None)
    if first is None or first.lower not in {"select", "with"}:
        raise UnsafeQuery("A SQL source must be a single SELECT or WITH query")
    _check_words(tokens)
    _check_relations(tokens)
    inputs = [t.text[2:-2].strip() for t in tokens if t.kind == "placeholder"]
    return CheckedQuery(tuple(dict.fromkeys(inputs)), tuple(dict.fromkeys(_parameters(tokens))))


def check_parameter_name(name: str) -> None:
    if _CREDENTIAL_NAME.search(name):
        raise UnsafeQuery(
            f"Parameter {name} looks like a credential; SQL source parameters are recorded "
            "in results and must not carry secrets"
        )


def render(query: str, relations: dict[str, str]) -> str:
    """Replace each placeholder with a trusted relation string; nothing else changes.

    The optional trailing ``;`` is dropped and a trailing line comment is terminated, so
    the result can be embedded in a larger statement.
    """
    tokens = tokenize(query)
    significant = [
        i for i, t in enumerate(tokens) if t.kind not in {"ws", "line_comment", "block_comment"}
    ]
    if significant and tokens[significant[-1]].text == ";":
        tokens.pop(significant[-1])
    parts = []
    for token in tokens:
        if token.kind == "placeholder":
            parts.append(relations[token.text[2:-2].strip()])
        else:
            parts.append(token.text)
    rendered = "".join(parts).rstrip()
    last = next((t for t in reversed(tokens) if t.kind != "ws"), None)
    return rendered + "\n" if last is not None and last.kind == "line_comment" else rendered


def query_hash(query: str) -> str:
    return hashlib.sha256(query.encode()).hexdigest()
