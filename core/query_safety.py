#!/usr/bin/env python3
"""Keeps AI-generated queries read-only unless the user opts in.

The AI agent writes and runs its own queries, and it reads data from the
database while doing so. A prompt, or text stored in the graph, could steer it
into deleting or changing data. Queries that change data are refused on the AI
path unless ALLOW_AI_WRITES is set. Queries the user types into "Execute Your
Query" are not checked.

The check is keyword based and errs towards refusing: a read query that uses a
write keyword outside a string, IRI or comment is refused, and the user can run
it from "Execute Your Query" instead. For a hard guarantee, run the shell with
IAM credentials that only allow neptune-db:ReadDataViaQuery.
"""

import os
import re
from typing import Optional

from core.enums import QueryLanguage

# Single-line strings may not span lines, so an apostrophe in a comment
# can't hide the code that follows it.
_STRINGS = re.compile(
    r'"""[\s\S]*?"""'
    r"|'''[\s\S]*?'''"
    r'|"(?:\\.|[^"\\\n])*"'
    r"|'(?:\\.|[^'\\\n])*'"
)
_IRIS = re.compile(r"<[^<>\s]*>")
_HASH_COMMENTS = re.compile(r"#[^\n]*")
_SLASH_COMMENTS = re.compile(r"//[^\n]*|/\*[\s\S]*?\*/")
_BACKTICKS = re.compile(r"`[^`]*`")

# SPARQL 1.1 Update operations. Variables (?x, $x) and prefixed names (ex:add) are skipped.
_SPARQL_UPDATE = re.compile(
    r"(?<![\w?$:])(INSERT|DELETE|LOAD|CLEAR|CREATE|DROP|COPY|MOVE|ADD)\b", re.IGNORECASE
)
# Gremlin steps that add, change or remove vertices, edges or properties.
_GREMLIN_WRITE = re.compile(r"(?<![\w$])(addV|addE|mergeV|mergeE|drop|property)\s*\(")
# openCypher clauses that change data. Property access (n.set) is skipped.
_CYPHER_WRITE = re.compile(
    r"(?<![\w.$])(CREATE|MERGE|DELETE|DETACH|SET|REMOVE)\b", re.IGNORECASE
)


def is_write_query(query: str, language: QueryLanguage) -> bool:
    """Return True if the query would add, change or delete data."""
    code = _STRINGS.sub(" ", query)
    if language == QueryLanguage.SPARQL:
        code = _HASH_COMMENTS.sub(" ", _IRIS.sub(" ", code))
        return bool(_SPARQL_UPDATE.search(code))
    if language == QueryLanguage.GREMLIN:
        code = _SLASH_COMMENTS.sub(" ", code)
        return bool(_GREMLIN_WRITE.search(code))
    if language == QueryLanguage.OPENCYPHER:
        code = _SLASH_COMMENTS.sub(" ", _BACKTICKS.sub(" ", code))
        return bool(_CYPHER_WRITE.search(code))
    raise ValueError(f"Unsupported query language: {language}")


def ai_writes_allowed() -> bool:
    """Return True if the user has set ALLOW_AI_WRITES to a true value."""
    return os.getenv("ALLOW_AI_WRITES", "").strip().lower() in ("1", "true", "yes")


def blocked_ai_query_reason(query: str, language: QueryLanguage) -> Optional[str]:
    """Return why an AI-generated query was refused, or None if it may run."""
    if ai_writes_allowed() or not is_write_query(query, language):
        return None
    return (
        "Refused: this query changes data, and AI-generated queries are read-only. "
        "Show the query to the user so they can run it from 'Execute Your Query', "
        "or the user can set ALLOW_AI_WRITES=true to let the AI change data."
    )
