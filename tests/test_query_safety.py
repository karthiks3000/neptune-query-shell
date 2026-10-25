"""AI-generated queries must not change data unless the user opts in."""

import pytest

from core.enums import QueryLanguage
from core.query_safety import blocked_ai_query_reason, is_write_query

SPARQL = QueryLanguage.SPARQL
GREMLIN = QueryLanguage.GREMLIN
CYPHER = QueryLanguage.OPENCYPHER

READS = [
    (SPARQL, "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10"),
    (SPARQL, "ASK { ?s ?p ?o }"),
    # Keywords inside strings, IRIs, variables, prefixed names and comments are not updates
    (SPARQL, 'SELECT ?a WHERE { ?a <http://example.org/property/desc> "Delete me" }'),
    (SPARQL, "SELECT ?s WHERE { ?s <http://example.org/insert/x> ?o }"),
    (SPARQL, "SELECT ?delete WHERE { ?delete ?p ?o }"),
    (SPARQL, "PREFIX ex: <http://example.org/> SELECT ?s WHERE { ?s ex:add ?o }"),
    (SPARQL, "# don't drop anything\nSELECT ?s WHERE { ?s ?p ?o }"),
    (SPARQL, "SELECT ?s WHERE { ?s ?p ?o FILTER(?o < 5 && ?o > 1) }"),
    (GREMLIN, "g.V().hasLabel('airport').properties('code').limit(5)"),
    (GREMLIN, "g.V().has('desc', 'drop zone').valueMap()"),
    (CYPHER, "MATCH (a:airport) WHERE a.code = 'SET' RETURN a.code"),
    (CYPHER, "MATCH (n) RETURN n.set LIMIT 5"),
    (CYPHER, "MATCH (n:`Create`) RETURN n // delete later"),
]

WRITES = [
    (SPARQL, 'INSERT DATA { <http://example.org/a> <http://example.org/b> "c" }'),
    (SPARQL, "DELETE WHERE { ?s ?p ?o }"),
    (SPARQL, "prefix ex: <http://example.org/> delete { ?s ex:p ?o } where { ?s ex:p ?o }"),
    (SPARQL, "CLEAR ALL"),
    (SPARQL, "DROP GRAPH <http://example.org/g>"),
    (SPARQL, "LOAD <http://example.org/data.ttl>"),
    (SPARQL, "# harmless\nCLEAR DEFAULT"),
    (GREMLIN, "g.V().drop()"),
    (GREMLIN, "g.addV('airport').property('code', 'XYZ')"),
    (GREMLIN, "g.V('1').addE('route').to(__.V('2'))"),
    (GREMLIN, "g.V('1') . property ('city', 'Austin')"),
    (GREMLIN, "g.mergeV([(T.label): 'airport'])"),
    (CYPHER, "CREATE (a:airport {code: 'XYZ'})"),
    (CYPHER, "MATCH (n) DETACH DELETE n"),
    (CYPHER, "MATCH (a:airport {code: 'AUS'}) SET a.city = 'Austin'"),
    (CYPHER, "MERGE (a:airport {code: 'XYZ'})"),
    (CYPHER, "MATCH (a) REMOVE a.city"),
]


@pytest.mark.parametrize("language,query", READS)
def test_read_queries_are_not_writes(language, query):
    assert is_write_query(query, language) is False


@pytest.mark.parametrize("language,query", WRITES)
def test_write_queries_are_detected(language, query):
    assert is_write_query(query, language) is True


def test_ai_write_is_blocked_by_default(monkeypatch):
    monkeypatch.delenv("ALLOW_AI_WRITES", raising=False)
    reason = blocked_ai_query_reason("MATCH (n) DETACH DELETE n", CYPHER)
    assert reason is not None
    assert "ALLOW_AI_WRITES" in reason


def test_ai_read_is_never_blocked(monkeypatch):
    monkeypatch.delenv("ALLOW_AI_WRITES", raising=False)
    assert blocked_ai_query_reason("MATCH (n) RETURN n LIMIT 1", CYPHER) is None


@pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes"])
def test_ai_write_allowed_when_opted_in(monkeypatch, value):
    monkeypatch.setenv("ALLOW_AI_WRITES", value)
    assert blocked_ai_query_reason("g.V().drop()", GREMLIN) is None


def test_opt_in_needs_a_true_value(monkeypatch):
    monkeypatch.setenv("ALLOW_AI_WRITES", "false")
    assert blocked_ai_query_reason("g.V().drop()", GREMLIN) is not None
