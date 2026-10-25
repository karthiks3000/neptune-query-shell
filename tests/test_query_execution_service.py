"""The shared service keeps AI queries read-only and exports inside exports/."""

import asyncio
from pathlib import Path

import pytest

from core.enums import QueryLanguage
from core.services.query_execution_service import QueryExecutionService
from export.csv_exporter import NeptuneCSVExporter


class FakeNeptuneClient:
    """Records the queries it is sent and returns one row for each."""

    def __init__(self):
        self.sent = []

    async def execute_sparql(self, query):
        self.sent.append(query)
        return {"results": [{"code": "AUS", "city": "Austin"}]}

    async def execute_gremlin(self, query):
        return await self.execute_sparql(query)

    async def execute_opencypher(self, query):
        return await self.execute_sparql(query)


@pytest.fixture
def service(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ALLOW_AI_WRITES", raising=False)
    return QueryExecutionService(FakeNeptuneClient())


def run(coro):
    return asyncio.run(coro)


def test_ai_write_query_is_not_sent(service):
    result = run(service.execute_query("DELETE WHERE { ?s ?p ?o }", QueryLanguage.SPARQL, for_ai_context=True))
    assert result["success"] is False
    assert "ALLOW_AI_WRITES" in result["error"]
    assert service.neptune_client.sent == []


def test_ai_read_query_is_sent(service):
    result = run(service.execute_query("SELECT ?s WHERE { ?s ?p ?o }", QueryLanguage.SPARQL, for_ai_context=True))
    assert result["success"] is True
    assert len(service.neptune_client.sent) == 1


def test_user_typed_write_query_is_sent(service):
    # Queries typed by the user in "Execute Your Query" are theirs to run
    result = run(service.execute_query("DELETE WHERE { ?s ?p ?o }", QueryLanguage.SPARQL, for_ai_context=False))
    assert result["success"] is True
    assert len(service.neptune_client.sent) == 1


@pytest.mark.parametrize("name", ["../escape", "../../escape.csv", "..\\..\\escape", "sub/dir/escape"])
def test_export_stays_inside_exports_dir(service, tmp_path, name):
    run(service.execute_query("SELECT ?s WHERE { ?s ?p ?o }", QueryLanguage.SPARQL, for_ai_context=True))
    result = service.export_last_results("ai_query_results", name)
    assert result["success"] is True
    written = Path(result["filepath"]).resolve()
    assert written.parent == (tmp_path / "exports").resolve()
    assert result["filename"] == written.name
    assert result["file_size_mb"] >= 0
    assert not (tmp_path / "escape.csv").exists()


def test_exporter_rejects_names_outside_its_dir(tmp_path):
    exporter = NeptuneCSVExporter(output_dir=str(tmp_path / "exports"))
    path = exporter.export_results([{"a": 1}], filename=str(tmp_path / "outside.csv"))
    assert Path(path).resolve().parent == (tmp_path / "exports").resolve()
    assert not (tmp_path / "outside.csv").exists()


def test_exporter_names_hidden_or_empty_files(tmp_path):
    exporter = NeptuneCSVExporter(output_dir=str(tmp_path / "exports"))
    for name in ["..", ".csv", ".bashrc"]:
        path = Path(exporter.export_results([{"a": 1}], filename=name))
        assert not path.name.startswith(".")
        assert path.suffix == ".csv"
