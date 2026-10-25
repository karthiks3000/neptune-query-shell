"""The shipped example schema loads and renders every prompt template."""

import json
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

from agents.ai_query_generator import load_schema
from core.enums import QueryLanguage

ROOT = Path(__file__).resolve().parent.parent


def test_example_schema_is_used_when_no_user_schema(tmp_path):
    (tmp_path / "example_schema.json").write_text(json.dumps({"database_info": {"name": "Example"}}))
    assert load_schema(tmp_path)["database_info"]["name"] == "Example"


def test_user_schema_wins_over_example(tmp_path):
    (tmp_path / "example_schema.json").write_text(json.dumps({"database_info": {"name": "Example"}}))
    (tmp_path / "user_schema.json").write_text(json.dumps({"database_info": {"name": "Mine"}}))
    assert load_schema(tmp_path)["database_info"]["name"] == "Mine"


def test_missing_schemas_raise(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_schema(tmp_path)


@pytest.mark.parametrize("language", list(QueryLanguage))
def test_templates_render_with_example_schema(language):
    schema = json.loads((ROOT / "schema" / "example_schema.json").read_text())
    # Same settings as BaseNeptuneAgent._setup_jinja
    env = Environment(loader=FileSystemLoader(str(ROOT / "templates")), trim_blocks=True, lstrip_blocks=True)
    instructions = env.get_template(f"query_languages/{language.value}_instructions.j2").render(schema=schema)
    prompt = env.get_template("system_prompts/base_system.j2").render(
        schema=schema,
        current_language=language.value.upper(),
        language_specific_instructions=instructions,
    )
    assert schema["database_info"]["name"] in prompt
    assert schema["query_examples"][language.value][0]["query"] in prompt
