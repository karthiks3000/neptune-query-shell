#!/usr/bin/env python3
"""AI agent for discovering Neptune database schemas and generating user_schema.json."""

import json
from pathlib import Path
from typing import Dict, List, Any

from .base_agent import BaseNeptuneAgent
from core.enums import QueryLanguage


class SchemaDiscoveryAgent(BaseNeptuneAgent):
    """AI agent specialized in discovering Neptune database schemas."""
    
    def __init__(self, neptune_client, query_language: QueryLanguage):
        """Initialize the schema discovery agent.
        
        Args:
            neptune_client: NeptuneClient instance for query execution
            query_language: Target query language for discovery
        """
        super().__init__(neptune_client, query_language)
    
    def _create_system_prompt(self) -> str:
        """Create discovery-specific system prompt."""
        return f"""You are a Neptune database schema discovery specialist. Your mission is to explore a {self.query_language.value.upper()} database and generate a complete user_schema.json configuration file.

## Your Task
Systematically discover the database structure by executing queries and analyzing the results to create a comprehensive schema file.

## Discovery Process for {self.query_language.value.upper()}

{self._get_language_specific_instructions()}

## Required Output Format
After discovering the database structure, generate a complete user_schema.json file with this format:

```json
{{
  "database_info": {{
    "name": "Discovered Database Name",
    "description": "Brief description based on what you found",
    "query_languages_supported": ["{self.query_language.value.upper()}"]
  }},
  "vertices": [
    {{
      "label": "EntityName",
      "description": "Description of entity based on discovered data",
      "properties": {{
        "property_name": {{
          "type": "string|number|boolean",
          "description": "Purpose of this property",
          "examples": ["example1", "example2"]
        }}
      }}
    }}
  ],
  "edges": [
    {{
      "label": "RELATIONSHIP_NAME",
      "description": "Description of relationship",
      "from_vertex": "SourceEntity",
      "to_vertex": "TargetEntity"
    }}
  ],{'' if self.query_language != QueryLanguage.SPARQL else '''
  "rdf_namespaces": {
    "prefix_name": "http://example.com/namespace/",
    "another_prefix": "http://example.com/other/"
  },'''}
  "query_examples": {{
    "{self.query_language.value}": [
      {{
        "description": "Example query description",
        "query": "Actual query based on discovered structure"
      }}
    ]
  }}
}}
```

## Instructions
1. Use execute_neptune_query tool to systematically explore the database
2. Start with basic discovery queries to find entity/relationship types
3. **FOR SPARQL: Execute namespace discovery queries and analyze URI patterns**
4. Sample data to understand property types and patterns
5. Generate meaningful descriptions based on actual data patterns
6. **FOR SPARQL: Include complete rdf_namespaces section with discovered URI patterns**
7. Output ONLY the final JSON schema - no additional text

CRITICAL: Use real data from Neptune queries. Never make up schema elements.
{'' if self.query_language != QueryLanguage.SPARQL else 'For SPARQL databases, the rdf_namespaces section is MANDATORY and must contain all discovered namespace mappings.'}
"""

    def _get_language_specific_instructions(self) -> str:
        """Get discovery instructions for specific query language."""
        if self.query_language == QueryLanguage.SPARQL:
            return """### SPARQL Discovery Queries

**Step 1: Find all RDF types (entities):**
```sparql
SELECT DISTINCT ?type (COUNT(?s) as ?count)
WHERE { ?s a ?type }
GROUP BY ?type
ORDER BY DESC(?count)
```

**Step 2: Find all properties:**
```sparql
SELECT DISTINCT ?property (COUNT(?s) as ?usage_count)
WHERE { ?s ?property ?o }
GROUP BY ?property
ORDER BY DESC(?usage_count)
```

**Step 3: Sample URIs for namespace discovery (CRITICAL for SPARQL):**
```sparql
SELECT DISTINCT ?s ?p ?o
WHERE { ?s ?p ?o }
LIMIT 100
```

**Step 4: Find subject URI patterns:**
```sparql
SELECT ?s
WHERE { ?s ?p ?o }
LIMIT 50
```

**Step 5: Find object URIs (for relationships):**
```sparql
SELECT ?o
WHERE { ?s ?p ?o . FILTER(isURI(?o)) }
LIMIT 50
```

**Step 6: Sample data for property types:**
```sparql
SELECT ?s ?property ?value
WHERE { 
  ?s a ?type .
  ?s ?property ?value 
}
LIMIT 50
```

### Namespace Discovery Process (MANDATORY for SPARQL)
1. **Analyze all sampled URIs** from steps 3-5
2. **Extract base patterns** - everything before the last `/` or `#`
3. **Identify common prefixes** - group related URIs together
4. **Create logical names** - e.g., "standards", "relationships", "metadata", "subjects"
5. **Include rdf_namespaces section** with all discovered namespace mappings

Example namespace extraction:
- `http://example.com/standard/12345` → `"standards": "http://example.com/standard/"`
- `http://relationship/has_alignment` → `"relationships": "http://relationship/"`
- `http://example.com/grade/6` → `"grades": "http://example.com/grade/"`

**CRITICAL**: The rdf_namespaces section is REQUIRED for SPARQL schema files."""
        
        elif self.query_language == QueryLanguage.GREMLIN:
            return """### Gremlin Discovery Queries

**Find all vertex labels:**
```gremlin
g.V().label().groupCount()
```

**Find all edge labels:**
```gremlin
g.E().label().groupCount()
```

**Sample vertex data:**
```gremlin
g.V().limit(20).valueMap()
```"""
        
        else:
            return "# Discovery instructions not available"
    
    def _get_additional_tools(self) -> List:
        """Get additional tools beyond execute_neptune_query."""
        return []  # Schema discovery only needs the base Neptune query tool
    
    def _get_max_tokens(self) -> int:
        """Get maximum tokens for the agent."""
        return 8192  # Larger context for complex schema generation
    
    def _process_query_results(self, query: str, query_language: str, 
                             results: List[Dict[str, Any]], raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Process query results for schema discovery (no truncation needed)."""
        return {
            "success": True,
            "query": query,
            "query_language": query_language,
            "results": results,  # No truncation for discovery - need all data
            "result_count": len(results)
        }
    
    async def discover_schema(self) -> bool:
        """Discover database schema and generate user_schema.json file.
        
        Returns:
            True if schema was successfully discovered and saved
        """
        discovery_prompt = f"""Please discover the structure of this Neptune database using {self.query_language.value.upper()} queries.

Follow this process:
1. Execute discovery queries to find all entity types and relationships
2. Sample data to understand property types and patterns  
3. Analyze the structure to create meaningful descriptions
4. Generate a complete user_schema.json file

Begin the systematic discovery process now."""
        
        try:
            # Use regular agent conversation to get schema discovery
            agent_result = await self.agent.invoke_async(discovery_prompt)
            
            # Extract text from Message object
            response_text = self._extract_message_text(agent_result)
            
            # Extract JSON schema from response
            schema_data = self._extract_json_from_response(response_text)
            
            # Save schema to file
            schema_path = Path(__file__).parent.parent / "schema" / "user_schema.json"
            with open(schema_path, 'w', encoding='utf-8') as f:
                json.dump(schema_data, f, indent=2, ensure_ascii=False)
            
            return True
            
        except Exception as e:
            print(f"Schema discovery failed: {str(e)}")
            return False
