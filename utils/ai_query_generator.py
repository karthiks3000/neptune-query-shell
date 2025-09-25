#!/usr/bin/env python3
"""AI-powered query generator using Strands Agent SDK with configurable schema."""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field

from .base_neptune_agent import BaseNeptuneAgent, QueryLanguage
from strands import tool


class QueryResult(BaseModel):
    """Structured response from Neptune query execution."""
    query: str = Field(description="The executed query")
    query_language: str = Field(description="Query language used (sparql/gremlin/opencypher)")
    explanation: str = Field(description="Clear explanation of what the query does")
    results: List[Dict[str, Any]] = Field(description="Query results from Neptune database")
    result_count: int = Field(description="Number of results returned")
    display_format: str = Field(default="table", description="Display format: table|network|tree")
    display_config: Optional[Dict[str, Any]] = Field(default=None, description="Visualization configuration with field mappings")
    insights: Optional[str] = Field(default=None, description="AI insights about the results and data patterns")
    suggestions: Optional[List[str]] = Field(default=None, description="Suggestions for follow-up queries or exploration")


class AIQueryGenerator(BaseNeptuneAgent):
    """AI-powered query generator with configurable schema and multi-language support."""
    
    def __init__(self, neptune_client, query_language: QueryLanguage = QueryLanguage.SPARQL):
        """Initialize the AI query generator.
        
        Args:
            neptune_client: NeptuneClient instance for query execution
            query_language: Target query language
        """
        self.schema = self._load_schema()
        self.last_query_results = []
        self.last_query_info = {}
        super().__init__(neptune_client, query_language)
    
    def _load_schema(self) -> Dict[str, Any]:
        """Load user schema configuration."""
        schema_path = Path(__file__).parent.parent / "schema" / "user_schema.json"
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_path, 'r') as f:
            return json.load(f)
    
    def _load_language_instructions(self) -> str:
        """Load query language specific instructions."""
        template_name = f"query_languages/{self.query_language.value}_instructions.j2"
        
        try:
            template = self.jinja_env.get_template(template_name)
            return template.render(schema=self.schema)
        except Exception as e:
            return f"# {self.query_language.value.upper()} instructions not available: {e}"
    
    def _create_system_prompt(self) -> str:
        """Create dynamic system prompt using Jinja templates."""
        try:
            base_template = self.jinja_env.get_template("system_prompts/base_system.j2")
            language_instructions = self._load_language_instructions()
            
            return base_template.render(
                schema=self.schema,
                current_language=self.query_language.value.upper(),
                language_specific_instructions=language_instructions
            )
        except Exception as e:
            return f"Error creating system prompt: {e}"
    
    def _get_additional_tools(self) -> List:
        """Get additional tools beyond execute_neptune_query."""
        return [self.export_to_csv]
    
    def _get_max_tokens(self) -> int:
        """Get maximum tokens for the agent."""
        return 4096
    
    def _process_query_results(self, query: str, query_language: str, 
                             results: List[Dict[str, Any]], raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Process query results for AI query generation (with truncation)."""
        # Store ALL results for CSV export (complete dataset)
        self.last_query_results = results
        self.last_query_info = {
            "query": query,
            "query_language": query_language,
            "timestamp": self._get_timestamp()
        }
        
        # Truncate results for AI to prevent context window overflow
        truncated_results = results[:10] if len(results) > 10 else results
        is_truncated = len(results) > 10
        
        return {
            "success": True,
            "query": query,
            "query_language": query_language,
            "results": truncated_results,  # Only first 10 records for AI analysis
            "result_count": len(results),  # Total count so AI knows full size
            "truncated": is_truncated,     # AI knows data was truncated
            "sample_size": len(truncated_results),  # AI knows sample size
            "execution_metadata": {
                "status": raw_result.get("status", "success"),
                "code": raw_result.get("code", 200)
            }
        }

    @tool 
    async def export_to_csv(self, filename: Optional[str] = None, description: str = "query results") -> Dict[str, Any]:
        """Export the last query results to CSV file.
        
        Args:
            filename: Optional custom filename (without extension)
            description: Description of what's being exported
            
        Returns:
            Export status and file information
        """
        if not hasattr(self, 'last_query_results') or not self.last_query_results:
            return {
                "success": False,
                "error": "No query results available to export. Execute a query first."
            }
        
        try:
            # Generate filename if not provided
            if filename is None:
                timestamp = self._get_timestamp()
                filename = f"{description.replace(' ', '_')}_{timestamp}"
            
            # Import CSV exporter
            from utils.csv_exporter import NeptuneCSVExporter
            csv_exporter = NeptuneCSVExporter()
            
            # Export results
            filepath = csv_exporter.export_results(
                self.last_query_results,
                description,
                f"{filename}.csv"
            )
            
            # Get export info
            export_info = csv_exporter.get_export_info(f"{filename}.csv")
            
            return {
                "success": True,
                "filepath": filepath,
                "filename": f"{filename}.csv",
                "record_count": len(self.last_query_results),
                "file_size_mb": export_info.get('size_mb', 0) if export_info else 0,
                "message": f"Successfully exported {len(self.last_query_results)} records to {filepath}"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Export failed: {str(e)}"
            }
    
    async def process_natural_language_query(self, natural_query: str) -> QueryResult:
        """Process natural language query and return structured results.
        
        Args:
            natural_query: User's natural language description
            
        Returns:
            Structured QueryResult with query, execution results, and insights
        """
        # Create conversation prompt that enforces tool usage
        conversation_prompt = f"""
The user wants to query the Neptune database with this natural language request:

"{natural_query}"

You MUST follow this process:
1. Generate an appropriate {self.query_language.value.upper()} query for this request
2. Execute the query using the execute_neptune_query tool to get REAL results
3. Analyze the ACTUAL results returned by the tool
4. Respond in JSON format with the real data

Do NOT make up or fabricate any results. Use only the actual data returned by the execute_neptune_query tool.
"""
        
        try:
            # Use regular agent conversation to force tool usage
            agent_result = await self.agent.invoke_async(conversation_prompt)
            
            # Extract text from Message object
            response_text = self._extract_message_text(agent_result)
            
            # Extract JSON from response
            result_data = self._extract_json_from_response(response_text)
            
            # Create QueryResult from parsed JSON
            return QueryResult(
                query=result_data.get("query", ""),
                query_language=result_data.get("query_language", self.query_language.value),
                explanation=result_data.get("explanation", ""),
                results=result_data.get("results", []),
                result_count=result_data.get("result_count", 0),
                display_format=result_data.get("display_format", "table"),
                display_config=result_data.get("display_config"),
                insights=result_data.get("insights"),
                suggestions=result_data.get("suggestions")
            )
            
        except Exception as e:
            # Return error result in structured format
            return QueryResult(
                query="",
                query_language=self.query_language.value,
                explanation=f"Failed to process query: {str(e)}",
                results=[],
                result_count=0,
                insights="Query generation failed due to an error",
                suggestions=["Try rephrasing your request", "Check database connectivity"]
            )
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get current schema information.
        
        Returns:
            Schema configuration dictionary
        """
        return self.schema
