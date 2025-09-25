#!/usr/bin/env python3
"""AI-powered query generator using Strands Agent SDK with configurable schema."""

import asyncio
import json
import re
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from enum import Enum
from pydantic import BaseModel, Field

from jinja2 import Environment, FileSystemLoader
from strands import Agent, tool
from strands.models import BedrockModel


class QueryLanguage(Enum):
    """Supported query languages."""
    SPARQL = "sparql"
    GREMLIN = "gremlin" 
    OPENCYPHER = "opencypher"


class QueryResult(BaseModel):
    """Structured response from Neptune query execution."""
    query: str = Field(description="The executed query")
    query_language: str = Field(description="Query language used (sparql/gremlin/opencypher)")
    explanation: str = Field(description="Clear explanation of what the query does")
    results: List[Dict[str, Any]] = Field(description="Query results from Neptune database")
    result_count: int = Field(description="Number of results returned")
    insights: Optional[str] = Field(default=None, description="AI insights about the results and data patterns")
    suggestions: Optional[List[str]] = Field(default=None, description="Suggestions for follow-up queries or exploration")


class AIQueryGenerator:
    """AI-powered query generator with configurable schema and multi-language support."""
    
    def __init__(self, neptune_client, query_language: QueryLanguage = QueryLanguage.SPARQL):
        """Initialize the AI query generator.
        
        Args:
            neptune_client: NeptuneClient instance for query execution
            query_language: Target query language
        """
        self.neptune_client = neptune_client
        self.query_language = query_language
        self.schema = self._load_schema()
        self.jinja_env = self._setup_jinja()
        self.last_query_results = []
        self.last_query_info = {}
        self.agent = self._create_agent()
    
    def _load_schema(self) -> Dict[str, Any]:
        """Load user schema configuration."""
        schema_path = Path(__file__).parent.parent / "schema" / "user_schema.json"
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with open(schema_path, 'r') as f:
            return json.load(f)
    
    def _setup_jinja(self) -> Environment:
        """Setup Jinja environment for template rendering."""
        template_dir = Path(__file__).parent.parent / "templates"
        return Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True
        )
    
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
    
    @tool
    async def execute_neptune_query(self, query: str, query_language: Optional[str] = None) -> Dict[str, Any]:
        """Execute query against Neptune database and return formatted results.
        
        Args:
            query: The query to execute
            query_language: Query language (sparql/gremlin/opencypher), defaults to current language
            
        Returns:
            Execution results with metadata for agent analysis
        """
        if query_language is None:
            query_language = self.query_language.value
        
        try:
            # Execute query using appropriate method based on language
            if query_language.lower() == "sparql":
                result = await self.neptune_client.execute_sparql(query)
            elif query_language.lower() == "gremlin":
                if hasattr(self.neptune_client, 'execute_gremlin'):
                    result = await self.neptune_client.execute_gremlin(query)
                else:
                    raise NotImplementedError("Gremlin support not yet implemented")
            elif query_language.lower() == "opencypher":
                if hasattr(self.neptune_client, 'execute_opencypher'):
                    result = await self.neptune_client.execute_opencypher(query)
                else:
                    raise NotImplementedError("OpenCypher support not yet implemented")
            else:
                raise ValueError(f"Unsupported query language: {query_language}")
            
            # Format results for agent analysis
            results = result.get('results', [])
            
            # Store ALL results for CSV export (complete dataset)
            self.last_query_results = results
            self.last_query_info = {
                "query": query,
                "query_language": query_language,
                "timestamp": self._get_timestamp()
            }
            
            # Truncate results for AI to prevent context window overflow
            # AI gets enough data to analyze patterns but not overwhelm token limits
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
                    "status": result.get("status", "success"),
                    "code": result.get("code", 200)
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "query": query,
                "query_language": query_language,
                "error": str(e),
                "results": [],
                "result_count": 0
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
    
    def _get_timestamp(self) -> str:
        """Get current timestamp for filenames."""
        from datetime import datetime
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def _create_agent(self) -> Agent:
        """Create Strands agent with Neptune execution and CSV export tools."""
        # Configure Bedrock model
        model_id = os.getenv('BEDROCK_MODEL_ID', 'us.anthropic.claude-sonnet-4-20250514-v1:0')
        region = os.getenv('NEPTUNE_REGION', 'us-east-1')
        
        bedrock_model = BedrockModel(
            model_id=model_id,
            region_name=region,
            temperature=0.1,  # Low temperature for consistent query generation
            max_tokens=4096
        )
        
        # Create system prompt
        system_prompt = self._create_system_prompt()
        
        # Create agent with Neptune execution and CSV export tools
        return Agent(
            model=bedrock_model,
            system_prompt=system_prompt,
            tools=[self.execute_neptune_query, self.export_to_csv],
            callback_handler=None  # We'll handle output in the shell
        )
    
    def _extract_json_from_response(self, text: str) -> Dict[str, Any]:
        """Extract JSON from agent response text."""
        # Try to find JSON in code blocks first
        json_pattern = r"```(?:json)?\s*(\{.*?\})\s*```"
        matches = re.search(json_pattern, text, re.DOTALL)
        
        if matches:
            json_str = matches.group(1)
        else:
            # Try to find JSON without code blocks
            json_start = text.find("{")
            json_end = text.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                json_str = text[json_start:json_end]
            else:
                raise ValueError("No JSON found in response")
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from response: {e}")
    
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
            
            # Extract text from Message object (TypedDict)
            response_text = ""
            if isinstance(agent_result.message, dict) and 'content' in agent_result.message:
                for content_block in agent_result.message['content']:
                    if isinstance(content_block, dict) and 'text' in content_block:
                        response_text += content_block['text']
            else:
                response_text = str(agent_result.message)
            
            # Extract JSON from response
            result_data = self._extract_json_from_response(response_text)
            
            # Create QueryResult from parsed JSON
            return QueryResult(
                query=result_data.get("query", ""),
                query_language=result_data.get("query_language", self.query_language.value),
                explanation=result_data.get("explanation", ""),
                results=result_data.get("results", []),
                result_count=result_data.get("result_count", 0),
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
    
    def switch_language(self, new_language: QueryLanguage) -> None:
        """Switch to a different query language.
        
        Args:
            new_language: New query language to use
        """
        self.query_language = new_language
        # Recreate agent with updated system prompt
        self.agent = self._create_agent()
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get current schema information.
        
        Returns:
            Schema configuration dictionary
        """
        return self.schema
    
    async def close(self) -> None:
        """Clean up resources."""
        # Strands agents handle cleanup automatically
        pass
