#!/usr/bin/env python3
"""AI-powered query generator using Strands Agent SDK with configurable schema."""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field

from .base_agent import BaseNeptuneAgent
from core.enums import QueryLanguage
from core.services.query_execution_service import QueryExecutionService
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
    
    def __init__(self, query_execution_service: QueryExecutionService, query_language: QueryLanguage = QueryLanguage.SPARQL):
        """Initialize the AI query generator.
        
        Args:
            query_execution_service: Shared query execution service
            query_language: Target query language
        """
        self.query_service = query_execution_service
        self.schema = self._load_schema()
        # Use the service's neptune_client for the base agent
        super().__init__(query_execution_service.neptune_client, query_language)
    
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
    
    
    def _process_query_results(self, query: str, query_language: str, 
                             results: List[Dict[str, Any]], raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Process query results using the shared service (maintains AI context truncation)."""
        # The service already stores complete results, just return the data for AI
        return {
            "success": True,
            "query": query,
            "query_language": query_language,
            "results": results,  # Already truncated by service for AI context
            "result_count": raw_result.get("result_count", len(results)),
            "truncated": raw_result.get("truncated", False),
            "returned_count": raw_result.get("returned_count", len(results)),
            "execution_metadata": {
                "status": raw_result.get("status", "success"),
                "code": raw_result.get("code", 200)
            }
        }

    @tool
    async def execute_neptune_query(self, query: str, query_language: Optional[str] = None) -> Dict[str, Any]:
        """Execute Neptune query using the shared service (AI tool interface).
        
        Args:
            query: The query to execute
            query_language: Query language (optional, defaults to current language)
            
        Returns:
            Query execution results (truncated for AI context)
        """
        if query_language is None:
            query_language_enum = self.query_language
        else:
            query_language_enum = QueryLanguage.from_string(query_language)
        
        # Use shared service with AI context truncation (now character-based)
        return await self.query_service.execute_query(
            query, 
            query_language_enum, 
            for_ai_context=True  # Truncate based on character count
        )

    @tool 
    async def export_to_csv(self, filename: Optional[str] = None, description: str = "ai_query_results") -> Dict[str, Any]:
        """Export the last query results to CSV using the shared service.
        
        Args:
            filename: Optional custom filename (without extension)
            description: Description of what's being exported
            
        Returns:
            Export status and file information
        """
        # Use shared service for consistent export (always exports complete dataset)
        return self.query_service.export_last_results(description, filename)
    
    async def process_natural_language_query(self, natural_query: str, streaming: bool = False) -> QueryResult:
        """Process natural language query and return structured results.
        
        Args:
            natural_query: User's natural language description
            streaming: Whether to show AI thinking process in real-time
            
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
3. If the user requests CSV export, use the export_to_csv tool
4. Analyze the ACTUAL results returned by the query tool
5. Respond in JSON format with the real data

CRITICAL JSON FORMAT REQUIREMENTS:
- The "results" field MUST always contain the actual query results as a list of objects
- NEVER put export messages or strings in the "results" field
- If you export to CSV, mention it in the "insights" field, not in "results"
- The "results" field must be: [{{"field1": "value1", "field2": "value2"}}, ...]
- IMPORTANT: Even if you export many records to CSV, only include a SAMPLE of results in the JSON response (typically 5-10 records) to keep the response readable
- Use the actual results returned by execute_neptune_query (which are already truncated for display)

Example correct format:
{{
  "query": "SELECT ...",
  "query_language": "sparql", 
  "results": [{{"standard": "AK.1", "text": "Standard text"}}, {{"standard": "AK.2", "text": "Other text"}}],
  "result_count": 120,
  "insights": "Query executed successfully returning 120 total records. Sample of results shown above. Complete dataset exported to CSV file: filename.csv"
}}

Do NOT make up or fabricate any results. Use only the actual data returned by the execute_neptune_query tool.
"""
        
        try:
            if streaming:
                return await self._process_with_streaming(conversation_prompt)
            else:
                # Use regular agent conversation (non-streaming)
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
    
    async def _process_with_streaming(self, conversation_prompt: str) -> QueryResult:
        """Process query with streaming to show AI's thinking process."""
        print("\n🤖 AI Thinking Process:")
        print("─" * 50)
        
        full_response_text = ""
        tool_execution_count = {}
        current_tool_message = ""
        seen_tool_use_ids = set()
        
        try:
            # Use streaming async iterator to show AI thinking
            agent_stream = self.agent.stream_async(conversation_prompt)
            
            async for event in agent_stream:
                if "data" in event:
                    # Clear any tool message when AI starts generating text
                    if current_tool_message:
                        # Clear the line and move to next line
                        print(f"\r{' ' * len(current_tool_message)}\r", end="", flush=True)
                        current_tool_message = ""
                        print()  # Add newline after clearing tool message
                    
                    # AI is generating text - show it live
                    text_chunk = event["data"]
                    print(text_chunk, end="", flush=True)
                    full_response_text += text_chunk
                    
                elif "current_tool_use" in event and event["current_tool_use"].get("name"):
                    # Get tool use ID to track unique tool executions
                    tool_use_id = event["current_tool_use"].get("toolUseId")
                    tool_name = event["current_tool_use"]["name"]
                    
                    # Only show message for the FIRST time we see this tool use ID
                    if tool_use_id and tool_use_id not in seen_tool_use_ids:
                        seen_tool_use_ids.add(tool_use_id)
                        tool_execution_count[tool_name] = tool_execution_count.get(tool_name, 0) + 1
                        
                        # Build the new message
                        if tool_name == "execute_neptune_query":
                            count = tool_execution_count[tool_name]
                            if count == 1:
                                new_message = "🔍 Executing Neptune query..."
                            else:
                                new_message = f"🔍 Executing Neptune query ({count})..."
                        elif tool_name == "export_to_csv":
                            new_message = "💾 Exporting to CSV..."
                        else:
                            new_message = f"🔧 Using tool: {tool_name}..."
                        
                        # If we already have a tool message, replace it on the same line
                        if current_tool_message:
                            print(f"\r{new_message}", end="", flush=True)
                        else:
                            # First tool use - add newline before it
                            print(f"\n{new_message}", end="", flush=True)
                        
                        current_tool_message = new_message
            
            # Clear any remaining tool message
            if current_tool_message:
                print(f"\r{' ' * len(current_tool_message)}\r", end="", flush=True)
                print()  # Move to next line
                        
            print("\n" + "─" * 50)
            
            # Show summary of tool usage
            if tool_execution_count:
                print("🔧 Tools used:")
                for tool, count in tool_execution_count.items():
                    if tool == "execute_neptune_query":
                        print(f"   • Neptune queries: {count}")
                    elif tool == "export_to_csv":
                        print(f"   • CSV exports: {count}")
                    else:
                        print(f"   • {tool}: {count}")
                print()
            
            print("🤖 Processing complete!\n")
            
            # Extract JSON from the full response
            result_data = self._extract_json_from_response(full_response_text)
            
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
            # Clear any remaining tool message on error
            if current_tool_message:
                print(f"\r{' ' * len(current_tool_message)}\r", end="", flush=True)
                
            print(f"\n❌ Streaming failed: {str(e)}")
            # Return error result
            return QueryResult(
                query="",
                query_language=self.query_language.value,
                explanation=f"Failed to process streaming query: {str(e)}",
                results=[],
                result_count=0,
                insights="Streaming query generation failed",
                suggestions=["Try again or switch to non-streaming mode"]
            )
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get current schema information.
        
        Returns:
            Schema configuration dictionary
        """
        return self.schema
