#!/usr/bin/env python3
"""Abstract base class for Neptune AI agents."""

import json
import re
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Any, Optional

from jinja2 import Environment, FileSystemLoader
from strands import Agent, tool
from strands.models import BedrockModel
from botocore.config import Config as BotocoreConfig

from core.enums import QueryLanguage
from utils.value_cleaner import TimestampUtils



class BaseNeptuneAgent(ABC):
    """Abstract base class for Neptune AI agents with common functionality."""
    
    def __init__(self, neptune_client, query_language: QueryLanguage):
        """Initialize the base Neptune agent.
        
        Args:
            neptune_client: NeptuneClient instance for query execution
            query_language: Target query language
        """
        self.neptune_client = neptune_client
        self.query_language = query_language
        self.jinja_env = self._setup_jinja()
        self.agent = self._create_agent()
    
    def _setup_jinja(self) -> Environment:
        """Setup Jinja environment for template rendering."""
        template_dir = Path(__file__).parent.parent / "templates"
        return Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True
        )
    
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
            
            # Let subclasses customize result processing
            return self._process_query_results(query, query_language, results, result)
            
        except Exception as e:
            return {
                "success": False,
                "query": query,
                "query_language": query_language,
                "error": str(e),
                "results": [],
                "result_count": 0
            }
    
    def _get_timestamp(self) -> str:
        """Get current timestamp for filenames."""
        return TimestampUtils.get_timestamp()
    
    def _create_agent(self) -> Agent:
        """Create Strands agent with Neptune execution tools."""
        # Configure Bedrock model
        model_id = os.getenv('BEDROCK_MODEL_ID', 'us.anthropic.claude-sonnet-4-20250514-v1:0')
        region = os.getenv('NEPTUNE_REGION', 'us-east-1')
        
        # Configure boto client with extended timeout for Claude 4+ models
        boto_config = BotocoreConfig(
            read_timeout=3600,  # 60 minutes for Claude 4+ models
            connect_timeout=60,
            retries={'max_attempts': 3}
        )
        
        # Load additional request fields from environment variable
        additional_fields = self._load_additional_request_fields()
        
        bedrock_model = BedrockModel(
            model_id=model_id,
            region_name=region,
            temperature=0.1,  # Low temperature for consistent behavior
            max_tokens=self._get_max_tokens(),
            boto_client_config=boto_config,
            additional_request_fields=additional_fields
        )
        
        # Create system prompt (implemented by subclasses)
        system_prompt = self._create_system_prompt()
        
        # Get tools (base + additional from subclasses)
        all_tools = [self.execute_neptune_query] + self._get_additional_tools()
        
        # Create agent with all tools
        return Agent(
            model=bedrock_model,
            system_prompt=system_prompt,
            tools=all_tools,
            callback_handler=None
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
    
    def _extract_message_text(self, agent_result) -> str:
        """Extract text from Strands Agent SDK message object."""
        response_text = ""
        if isinstance(agent_result.message, dict) and 'content' in agent_result.message:
            for content_block in agent_result.message['content']:
                if isinstance(content_block, dict) and 'text' in content_block:
                    response_text += content_block['text']
        else:
            response_text = str(agent_result.message)
        return response_text
    
    def switch_language(self, new_language: QueryLanguage) -> None:
        """Switch to a different query language.
        
        Args:
            new_language: New query language to use
        """
        self.query_language = new_language
        # Recreate agent with updated system prompt
        self.agent = self._create_agent()
    
    async def close(self) -> None:
        """Clean up resources."""
        # Strands agents handle cleanup automatically
        pass
    
    # Abstract methods that subclasses must implement
    @abstractmethod
    def _create_system_prompt(self) -> str:
        """Create system prompt for the specific agent type."""
        pass
    
    @abstractmethod
    def _get_additional_tools(self) -> List:
        """Get additional tools beyond execute_neptune_query."""
        pass
    
    def _get_max_tokens(self) -> int:
        """Get maximum tokens for the agent from environment."""
        return int(os.getenv('MAX_TOKENS', '4096'))  # Default to 4096 if not set
    
    def _load_additional_request_fields(self) -> Optional[Dict[str, Any]]:
        """Load provider-specific additional request fields from environment variable.
        
        This allows different model providers to specify their own request fields
        without hardcoding provider-specific logic in the base agent.
        
        Environment variable format (JSON string):
            BEDROCK_ADDITIONAL_REQUEST_FIELDS='{"anthropic_beta": ["context-1m-2025-08-07"]}'
        
        Returns:
            Dictionary of additional request fields or None
        """
        fields_json = os.getenv('BEDROCK_ADDITIONAL_REQUEST_FIELDS')
        
        if not fields_json:
            return None
        
        try:
            return json.loads(fields_json)
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse BEDROCK_ADDITIONAL_REQUEST_FIELDS: {e}")
            return None
    
    @abstractmethod
    def _process_query_results(self, query: str, query_language: str, 
                             results: List[Dict[str, Any]], raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """Process query results for the specific agent type."""
        pass
