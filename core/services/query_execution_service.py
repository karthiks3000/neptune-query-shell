#!/usr/bin/env python3
"""Centralized query execution and export service for Neptune Query Shell."""

from typing import Dict, List, Any, Optional
from datetime import datetime

from core.enums import QueryLanguage
from export.csv_exporter import NeptuneCSVExporter
from utils.value_cleaner import TimestampUtils


class QueryExecutionService:
    """Centralized service for Neptune query execution and result management.
    
    This service eliminates duplication between the AI Generator and Shell by:
    - Providing single query execution implementation
    - Managing complete result storage for consistent exports
    - Handling result truncation for AI context management
    - Offering unified CSV export functionality
    """
    
    def __init__(self, neptune_client):
        """Initialize the query execution service.
        
        Args:
            neptune_client: NeptuneClient instance for query execution
        """
        self.neptune_client = neptune_client
        self.csv_exporter = NeptuneCSVExporter()
        
        # Centralized result storage - single source of truth
        self._last_complete_results: List[Dict[str, Any]] = []
        self._last_query_metadata: Dict[str, Any] = {}
    
    async def execute_query(self, 
                           query: str, 
                           query_language: QueryLanguage = QueryLanguage.SPARQL,
                           for_ai_context: bool = True,
                           max_ai_results: int = 10) -> Dict[str, Any]:
        """Execute a Neptune query and manage results consistently.
        
        Args:
            query: The query to execute
            query_language: Query language to use
            for_ai_context: If True, truncate results for AI context window management
            max_ai_results: Maximum results to return for AI context
            
        Returns:
            Query execution results (truncated if for_ai_context=True)
        """
        try:
            # Execute query using appropriate method based on language
            if query_language == QueryLanguage.SPARQL:
                raw_result = await self.neptune_client.execute_sparql(query)
            elif query_language == QueryLanguage.GREMLIN:
                if hasattr(self.neptune_client, 'execute_gremlin'):
                    raw_result = await self.neptune_client.execute_gremlin(query)
                else:
                    raise NotImplementedError("Gremlin support not yet implemented")
            elif query_language == QueryLanguage.OPENCYPHER:
                if hasattr(self.neptune_client, 'execute_opencypher'):
                    raw_result = await self.neptune_client.execute_opencypher(query)
                else:
                    raise NotImplementedError("OpenCypher support not yet implemented")
            else:
                raise ValueError(f"Unsupported query language: {query_language}")
            
            # Extract complete results
            complete_results = raw_result.get('results', [])
            
            # Store complete results and metadata (single source of truth)
            self._last_complete_results = complete_results
            self._last_query_metadata = {
                "query": query,
                "query_language": query_language.value,
                "timestamp": TimestampUtils.get_timestamp(),
                "result_count": len(complete_results),
                "execution_status": raw_result.get("status", "success"),
                "execution_code": raw_result.get("code", 200)
            }
            
            # Determine what results to return based on context
            if for_ai_context and len(complete_results) > max_ai_results:
                # Return truncated results for AI to prevent context overflow
                returned_results = complete_results[:max_ai_results]
                is_truncated = True
            else:
                # Return complete results
                returned_results = complete_results
                is_truncated = False
            
            return {
                "success": True,
                "query": query,
                "query_language": query_language.value,
                "results": returned_results,
                "result_count": len(complete_results),  # Total count
                "returned_count": len(returned_results),  # Actually returned
                "truncated": is_truncated,
                "execution_metadata": {
                    "status": raw_result.get("status", "success"),
                    "code": raw_result.get("code", 200)
                }
            }
            
        except Exception as e:
            # Store empty results on error
            self._last_complete_results = []
            self._last_query_metadata = {
                "query": query,
                "query_language": query_language.value,
                "timestamp": TimestampUtils.get_timestamp(),
                "error": str(e)
            }
            
            return {
                "success": False,
                "query": query,
                "query_language": query_language.value,
                "error": str(e),
                "results": [],
                "result_count": 0,
                "returned_count": 0,
                "truncated": False
            }
    
    def export_last_results(self, 
                           description: str = "query_results",
                           filename: Optional[str] = None) -> Dict[str, Any]:
        """Export the last executed query results to CSV.
        
        This method always exports the COMPLETE dataset, regardless of whether
        the results were truncated for AI context or display purposes.
        
        Args:
            description: Description for filename generation
            filename: Optional custom filename (without extension)
            
        Returns:
            Export status and file information
        """
        if not self._last_complete_results:
            return {
                "success": False,
                "error": "No query results available to export. Execute a query first."
            }
        
        try:
            # Generate filename if not provided
            if filename is None:
                timestamp = self._last_query_metadata.get("timestamp", TimestampUtils.get_timestamp())
                safe_description = description.replace(' ', '_').replace('/', '_')
                filename = f"{safe_description}_{timestamp}"
            
            # Ensure .csv extension
            if not filename.endswith('.csv'):
                filename = f"{filename}.csv"
            
            # Export complete results (never truncated)
            filepath = self.csv_exporter.export_results(
                self._last_complete_results,
                description,
                filename
            )
            
            # Get export info
            export_info = self.csv_exporter.get_export_info(filename)
            
            return {
                "success": True,
                "filepath": filepath,
                "filename": filename,
                "record_count": len(self._last_complete_results),
                "file_size_mb": export_info.get('size_mb', 0) if export_info else 0,
                "query_info": self._last_query_metadata,
                "message": f"Successfully exported {len(self._last_complete_results)} records to {filepath}"
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Export failed: {str(e)}"
            }
    
    def get_last_query_info(self) -> Dict[str, Any]:
        """Get information about the last executed query.
        
        Returns:
            Query metadata including query text, language, timestamp, etc.
        """
        return self._last_query_metadata.copy()
    
    def get_last_results_summary(self) -> Dict[str, Any]:
        """Get summary information about the last query results.
        
        Returns:
            Summary including result count, sample data, etc.
        """
        if not self._last_complete_results:
            return {
                "has_results": False,
                "result_count": 0
            }
        
        return {
            "has_results": True,
            "result_count": len(self._last_complete_results),
            "sample_keys": list(self._last_complete_results[0].keys()) if self._last_complete_results else [],
            "query_info": self._last_query_metadata
        }
    
    def clear_results(self) -> None:
        """Clear stored results and metadata.
        
        This can be useful for cleanup or when starting fresh operations.
        """
        self._last_complete_results = []
        self._last_query_metadata = {}
