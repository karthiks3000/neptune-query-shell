#!/usr/bin/env python3
"""Neptune database client with SPARQL support."""

import os
from typing import Any, Dict, Optional

from .connection import ConnectionManager


class NeptuneClient:
    """Generic Neptune database client with SPARQL support."""
    
    def __init__(self, endpoint: str, region: str, port: int = 8182):
        """Initialize Neptune client.
        
        Args:
            endpoint: Neptune cluster endpoint
            region: AWS region 
            port: Neptune port (default 8182)
        """
        self.endpoint = endpoint
        self.region = region
        self.port = port
        self.connection_manager = ConnectionManager(endpoint, region, port)
        self._initialized = False
    
    async def init(self) -> None:
        """Initialize the Neptune connection."""
        if not self._initialized:
            await self.connection_manager.init_sparql()
            self._initialized = True
    
    async def close(self) -> None:
        """Close the Neptune connection."""
        if self._initialized:
            await self.connection_manager.close()
            self._initialized = False
    
    async def execute_sparql(self, query: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute SPARQL query against Neptune.
        
        Args:
            query: SPARQL query string
            params: Optional query parameters
            
        Returns:
            Query results dictionary
            
        Raises:
            Exception: If connection not initialized or query fails
        """
        if not self._initialized:
            raise Exception("Neptune client not initialized. Call init() first.")
        
        return await self.connection_manager.execute_sparql(query, params)
    
    async def reset_database(self) -> bool:
        """Reset the entire Neptune database.
        
        WARNING: This will delete ALL data in the database.
        
        Returns:
            True if reset successful, False otherwise
            
        Raises:
            Exception: If connection not initialized or reset fails
        """
        if not self._initialized:
            raise Exception("Neptune client not initialized. Call init() first.")
        
        return await self.connection_manager.fast_reset_database()
    
    @classmethod
    def from_environment(cls) -> 'NeptuneClient':
        """Create Neptune client from environment variables.
        
        Expected environment variables:
        - NEPTUNE_ENDPOINT: Neptune cluster endpoint
        - NEPTUNE_REGION: AWS region
        - NEPTUNE_PORT: Neptune port (optional, defaults to 8182)
        
        Returns:
            Configured NeptuneClient instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        endpoint = os.getenv('NEPTUNE_ENDPOINT')
        region = os.getenv('NEPTUNE_REGION')
        port = int(os.getenv('NEPTUNE_PORT', '8182'))
        
        if not endpoint:
            raise ValueError("NEPTUNE_ENDPOINT environment variable is required")
        if not region:
            raise ValueError("NEPTUNE_REGION environment variable is required")
        
        return cls(endpoint=endpoint, region=region, port=port)
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information.
        
        Returns:
            Dictionary with connection details
        """
        return {
            'endpoint': self.endpoint,
            'region': self.region,
            'port': self.port,
            'initialized': self._initialized,
            'sparql_endpoint': f"https://{self.endpoint}:{self.port}/sparql"
        }
    
    async def test_connection(self) -> bool:
        """Test the Neptune connection with a simple query.
        
        Returns:
            True if connection is working, False otherwise
        """
        try:
            if not self._initialized:
                await self.init()
            
            # Simple ASK query to test connectivity
            test_query = "ASK { ?s ?p ?o }"
            result = await self.execute_sparql(test_query)
            
            # Should return a boolean result
            return 'results' in result
            
        except Exception:
            return False


# Convenience alias for backward compatibility
NeptuneConnectionManager = NeptuneClient
