#!/usr/bin/env python3
"""Core enumerations for Neptune Query Shell."""

from enum import Enum


class QueryLanguage(Enum):
    """Supported query languages with consistent naming."""
    SPARQL = "sparql"
    GREMLIN = "gremlin" 
    OPENCYPHER = "opencypher"
    
    @property
    def display_name(self) -> str:
        """Get display name for the query language."""
        return self.value.upper()
    
    @property
    def lowercase(self) -> str:
        """Get lowercase value for internal use."""
        return self.value
    
    @property
    def uppercase(self) -> str:
        """Get uppercase value for display."""
        return self.value.upper()
    
    @classmethod
    def from_string(cls, value: str) -> 'QueryLanguage':
        """Create QueryLanguage from string (case-insensitive)."""
        value_lower = value.lower()
        for lang in cls:
            if lang.value == value_lower:
                return lang
        raise ValueError(f"Unknown query language: {value}")


class DisplayFormat(Enum):
    """Supported display formats for query results."""
    TABLE = "table"
    NETWORK = "network"
    TREE = "tree"
    
    @property
    def display_name(self) -> str:
        """Get display name for the format."""
        return self.value.title()
