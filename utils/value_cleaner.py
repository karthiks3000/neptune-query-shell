#!/usr/bin/env python3
"""Shared utilities for cleaning and formatting values across display and export."""

import json
from datetime import datetime
from typing import Any, Optional


class ValueCleaner:
    """Utility class for cleaning and formatting values for display and export."""
    
    @staticmethod
    def clean_uri(value_str: str) -> str:
        """Clean URI values by extracting the meaningful part.
        
        Args:
            value_str: URI string to clean
            
        Returns:
            Cleaned URI with just the last segment
        """
        if value_str.startswith('<') and value_str.endswith('>'):
            # Extract URI content and get last segment
            uri_content = value_str[1:-1]
            return uri_content.split('/')[-1]
        return value_str
    
    @staticmethod
    def clean_typed_literal(value_str: str) -> str:
        """Clean RDF typed literals (e.g., "value"^^xsd:string).
        
        Args:
            value_str: Typed literal string to clean
            
        Returns:
            Value without type annotation
        """
        if '^^xsd:' in value_str:
            return value_str.split('^^')[0].strip('"')
        return value_str
    
    @staticmethod
    def clean_json_value(value_str: str) -> str:
        """Clean JSON strings/objects for display.
        
        Args:
            value_str: JSON string to clean
            
        Returns:
            Formatted JSON or flattened representation
        """
        if not (value_str.startswith('{') or value_str.startswith('[')):
            return value_str
            
        try:
            parsed = json.loads(value_str)
            if isinstance(parsed, list):
                return ', '.join(str(item) for item in parsed)
            elif isinstance(parsed, dict):
                # Flatten simple dictionaries
                if len(parsed) <= 3:
                    return '; '.join(f"{k}: {v}" for k, v in parsed.items())
                else:
                    return str(parsed)
            else:
                return str(parsed)
        except json.JSONDecodeError:
            return value_str
    
    @staticmethod
    def remove_quotes(value_str: str) -> str:
        """Remove surrounding quotes from string values.
        
        Args:
            value_str: String that may have quotes
            
        Returns:
            String without surrounding quotes
        """
        if value_str.startswith('"') and value_str.endswith('"'):
            return value_str[1:-1]
        return value_str
    
    @staticmethod
    def truncate_value(value_str: str, max_length: int) -> str:
        """Truncate value to specified length with ellipsis.
        
        Args:
            value_str: String to truncate
            max_length: Maximum allowed length
            
        Returns:
            Truncated string with ellipsis if needed
        """
        if len(value_str) > max_length:
            return value_str[:max_length - 3] + "..."
        return value_str
    
    @classmethod
    def clean_for_display(cls, value: Any, format_type: str = "table", max_length: Optional[int] = None) -> str:
        """Clean value for display with format-specific settings.
        
        Args:
            value: Value to clean
            format_type: Display format (table, tree, network)
            max_length: Override default max length
            
        Returns:
            Cleaned string value ready for display
        """
        if value is None:
            return ""
        
        value_str = str(value)
        
        # Apply all cleaning steps
        value_str = cls.clean_uri(value_str)
        value_str = cls.clean_typed_literal(value_str)
        value_str = cls.clean_json_value(value_str)
        value_str = cls.remove_quotes(value_str)
        
        # Format-specific truncation limits
        if max_length is None:
            if format_type == "tree":
                max_length = 100  # Tree has more vertical space
            elif format_type == "network":
                max_length = 60   # Panels have medium space
            else:  # table
                max_length = 30   # Table columns are constrained
        
        return cls.truncate_value(value_str, max_length)
    
    @classmethod
    def clean_for_export(cls, value: Any, max_length: int = 500) -> str:
        """Clean value for CSV export with generous length limit.
        
        Args:
            value: Value to clean
            max_length: Maximum length for export (default 500)
            
        Returns:
            Cleaned string value ready for export
        """
        if value is None:
            return ""
        
        value_str = str(value)
        
        # Apply all cleaning steps
        value_str = cls.clean_uri(value_str)
        value_str = cls.clean_typed_literal(value_str)
        value_str = cls.clean_json_value(value_str)
        value_str = cls.remove_quotes(value_str)
        
        # Export allows longer values
        return cls.truncate_value(value_str, max_length)


class TimestampUtils:
    """Utility class for consistent timestamp generation."""
    
    @staticmethod
    def get_timestamp() -> str:
        """Get current timestamp in standard format for filenames.
        
        Returns:
            Timestamp string in YYYYMMDD_HHMMSS format
        """
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    @staticmethod
    def get_readable_timestamp() -> str:
        """Get current timestamp in human-readable format.
        
        Returns:
            Timestamp string in YYYY-MM-DD HH:MM:SS format
        """
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    def format_datetime(dt: datetime, format_type: str = "filename") -> str:
        """Format datetime object with specified format.
        
        Args:
            dt: Datetime object to format
            format_type: Type of format (filename, readable, iso)
            
        Returns:
            Formatted datetime string
        """
        if format_type == "filename":
            return dt.strftime("%Y%m%d_%H%M%S")
        elif format_type == "readable":
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        elif format_type == "iso":
            return dt.isoformat()
        else:
            return str(dt)
