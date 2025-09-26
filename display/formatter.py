#!/usr/bin/env python3
"""Display formatter for Neptune query results using Rich library."""

from typing import Any, Dict, List, Optional
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text
from rich import box

from utils.value_cleaner import ValueCleaner, TimestampUtils


class NeptuneDisplayFormatter:
    """Formats Neptune query results for console display using Rich library."""
    
    def __init__(self):
        """Initialize the formatter with Rich console."""
        self.console = Console()
       
    def format_sparql_results(self, results: List[Dict[str, Any]], 
                            query_type: str = "Query", 
                            display_format: str = "table",
                            display_config: Optional[Dict[str, Any]] = None) -> str:
        """Format query results using specified visualization format.
        
        Args:
            results: Query results
            query_type: Type of query for header
            display_format: Visualization format (table|network|tree)
            display_config: AI-provided field mappings for visualization
            
        Returns:
            Formatted results as string
        """
        if not results:
            return f"\n📊 No results found for {query_type}."
        
        if display_format == "network":
            return self._format_as_network(results, query_type, display_config)
        elif display_format == "tree":
            return self._format_as_tree(results, query_type, display_config)
        else:  # Default to table
            return self._format_rich_sparql_results(results, query_type)
    
    def _format_as_network(self, results: List[Dict[str, Any]], query_type: str, display_config: Optional[Dict[str, Any]] = None) -> str:
        """Format results as a network graph visualization using AI-provided field mappings."""
        if not display_config:
            display_config = {}
        
        # Get field mappings from AI configuration
        name_field = display_config.get('item_name_field')
        primary_fields = display_config.get('primary_fields', [])
        
        # Create network visualization
        panels = []
        
        # Create panels for each result
        for result in results:
            # Get display name using AI-specified field
            if name_field and name_field in result:
                display_name = self._clean_display_value(result[name_field])
            else:
                # Fallback to common name fields
                display_name = (result.get('name') or result.get('id') or 
                               result.get('guid') or result.get('label') or 'Node')
                display_name = self._clean_display_value(display_name)
            
            # Show primary fields specified by AI
            content_lines = []
            if primary_fields:
                for field in primary_fields:
                    if field in result:
                        clean_value = self._clean_display_value(result[field], "network")
                        content_lines.append(f"{field}: {clean_value}")
            else:
                # Fallback to showing some properties
                for key, value in list(result.items())[:3]:
                    if key != name_field:
                        clean_value = self._clean_display_value(value, "network")
                        content_lines.append(f"{key}: {clean_value}")
            
            content_text = "\n".join(content_lines)
            panel = Panel(
                content_text,
                title=f"🔵 {display_name}",
                border_style="blue"
            )
            panels.append(panel)
        
        # Arrange in columns
        output = Columns(panels, equal=True, expand=True)
        
        with self.console.capture() as capture:
            self.console.print(f"\n🌐 {query_type} Network ({len(results)} items)")
            self.console.print(output)
        return capture.get()
    
    def _format_as_tree(self, results: List[Dict[str, Any]], query_type: str, display_config: Optional[Dict[str, Any]] = None) -> str:
        """Format results as a tree/hierarchy visualization using AI-provided field mappings."""
        if not display_config:
            display_config = {}
        
        tree = Tree(f"🌳 {query_type} Hierarchy")
        
        # Get field mappings from AI configuration
        name_field = display_config.get('item_name_field')
        primary_fields = display_config.get('primary_fields', [])
        
        # Add each result as a tree item
        for result in results:
            # Get display name using AI-specified field
            if name_field and name_field in result:
                display_name = self._clean_display_value(result[name_field])
            else:
                # Fallback to common name fields
                display_name = (result.get('name') or result.get('id') or 
                               result.get('guid') or result.get('label') or 'Item')
                display_name = self._clean_display_value(display_name)
            
            # Create branch for this item
            branch = tree.add(f"📁 {display_name}")
            
            # Add primary fields as sub-items
            if primary_fields:
                for field in primary_fields:
                    if field in result:
                        clean_value = self._clean_display_value(result[field], "tree")
                        branch.add(f"{field}: {clean_value}")
            else:
                # Fallback to showing some properties
                for key, value in list(result.items())[:3]:
                    if key != name_field:
                        clean_value = self._clean_display_value(value, "tree")
                        branch.add(f"{key}: {clean_value}")
        
        with self.console.capture() as capture:
            self.console.print(tree)
        return capture.get()
    
    def _looks_like_relationship(self, result: Dict[str, Any]) -> bool:
        """Check if result looks like a relationship/edge."""
        rel_keys = ['source', 'target', 'from', 'to', 'relationship', 'edge']
        return any(key in result for key in rel_keys) or len(result) <= 3
    
    def _looks_like_hierarchy(self, result: Dict[str, Any]) -> bool:
        """Check if result looks like hierarchical data."""
        hier_keys = ['parent', 'child', 'level', 'depth']
        return any(key in result for key in hier_keys)
    
    def _clean_display_value(self, value: Any, format_type: str = "tree") -> str:
        """Clean value for display in graph formats with format-specific limits."""
        return ValueCleaner.clean_for_display(value, format_type)
    
    def _format_rich_sparql_results(self, results: List[Dict[str, Any]], 
                                  query_type: str) -> str:
        """Format SPARQL results using Rich library."""
        # Get all unique keys for columns
        all_keys = set()
        for result in results:
            all_keys.update(result.keys())
        
        # Create table with professional styling
        table = Table(
            title=f"🔍 {query_type} Results ({len(results)} rows)", 
            box=box.ROUNDED, 
            show_header=True,
            header_style="bold cyan",
            title_style="bold blue"
        )
        
        # Add columns with appropriate minimum widths for readability
        for key in sorted(all_keys):
            # Set minimum column width to prevent squishing
            min_width = max(15, len(key) + 4)  # At least 15 chars for good spacing
            table.add_column(
                key, 
                style="yellow", 
                min_width=min_width,
                header_style="bold cyan"
            )
        
        # Add data rows
        for result in results:
            row_data = []
            for key in sorted(all_keys):
                raw_value = result.get(key, '')
                # Use shared value cleaner for consistent formatting
                clean_value = ValueCleaner.clean_for_display(raw_value, "table", max_length=80)
                row_data.append(clean_value)
            
            table.add_row(*row_data)
        
        # Capture rich output as string
        with self.console.capture() as capture:
            self.console.print(table)
        return capture.get()
    
    def format_error(self, error_msg: str, context: str = "") -> str:
        """Format error messages with Rich styling.
        
        Args:
            error_msg: The error message
            context: Optional context for the error
            
        Returns:
            Formatted error message
        """
        timestamp = TimestampUtils.get_readable_timestamp()
        
        with self.console.capture() as capture:
            self.console.print(f"\n[{timestamp}] ", style="dim", end="")
            self.console.print("❌ ERROR", style="bold red")
            if context:
                self.console.print(f"Context: {context}", style="yellow")
            self.console.print(f"Details: {error_msg}", style="red")
            self.console.print()
        
        return capture.get()
    
    def format_info(self, message: str) -> str:
        """Format info messages with Rich styling.
        
        Args:
            message: The info message
            
        Returns:
            Formatted info message
        """
        timestamp = TimestampUtils.get_readable_timestamp()
        
        with self.console.capture() as capture:
            self.console.print(f"[{timestamp}] ℹ️  {message}", style="blue")
        
        return capture.get()
    
    def format_success(self, message: str) -> str:
        """Format success messages with Rich styling.
        
        Args:
            message: The success message
            
        Returns:
            Formatted success message
        """
        timestamp = TimestampUtils.get_readable_timestamp()
        
        with self.console.capture() as capture:
            self.console.print(f"[{timestamp}] ✅ {message}", style="green")
        
        return capture.get()
    
    def format_warning(self, message: str) -> str:
        """Format warning messages with Rich styling.
        
        Args:
            message: The warning message
            
        Returns:
            Formatted warning message
        """
        timestamp = TimestampUtils.get_readable_timestamp()
        
        with self.console.capture() as capture:
            self.console.print(f"[{timestamp}] ⚠️  {message}", style="yellow")
        
        return capture.get()
