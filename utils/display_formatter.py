#!/usr/bin/env python3
"""Display formatter for Neptune query results using Rich library."""

from typing import Any, Dict, List
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich import box


class NeptuneDisplayFormatter:
    """Formats Neptune query results for console display using Rich library."""
    
    def __init__(self):
        """Initialize the formatter with Rich console."""
        self.console = Console()
       
    def format_sparql_results(self, results: List[Dict[str, Any]], 
                            query_type: str = "Query") -> str:
        """Format SPARQL query results as a rich table.
        
        Args:
            results: SPARQL query results
            query_type: Type of query for header
            
        Returns:
            Formatted results as string
        """
        if not results:
            return f"\n📊 No results found for {query_type}."
        
        return self._format_rich_sparql_results(results, query_type)
    
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
                value = str(result.get(key, ''))
                
                # Clean up URIs and typed literals for better readability
                if value.startswith('<') and value.endswith('>'):
                    value = value[1:-1].split('/')[-1]
                elif '^^xsd:' in value:
                    value = value.split('^^')[0].strip('"')
                
                # Handle long values with smart truncation
                if len(value) > 80:
                    value = value[:77] + "..."
                
                row_data.append(value)
            
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
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
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
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
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
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
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
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with self.console.capture() as capture:
            self.console.print(f"[{timestamp}] ⚠️  {message}", style="yellow")
        
        return capture.get()
