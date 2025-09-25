#!/usr/bin/env python3
"""Generic CSV exporter for Neptune query results."""

import csv
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional


class NeptuneCSVExporter:
    """Exports Neptune query results to CSV format with dynamic column detection."""
    
    def __init__(self, output_dir: str = "exports"):
        """Initialize CSV exporter.
        
        Args:
            output_dir: Directory to save CSV files to
        """
        self.output_dir = output_dir
        self._ensure_output_dir()
    
    def _ensure_output_dir(self) -> None:
        """Ensure output directory exists."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def export_results(self, results: List[Dict[str, Any]],
                      description: str = "query_results",
                      filename: Optional[str] = None) -> str:
        """Export any query results to CSV with dynamic column detection.
        
        Args:
            results: Query results (any format)
            description: Description for filename generation
            filename: Optional custom filename
            
        Returns:
            Path to created CSV file
        """
        if not results:
            raise ValueError("No results to export")
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_description = description.replace(' ', '_').replace('/', '_')
            filename = f"{safe_description}_{timestamp}.csv"
        
        filepath = os.path.join(self.output_dir, filename)
        
        # Dynamic column detection with smart ordering
        all_columns = set()
        for result in results:
            all_columns.update(result.keys())
        
        # Smart column ordering: common key fields first, then alphabetical
        key_fields = ['id', 'guid', 'name', 'label', 'type', 'set']
        ordered_columns = []
        
        # Add key fields that exist (in priority order)
        for key_field in key_fields:
            if key_field in all_columns:
                ordered_columns.append(key_field)
                all_columns.remove(key_field)
        
        # Add remaining columns alphabetically
        ordered_columns.extend(sorted(all_columns))
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=ordered_columns)
            writer.writeheader()
            
            for result in results:
                row = dict(result)
                self._clean_row_values(row)
                writer.writerow(row)
        
        return filepath
    
    def _clean_row_values(self, row: Dict[str, Any]) -> None:
        """Clean values in a row dictionary in-place.
        
        Args:
            row: Dictionary to clean values in
        """
        for key, value in row.items():
            row[key] = self._clean_value(value)
    
    def _clean_value(self, value: Any) -> str:
        """Clean a single value for CSV export.
        
        Args:
            value: Value to clean
            
        Returns:
            Cleaned string value
        """
        if value is None:
            return ""
        
        value_str = str(value)
        
        # Handle URI values (RDF/SPARQL)
        if value_str.startswith('<') and value_str.endswith('>'):
            # Extract just the last part of URI
            clean_value = value_str[1:-1].split('/')[-1]
            return clean_value
        
        # Handle typed literals (e.g., "value"^^xsd:string)
        if '^^xsd:' in value_str:
            clean_value = value_str.split('^^')[0].strip('"')
            return clean_value
        
        # Handle JSON strings/objects
        if value_str.startswith('{') or value_str.startswith('['):
            try:
                # Try parsing as JSON and flatten if it's simple
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
                pass
        
        # Remove extra quotes
        if value_str.startswith('"') and value_str.endswith('"'):
            value_str = value_str[1:-1]
        
        # Handle very long values with truncation
        if len(value_str) > 500:
            value_str = value_str[:497] + "..."
        
        return value_str
    
    def list_exports(self) -> List[str]:
        """List all CSV files in the export directory.
        
        Returns:
            List of CSV filenames
        """
        if not os.path.exists(self.output_dir):
            return []
        
        csv_files = [f for f in os.listdir(self.output_dir) if f.endswith('.csv')]
        return sorted(csv_files, reverse=True)  # Most recent first
    
    def get_export_info(self, filename: str) -> Dict[str, Any]:
        """Get information about an exported CSV file.
        
        Args:
            filename: Name of CSV file
            
        Returns:
            Dictionary with file information
        """
        filepath = os.path.join(self.output_dir, filename)
        
        if not os.path.exists(filepath):
            return {}
        
        stat = os.stat(filepath)
        
        # Count rows
        row_count = 0
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                row_count = sum(1 for _ in reader) - 1  # Subtract header
        except Exception:
            row_count = -1
        
        return {
            'filename': filename,
            'filepath': filepath,
            'size_bytes': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'created': datetime.fromtimestamp(stat.st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
            'modified': datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            'row_count': row_count
        }
