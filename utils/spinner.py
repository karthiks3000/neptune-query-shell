#!/usr/bin/env python3
"""Loading spinner utilities for Neptune query shell."""

import asyncio
import itertools
import sys
from typing import Any, Optional


class LoadingSpinner:
    """Loading spinner with Rich and ASCII fallback support."""
    
    def __init__(self, message: str = "Loading...", spinner_type: str = "dots"):
        """Initialize loading spinner.
        
        Args:
            message: Message to display
            spinner_type: Type of spinner animation
        """
        self.message = message
        self.spinner_type = spinner_type
        self._task = None
        self._stop_event = None
        
        # Try to use Rich if available
        try:
            from rich.console import Console
            from rich.live import Live
            from rich.spinner import Spinner
            from rich.text import Text
            
            self.rich_available = True
            self.console = Console()
            self.Spinner = Spinner
            self.Live = Live
            self.Text = Text
        except ImportError:
            self.rich_available = False
            
        # ASCII spinner frames for fallback
        self.ascii_spinners = {
            'dots': ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'],
            'classic': ['|', '/', '-', '\\'],
            'arrow': ['←', '↖', '↑', '↗', '→', '↘', '↓', '↙'],
            'clock': ['🕐', '🕑', '🕒', '🕓', '🕔', '🕕', '🕖', '🕗', '🕘', '🕙', '🕚', '🕛'],
            'moon': ['🌑', '🌒', '🌓', '🌔', '🌕', '🌖', '🌗', '🌘'],
            'earth': ['🌍', '🌎', '🌏'],
            'bouncing': ['⠁', '⠂', '⠄', '⡀', '⢀', '⠠', '⠐', '⠈']
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()
    
    async def start(self):
        """Start the spinner animation."""
        if self.rich_available:
            await self._start_rich_spinner()
        else:
            await self._start_ascii_spinner()
    
    async def stop(self):
        """Stop the spinner animation."""
        if self._stop_event:
            self._stop_event.set()
        
        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
    
    async def _start_rich_spinner(self):
        """Start Rich spinner animation."""
        try:
            spinner = self.Spinner(self.spinner_type, text=self.Text(self.message, style="blue"))
            
            # Create a task to run the spinner
            async def run_spinner():
                with self.Live(spinner, console=self.console, auto_refresh=True) as live:
                    while not self._stop_event.is_set():
                        await asyncio.sleep(0.1)
            
            self._stop_event = asyncio.Event()
            self._task = asyncio.create_task(run_spinner())
            
        except Exception as e:
            # Fall back to ASCII if Rich fails
            await self._start_ascii_spinner()
    
    async def _start_ascii_spinner(self):
        """Start ASCII spinner animation."""
        frames = self.ascii_spinners.get(self.spinner_type, self.ascii_spinners['dots'])
        spinner_cycle = itertools.cycle(frames)
        
        async def run_ascii_spinner():
            while not self._stop_event.is_set():
                frame = next(spinner_cycle)
                sys.stdout.write(f'\r{frame} {self.message}')
                sys.stdout.flush()
                await asyncio.sleep(0.1)
            
            # Clear the spinner line
            sys.stdout.write('\r' + ' ' * (len(self.message) + 5) + '\r')
            sys.stdout.flush()
        
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(run_ascii_spinner())


class SpinnerManager:
    """Manager for different types of loading operations."""
    
    @staticmethod
    async def with_spinner(message: str, operation, spinner_type: str = "dots"):
        """Execute an async operation with a loading spinner.
        
        Args:
            message: Loading message to display
            operation: Async operation to execute
            spinner_type: Type of spinner animation
            
        Returns:
            Result of the operation
        """
        async with LoadingSpinner(message, spinner_type):
            return await operation()
    
    @staticmethod  
    async def ai_generation(operation):
        """Execute AI operation with brain spinner."""
        return await SpinnerManager.with_spinner(
            "🤖 Generating SPARQL query...", 
            operation, 
            "bouncing"
        )
    
    @staticmethod
    async def query_execution(operation):
        """Execute database query with loading spinner."""
        return await SpinnerManager.with_spinner(
            "🚀 Executing SPARQL query...", 
            operation, 
            "dots"
        )
    
    @staticmethod
    async def data_processing(operation, record_count: int = 0):
        """Execute data processing with progress indicator."""
        if record_count > 0:
            message = f"📊 Processing {record_count:,} records..."
        else:
            message = "📊 Processing results..."
        
        return await SpinnerManager.with_spinner(
            message, 
            operation, 
            "arrow"
        )
    
    @staticmethod
    async def ai_refinement(operation):
        """Execute AI refinement with thinking spinner."""
        return await SpinnerManager.with_spinner(
            "🔄 Refining query based on feedback...", 
            operation, 
            "clock"
        )
    
    @staticmethod
    async def csv_export(operation, filename: str = ""):
        """Execute CSV export with file spinner."""
        if filename:
            message = f"💾 Exporting to {filename}..."
        else:
            message = "💾 Exporting to CSV..."
        
        return await SpinnerManager.with_spinner(
            message, 
            operation, 
            "earth"
        )
    
    @staticmethod
    async def connection(operation, endpoint: str = ""):
        """Execute connection with network spinner."""
        if endpoint:
            message = f"📡 Connecting to {endpoint}..."
        else:
            message = "📡 Connecting to Neptune..."
        
        return await SpinnerManager.with_spinner(
            message, 
            operation, 
            "moon"
        )
