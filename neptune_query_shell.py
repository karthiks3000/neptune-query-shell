#!/usr/bin/env python3
"""Professional Neptune Query Shell - Multi-language Graph Database Interface."""

import asyncio
import os
import sys
from enum import Enum
from typing import Any, Dict, List, Optional


# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️  python-dotenv not available. Make sure environment variables are set.")

from display.formatter import NeptuneDisplayFormatter
from agents.ai_query_generator import AIQueryGenerator
from agents.schema_discovery_agent import SchemaDiscoveryAgent
from core.enums import QueryLanguage
from core.services.query_execution_service import QueryExecutionService
from utils.spinner import SpinnerManager
from neptune import NeptuneClient


class NeptuneQueryShell:
    """Professional Neptune Query Shell with multi-language support."""
    
    def __init__(self):
        """Initialize the Neptune query shell."""
        self.formatter = NeptuneDisplayFormatter()
        self.query_service: Optional[QueryExecutionService] = None
        self.ai_generator: Optional[AIQueryGenerator] = None
        self.neptune_client: Optional[NeptuneClient] = None
        self.connected = False
        self.current_language = QueryLanguage.SPARQL
    
    def print_banner(self) -> None:
        """Display application banner."""
        print("\n" + "="*70)
        print("🚀 NEPTUNE QUERY SHELL")
        print("="*70)
        print("Professional Multi-Language Graph Database Interface")
        print("-"*70)
    
    async def validate_connection(self) -> bool:
        """Validate Neptune connection with retry options."""
        self.print_banner()
        
        # Get Neptune configuration
        endpoint = os.getenv('NEPTUNE_ENDPOINT')
        region = os.getenv('NEPTUNE_REGION')
        port = int(os.getenv('NEPTUNE_PORT', '8182'))
        
        if not endpoint or not region:
            print(self.formatter.format_error(
                "Missing Neptune configuration. Please set environment variables:",
                "Configuration Error"
            ))
            print("Required variables:")
            print("  • NEPTUNE_ENDPOINT - Neptune cluster endpoint")
            print("  • NEPTUNE_REGION   - AWS region")
            print("  • NEPTUNE_PORT     - Port (optional, defaults to 8182)")
            return False
        
        print(f"📡 Target: {endpoint}:{port}")
        print(f"🌍 Region: {region}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"\n🔄 Connection attempt {attempt + 1}/{max_retries}")
                
                # Initialize Neptune client
                self.neptune_client = NeptuneClient(
                    endpoint=endpoint,
                    region=region,
                    port=port
                )
                
                print(self.formatter.format_info("Connecting to Neptune..."))
                await self.neptune_client.init()
                
                # Test connection with a simple query
                test_query = "ASK { ?s ?p ?o }"
                await self.neptune_client.execute_sparql(test_query)
                
                self.connected = True
                
                # Initialize shared query execution service
                self.query_service = QueryExecutionService(self.neptune_client)
                
                print(self.formatter.format_info("✅ Connection validated successfully!"))
                return True
                
            except Exception as e:
                error_msg = str(e)
                print(self.formatter.format_error(f"Connection failed: {error_msg}", "Connection Error"))
                
                if attempt < max_retries - 1:
                    retry = input(f"\n🤔 Retry connection? [Y/n]: ").strip().lower()
                    if retry in ['n', 'no']:
                        break
                    await asyncio.sleep(2)  # Brief delay before retry
        
        print("\n❌ Unable to establish Neptune connection. Please check your configuration and network.")
        return False
    
    def select_query_language(self) -> QueryLanguage:
        """Allow user to select query language."""
        print(f"\n📝 Query Language Selection")
        print("-" * 30)
        print("1. SPARQL (default) - RDF/Semantic queries")
        print("2. Gremlin - Graph traversal queries")  
        print("3. OpenCypher - Cypher-style queries")
        
        while True:
            try:
                choice = input(f"\nSelect language [1]: ").strip()
                
                if not choice or choice == '1':
                    return QueryLanguage.SPARQL
                elif choice == '2':
                    print(self.formatter.format_info("⚠️  Gremlin support coming in Phase 2"))
                    return QueryLanguage.SPARQL  # Fallback for now
                elif choice == '3':
                    print(self.formatter.format_info("⚠️  OpenCypher support coming in Phase 2"))
                    return QueryLanguage.SPARQL  # Fallback for now
                else:
                    print("❌ Please choose 1, 2, or 3")
                    
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                sys.exit(0)
    
    async def show_schema_setup_choice(self) -> bool:
        """Show schema setup options and handle discovery if chosen.
        
        Returns:
            True if user chose discovery, False if using existing schema
        """
        print(f"\n⚙️ Schema Configuration")
        print("=" * 40)
        print("Choose your setup:")
        print("1. 🔍 Discover Database Schema - AI explores your database structure")
        print("2. 📄 Use Existing Schema - Continue with schema/user_schema.json")
        
        while True:
            try:
                choice = input(f"\nChoose option [2]: ").strip()
                
                if not choice or choice == '2':
                    print(self.formatter.format_info("Using existing schema configuration"))
                    return False
                elif choice == '1':
                    return await self.run_schema_discovery()
                else:
                    print("❌ Please choose 1 or 2")
                    
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                sys.exit(0)
    
    async def run_schema_discovery(self) -> bool:
        """Run AI-powered schema discovery.
        
        Returns:
            True if discovery completed successfully
        """
        print(f"\n🔍 AI Schema Discovery ({self.current_language.value})")
        print("=" * 50)
        print("AI will explore your Neptune database and generate schema/user_schema.json")
        print("This may take a few moments for large databases...")
        
        try:
            # Create schema discovery agent (using consolidated QueryLanguage enum)
            discovery_agent = SchemaDiscoveryAgent(self.neptune_client, self.current_language)
            
            # Run discovery with spinner
            async def discover():
                return await discovery_agent.discover_schema()
            
            success = await SpinnerManager.with_spinner(
                "🔍 AI discovering database structure...",
                discover,
                "clock"
            )
            
            if success:
                print(self.formatter.format_success("✅ Schema discovery completed!"))
                print("📄 Generated schema/user_schema.json with your database structure")
                print("🚀 Ready to start querying with AI assistance")
                return True
            else:
                print(self.formatter.format_error("Schema discovery failed", "Discovery Error"))
                print("📄 Falling back to existing schema configuration")
                return False
                
        except Exception as e:
            print(self.formatter.format_error(f"Discovery error: {str(e)}", "Schema Discovery"))
            print("📄 Falling back to existing schema configuration")
            return False

    def show_main_interface(self) -> None:
        """Display the main interface options."""
        print(f"\n🔍 {self.current_language.value} Query Interface")
        print("=" * 50)
        print("Choose your approach:")
        print("1. 📝 Execute Your Query - Write and run your own query")
        print("2. 🤖 Chat with AI - Describe what you want in natural language")
        print("3. 🔄 Change Language - Switch to different query language")
        print("4. 🚪 Exit")
        print("\nSpecial commands: /reset (database), /export (last results)")
        print("-" * 50)
    
    async def execute_user_query(self) -> None:
        """Allow user to input and execute their own query."""
        print(f"\n📝 {self.current_language.value} Query Input")
        print("=" * 40)
        print("Paste your query below (end with empty line):")
        print("Tip: Use Ctrl+C to cancel")
        
        query_lines = []
        try:
            while True:
                line = input()
                if not line.strip():
                    break
                query_lines.append(line)
        except KeyboardInterrupt:
            print("\n⏸️  Query input cancelled")
            return
        
        if not query_lines:
            print("❌ No query provided")
            return
        
        query = '\n'.join(query_lines)
        await self.execute_query(query, "User Query")
    
    async def chat_with_ai(self) -> None:
        """AI-powered natural language to query conversion with Neptune execution."""
        print(f"\n🤖 AI Query Assistant ({self.current_language.value})")
        print("=" * 50)
        print("Describe what you want to query in natural language.")
        print("The AI will generate, execute, and analyze the results for you!")
        print("\nExample: 'Find all standards from Texas for grade 6 mathematics'")
        
        try:
            natural_query = input("\n💬 Your request: ").strip()
            
            if not natural_query:
                print("❌ No request provided")
                return
            
            await self.process_ai_query(natural_query)
            
        except KeyboardInterrupt:
            print("\n⏸️  AI chat cancelled")
    
    async def process_ai_query(self, natural_query: str) -> None:
        """Process natural language query through AI with Neptune execution."""
        try:
            # Initialize AI generator if needed
            if not self.ai_generator and self.query_service:
                print(self.formatter.format_info("Initializing AI assistant..."))
                
                # Use shared QueryExecutionService
                self.ai_generator = AIQueryGenerator(self.query_service, self.current_language)
            
            # Process query with AI agent using streaming
            if not self.ai_generator:
                print(self.formatter.format_error("AI generator not initialized", "AI Assistant"))
                return
            
            ai_generator = self.ai_generator
            result = await ai_generator.process_natural_language_query(natural_query, streaming=True)
            
            # Display AI results
            print(f"\n🤖 AI Analysis Complete")
            print("=" * 40)
            print(f"💡 {result.explanation}")
            
            if result.query:
                print(f"\n📝 Generated {result.query_language.upper()} Query:")
                print("-" * 30)
                print(result.query)
                print("-" * 30)
            
            # Display Neptune results if available
            if result.results:
                self.last_results = result.results
                self.last_query_type = "ai_generated_query"
                
                # Cap display at 10 rows, suggest CSV for larger datasets
                display_results = result.results[:10] if len(result.results) > 10 else result.results
                display_format = getattr(result, 'display_format', 'table')
                display_config = getattr(result, 'display_config', None)
                print(self.formatter.format_sparql_results(display_results, "AI Query Results", display_format, display_config))
                
                if len(result.results) > 10:
                    print(f"\n📄 Showing first 10 of {len(result.results)} results. Ask me to 'export to CSV' to see all data.")
                
                # Show AI insights if available  
                if result.insights:
                    print(f"\n🧠 AI Insights:")
                    print(result.insights)
                
                if result.suggestions:
                    print(f"\n💡 Suggestions:")
                    for suggestion in result.suggestions:
                        print(f"  • {suggestion}")
            else:
                print(f"\n⚠️  No results found")
                if result.insights:
                    print(f"💭 {result.insights}")
            
            # Continue natural conversation - no menu interruptions
            await self.continue_ai_conversation()
                    
        except Exception as e:
            print(self.formatter.format_error(f"AI processing failed: {str(e)}", "AI Assistant"))
    
    async def continue_ai_conversation(self) -> None:
        """Continue natural conversation with AI - supports special commands."""
        while True:
            try:
                # Simple natural input - no options or menus
                follow_up = input("\n💬 ").strip()
                
                if not follow_up:
                    break  # Empty input returns to main interface
                
                # Handle special commands first
                if follow_up.startswith('/'):
                    if follow_up == '/export':
                        await self.export_results()
                        continue  # Stay in conversation after export
                    elif follow_up == '/reset':
                        await self.database_reset()
                        continue  # Stay in conversation after reset
                    elif follow_up in ['/back', '/quit', '/exit']:
                        print("👋 Ending AI conversation...")
                        break  # Exit AI chat mode
                    else:
                        print(f"❌ Unknown command: {follow_up}")
                        print("Available commands: /export (CSV export), /reset (database reset), /back (exit AI chat)")
                        continue
                
                # Handle common exit phrases
                if follow_up.lower() in ['quit', 'exit', 'back', 'done', 'stop']:
                    break
                
                # Process as new AI query
                await self.process_ai_query(follow_up)
                
            except KeyboardInterrupt:
                print("\n⏸️  AI conversation ended")
                break
    
    async def execute_query(self, query: str, query_source: str) -> None:
        """Execute a query and display results using shared service."""
        if not self.query_service:
            print(self.formatter.format_error("Query service not available", "Query Execution"))
            return
        
        try:
            # Execute query using shared service with spinner
            query_service = self.query_service
            if not query_service:
                raise ValueError("Query service not available")
            
            # Capture current language for async function
            current_lang = self.current_language
                
            async def run_query():
                return await query_service.execute_query(
                    query, 
                    current_lang, 
                    for_ai_context=False  # Don't truncate for shell display
                )
            
            result = await SpinnerManager.query_execution(run_query)
            
            if result['success'] and result['results']:
                # Display all results (service handles complete dataset)
                print(self.formatter.format_sparql_results(result['results'], query_source))
                
                # Show summary
                total_results = result['result_count']
                displayed_results = len(result['results'])
                
                if total_results > displayed_results:
                    print(f"\n📄 Showing {displayed_results} of {total_results} results. Use /export to save all data.")
                
                # Post-query options
                await self.show_post_query_options()
            else:
                print(f"\n⚠️  {query_source} executed successfully but returned no results")
                
        except Exception as e:
            print(self.formatter.format_error(f"Query execution failed: {str(e)}", query_source))
    
    async def show_post_query_options(self) -> None:
        """Show options after successful query execution using shared service."""
        if not self.query_service:
            return
        
        # Get result summary from shared service
        summary = self.query_service.get_last_results_summary()
        if not summary['has_results']:
            return
        
        print(f"\n📊 Found {summary['result_count']} result(s)")
        
        while True:
            choice = input("\n[E]xport CSV, [N]ew Query, [M]ain Menu, [Enter] to continue: ").strip().upper()
            
            if choice == 'E':
                await self.export_results()
                break
            elif choice == 'N':
                return  # Will return to main interface
            elif choice == 'M' or not choice:
                break
            else:
                print("❌ Please choose E, N, M, or press Enter")
    
    async def export_results(self) -> None:
        """Export last query results to CSV using shared service."""
        if not self.query_service:
            print("❌ No query service available")
            return
        
        # Check if we have results to export
        summary = self.query_service.get_last_results_summary()
        if not summary['has_results']:
            print("❌ No results to export")
            return
        
        try:
            # Use shared service for export (always exports complete dataset)
            query_service = self.query_service
            if not query_service:
                raise ValueError("Query service not available")
                
            async def do_export():
                return query_service.export_last_results("shell_query")
            
            export_result = await SpinnerManager.csv_export(do_export, "shell_export")
            
            if export_result['success']:
                print(self.formatter.format_info(f"✅ Results exported to: {export_result['filepath']}"))
                print(f"📊 Exported {export_result['record_count']} rows ({export_result['file_size_mb']} MB)")
            else:
                print(self.formatter.format_error(f"Export failed: {export_result['error']}", "CSV Export"))
                
        except Exception as e:
            print(self.formatter.format_error(f"Export failed: {str(e)}", "CSV Export"))
    
    async def handle_special_command(self, command: str) -> bool:
        """Handle special commands like /reset, /export."""
        if command == '/reset':
            await self.database_reset()
            return True
        elif command == '/export':
            await self.export_results()
            return True
        return False
    
    async def database_reset(self) -> None:
        """Handle database reset with confirmations."""
        if not self.neptune_client:
            print(self.formatter.format_error("Neptune client not available", "Database Reset"))
            return
        
        print(f"\n💥 DATABASE RESET")
        print("=" * 40)
        print("⚠️  WARNING: This will delete ALL data!")
        
        # Double confirmation
        confirm1 = input("\nType 'yes' to continue: ").strip().lower()
        if confirm1 != 'yes':
            print("🚫 Reset cancelled")
            return
        
        confirm2 = input("Type 'DELETE ALL DATA' to confirm: ").strip()
        if confirm2 != 'DELETE ALL DATA':
            print("🚫 Reset cancelled - confirmation text incorrect")
            return
        
        try:
            neptune_client = self.neptune_client
            result = await SpinnerManager.with_spinner(
                "💥 Resetting Neptune database...",
                lambda: neptune_client.reset_database(),
                "earth"
            )
            
            if result:
                print(self.formatter.format_info("✅ Database reset completed"))
                # Clear results from shared service
                if self.query_service:
                    self.query_service.clear_results()
            else:
                print(self.formatter.format_error("Reset failed", "Database Reset"))
                
        except Exception as e:
            print(self.formatter.format_error(f"Reset failed: {str(e)}", "Database Reset"))
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        if self.neptune_client and self.connected:
            try:
                await self.neptune_client.close()
            except Exception:
                pass
        
        if self.ai_generator:
            try:
                await self.ai_generator.close()
            except Exception:
                pass
    
    async def run(self) -> None:
        """Main application loop."""
        try:
            # 1. Validate connection
            if not await self.validate_connection():
                return
            
            # 2. Select query language
            self.current_language = self.select_query_language()
            print(self.formatter.format_info(f"Selected language: {self.current_language.value}"))
            
            # 3. Schema setup or query interface choice
            if await self.show_schema_setup_choice():
                # User chose schema discovery - proceed to query interface after discovery
                pass
            
            # 4. Main interaction loop
            while True:
                try:
                    self.show_main_interface()
                    choice = input("Choose option [1]: ").strip()
                    
                    if not choice or choice == '1':
                        await self.execute_user_query()
                    elif choice == '2':
                        await self.chat_with_ai()
                    elif choice == '3':
                        self.current_language = self.select_query_language()
                        print(self.formatter.format_info(f"Switched to: {self.current_language.value}"))
                    elif choice == '4' or choice.lower() in ['quit', 'exit', 'q']:
                        break
                    elif choice.startswith('/'):
                        if not await self.handle_special_command(choice):
                            print(f"❌ Unknown command: {choice}")
                    else:
                        print("❌ Please choose 1, 2, 3, or 4")
                        
                except KeyboardInterrupt:
                    print("\n\n👋 Goodbye!")
                    break
                except Exception as e:
                    print(self.formatter.format_error(f"Unexpected error: {str(e)}", "System Error"))
        
        finally:
            await self.cleanup()


async def main():
    """Application entry point."""
    shell = NeptuneQueryShell()
    await shell.run()


if __name__ == "__main__":
    asyncio.run(main())
