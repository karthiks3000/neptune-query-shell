"""Connection management for Neptune database."""

import asyncio
import json
from typing import Any, Optional

import aiohttp
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import ReadOnlyCredentials
from loguru import logger


class ConnectionManager:
    """Manages connection to Neptune database."""

    def __init__(self, endpoint: str, region: str, port: int = 8182, session=None):
        """Initialize Neptune connection manager.

        Args:
            endpoint (str): Neptune instance endpoint URL
            region (str): AWS region
            port (int, optional): Neptune port number. Defaults to 8182.
            session (aiohttp.ClientSession, optional): Existing client session to use
        """
        self.endpoint = endpoint
        self.port = port
        self.region = region
        self.session = None
        self.sparql_endpoint = None

        # Session ownership: we own it if we create it, otherwise caller owns it
        if session is None:
            self.client_session = None
            self._owns_session = True
        else:
            self.client_session = session
            self._owns_session = False
            
        self.logger = logger.bind(context="NeptuneConnectionManager")

    async def init_sparql(self) -> None:
        """Initialize SPARQL connection."""
        try:
            # Initialize SPARQL session with container credentials
            self.session = boto3.Session()
            self.credentials = self.session.get_credentials()

            # Create aiohttp session if one wasn't provided and we don't have one yet
            if self.client_session is None or self.client_session.closed:
                self.client_session = aiohttp.ClientSession()
                self._owns_session = True  # We created this session, so we own it

            self.sparql_endpoint = f"https://{self.endpoint}:{self.port}/sparql"
        except Exception as e:
            self.logger.error(f"Failed to initialize SPARQL connection: {e}")
            if self._owns_session:  # Only close if we own the session
                await self.close()
            self.client_session = None
            self.sparql_endpoint = None

    async def close(self) -> None:
        """Close aiohttp session if we own it."""
        if (
            self._owns_session
            and self.client_session
            and not self.client_session.closed
        ):
            await self.client_session.close()
            self.client_session = None

    async def execute_sparql(
        self, query: str, params: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        """Execute a SPARQL query on Neptune.

        Args:
            query: The SPARQL query string
            params: Query parameters (optional)

        Returns:
            The query result as a dictionary

        Raises:
            Exception: On query execution failure
        """
        if self.client_session is None or self.client_session.closed:
            await self.init_sparql()

        if not self.client_session or not self.sparql_endpoint:
            raise Exception("SPARQL connection not initialized")

        # If no params provided, use empty dict
        if params is None:
            params = {}

        # Apply parameters to the SPARQL query
        # SPARQL uses different parameter binding than OpenCypher
        formatted_query = query
        for k, v in params.items():
            if v is None:
                v = ""  # Empty string instead of None

            # Format value based on type
            if isinstance(v, str):
                # Escape quotes in strings
                v = v.replace('"', '\\"')
                formatted_value = f'"{v}"'
            elif isinstance(v, bool):
                formatted_value = str(v).lower()
            elif isinstance(v, (int, float)):
                formatted_value = str(v)
            elif isinstance(v, list):
                # For lists, we'll join with commas and wrap in parentheses
                formatted_items = []
                for item in v:
                    if isinstance(item, str):
                        item = item.replace('"', '\\"')
                        formatted_items.append(f'"{item}"')
                    else:
                        formatted_items.append(str(item))
                formatted_value = "(" + ", ".join(formatted_items) + ")"
            else:
                formatted_value = f'"{str(v)}"'

            # Replace parameter in query
            placeholder = f"${k}"
            formatted_query = formatted_query.replace(placeholder, formatted_value)

        try:
            # Create the request for signing
            method = "POST"
            url = self.sparql_endpoint

            # Determine the appropriate content type based on the query type
            content_type = "application/sparql-query"
            # Check if this is an update operation (INSERT, DELETE, etc.)
            if any(
                keyword in formatted_query.upper()
                for keyword in ["INSERT", "DELETE", "CLEAR", "CREATE", "DROP", "LOAD"]
            ):
                content_type = "application/sparql-update"

            self.logger.debug(f"Submitting query {formatted_query}")
            # Sign the request with the appropriate content type
            request = AWSRequest(
                method=method,
                url=url,
                data=formatted_query,  # Send the query directly in the request body
                headers={"Content-Type": content_type},
            )
            credentials = self.credentials.get_frozen_credentials()
            read_only_credentials = ReadOnlyCredentials(
                credentials.access_key, credentials.secret_key, credentials.token
            )
            SigV4Auth(read_only_credentials, "neptune-db", self.region).add_auth(
                request
            )

            # Execute the request with signed headers and multiple retries
            max_retries = 3
            retry_count = 0

            while retry_count < max_retries:
                try:
                    # Increase timeout for each retry, with extra time for DELETE operations
                    base_timeout = 120
                    if any(keyword in formatted_query.upper() for keyword in ["DELETE", "CLEAR"]):
                        base_timeout = 300  # 5 minutes for DELETE operations
                    
                    timeout_seconds = base_timeout * (retry_count + 1)  # 300s, 600s, 900s for DELETE
                    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

                    if retry_count > 0:
                        # Add exponential backoff delay between retries
                        delay = 5 * (2 ** (retry_count - 1))  # 5s, 10s, 20s
                        self.logger.debug(
                            f"Retry {retry_count}/{max_retries-1}: Waiting {delay}s before retry..."
                        )
                        await asyncio.sleep(delay)
                        self.logger.debug(f"Retrying with {timeout_seconds}s timeout...")

                    async with self.client_session.post(
                        url,
                        data=formatted_query,  # Send the query directly in the request body
                        headers=dict(request.headers),
                        timeout=timeout,
                    ) as response:
                        response.raise_for_status()

                        # Get content-type from response headers
                        content_type = response.headers.get("Content-Type", "")

                        if not content_type or "json" not in content_type.lower():
                            # For non-JSON responses like DELETE operations that return no content
                            if response.status == 200:
                                # Return a standard success response
                                return {"status": "success", "code": 200}
                            else:
                                # Try to get text content if possible
                                text = await response.text()
                                return {
                                    "status": "error",
                                    "code": response.status,
                                    "message": text,
                                }

                        # For JSON responses, parse as normal
                        result = await response.json()

                        # Transform SPARQL results to a consistent format
                        if "results" in result and "bindings" in result["results"]:
                            # Convert SPARQL result format to our standard format
                            transformed_results = []
                            for binding in result["results"]["bindings"]:
                                row = {}
                                for var_name, value in binding.items():
                                    # Extract the actual value from the SPARQL result format
                                    row[var_name] = value.get("value")
                                transformed_results.append(row)

                            return {"results": transformed_results}

                        # For ASK queries
                        if "boolean" in result:
                            return {"results": [{"boolean": result["boolean"]}]}

                        # For other types of results
                        return result

                except asyncio.TimeoutError:
                    retry_count += 1
                    if retry_count < max_retries:
                        self.logger.warning(
                            f"Request timed out after {timeout_seconds} seconds. "
                            f"Retry {retry_count}/{max_retries-1}..."
                        )
                    else:
                        self.logger.error(
                            f"Request failed after {max_retries} attempts with maximum timeout "
                            f"of {timeout_seconds} seconds"
                        )
                        raise

                except aiohttp.ClientConnectorError as e:
                    # Connection errors should be retried
                    retry_count += 1
                    if retry_count < max_retries:
                        self.logger.warning(
                            f"Connection error: {str(e)}. Retry {retry_count}/{max_retries-1}..."
                        )
                    else:
                        self.logger.error(
                            f"Connection failed after {max_retries} attempts: {str(e)}"
                        )
                        raise

                except Exception as e:
                    # For other errors, don't retry but log them properly
                    self.logger.error(
                        f"Request failed with non-retryable error: {str(e)}"
                    )
                    raise

        except aiohttp.ClientError as e:
            response_text = "No response"
            # Check if this is a ClientResponseError which has response attribute
            if isinstance(e, aiohttp.ClientResponseError):
                try:
                    response_text = str(e)
                except Exception:
                    response_text = "Error retrieving response text"

            self.logger.error(f"Neptune Query Error: {e}")
            self.logger.error(f"Query: {formatted_query}")
            self.logger.error(f"Response: {response_text}")

            raise Exception(f"Neptune query failed: {response_text}") from e
        
        # This should never be reached - if we get here, something is seriously wrong
        raise Exception("Unexpected code path reached in execute_sparql - this indicates a bug")

    async def initiate_database_reset(self) -> str:
        """Initiate database reset and get reset token.
        
        Returns:
            Reset token string for use in performDatabaseReset
            
        Raises:
            Exception: On API call failure
        """
        if self.client_session is None or self.client_session.closed:
            await self.init_sparql()

        if not self.client_session:
            raise Exception("Connection not initialized")

        try:
            # Create the reset initiation request
            system_url = f"https://{self.endpoint}:{self.port}/system"
            
            request_data = {"action": "initiateDatabaseReset"}
            
            # Create the request for signing
            request = AWSRequest(
                method="POST",
                url=system_url,
                data=json.dumps(request_data),
                headers={"Content-Type": "application/json"},
            )
            
            # Sign the request
            credentials = self.credentials.get_frozen_credentials()
            read_only_credentials = ReadOnlyCredentials(
                credentials.access_key, credentials.secret_key, credentials.token
            )
            SigV4Auth(read_only_credentials, "neptune-db", self.region).add_auth(request)
            
            # Execute the request
            timeout = aiohttp.ClientTimeout(total=60)  # 1 minute should be enough for token request
            
            self.logger.info("Initiating Neptune database reset...")
            
            async with self.client_session.post(
                system_url,
                data=json.dumps(request_data),
                headers=dict(request.headers),
                timeout=timeout,
            ) as response:
                response.raise_for_status()
                result = await response.json()
                
                if "payload" in result and "token" in result["payload"]:
                    token = result["payload"]["token"]
                    self.logger.info(f"Reset token obtained: {token[:8]}...")
                    return token
                else:
                    raise Exception(f"Invalid response format: {result}")
                    
        except Exception as e:
            self.logger.error(f"Failed to initiate database reset: {str(e)}")
            raise Exception(f"Failed to get reset token: {str(e)}") from e

    async def perform_database_reset(self, token: str) -> bool:
        """Perform database reset using the provided token.
        
        Args:
            token: Reset token from initiate_database_reset
            
        Returns:
            True if reset was successful
            
        Raises:
            Exception: On API call failure
        """
        if self.client_session is None or self.client_session.closed:
            await self.init_sparql()

        if not self.client_session:
            raise Exception("Connection not initialized")

        try:
            # Create the reset execution request
            system_url = f"https://{self.endpoint}:{self.port}/system"
            
            request_data = {
                "action": "performDatabaseReset",
                "token": token
            }
            
            # Create the request for signing
            request = AWSRequest(
                method="POST",
                url=system_url,
                data=json.dumps(request_data),
                headers={"Content-Type": "application/json"},
            )
            
            # Sign the request
            credentials = self.credentials.get_frozen_credentials()
            read_only_credentials = ReadOnlyCredentials(
                credentials.access_key, credentials.secret_key, credentials.token
            )
            SigV4Auth(read_only_credentials, "neptune-db", self.region).add_auth(request)
            
            # Execute the request with longer timeout for reset operation
            timeout = aiohttp.ClientTimeout(total=600)  # 10 minutes for reset
            
            self.logger.info("Performing Neptune database reset...")
            
            async with self.client_session.post(
                system_url,
                data=json.dumps(request_data),
                headers=dict(request.headers),
                timeout=timeout,
            ) as response:
                response.raise_for_status()
                result = await response.json()
                
                if result.get("status") == "200 OK":
                    self.logger.info("Database reset completed successfully")
                    return True
                else:
                    self.logger.error(f"Reset failed with response: {result}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Failed to perform database reset: {str(e)}")
            raise Exception(f"Database reset failed: {str(e)}") from e

    async def fast_reset_database(self) -> bool:
        """Perform complete database reset using Neptune fast reset API.
        
        This is a convenience method that combines both steps of the fast reset process.
        
        Returns:
            True if reset was successful
            
        Raises:
            Exception: On API call failure
        """
        try:
            # Step 1: Get reset token
            token = await self.initiate_database_reset()
            
            # Step 2: Perform reset with token
            result = await self.perform_database_reset(token)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Fast reset failed: {str(e)}")
            raise
