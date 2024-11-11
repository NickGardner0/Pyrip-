"""
PyripApp Module

This module provides a class `PyripApp` for interacting with the Pyrip engine API.
It includes methods to scrape URLs, perform searches, initiate and monitor crawl jobs,
and check the status of these jobs. The module uses requests for HTTP communication
and handles retries for certain HTTP status codes.

Classes:
    - PyripApp: Main class for interacting with the Pyrip engine API.
"""
import logging
import os
import time
from typing import Any, Dict, Optional, List
import json

import requests
import websockets

logger: logging.Logger = logging.getLogger("pyrip")

class PyripApp:
    def __init__(self, api_key: Optional[str] = None, api_url: Optional[str] = None) -> None:
        """
        Initialize the PyripApp instance with API key, API URL.

        Args:
            api_key (Optional[str]): API key for authenticating with the Pyrip engine API.
            api_url (Optional[str]): Base URL for the Pyrip engine API.
        """
        self.api_key = api_key or os.getenv('PYRIP_API_KEY')
        self.api_url = api_url or os.getenv('PYRIP_API_URL', 'https://api.pyrip.dev')
        if self.api_key is None:
            logger.warning("No API key provided")
            raise ValueError('No API key provided')
        logger.debug(f"Initialized PyripApp with API key: {self.api_key}")

    def scrape_url(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Scrape the specified URL using the Pyrip engine API.

        Args:
            url (str): The URL to scrape.
            params (Optional[Dict[str, Any]]): Additional parameters for the scrape request.

        Returns:
            Any: The scraped data if the request is successful.

        Raises:
            Exception: If the scrape request fails.
        """

        headers = self._prepare_headers()

        # Prepare the base scrape parameters with the URL
        scrape_params = {'url': url}

        # If there are additional params, process them
        if params:
            # Handle extract (for v1)
            extract = params.get('extract', {})
            if extract:
                if 'schema' in extract and hasattr(extract['schema'], 'schema'):
                    extract['schema'] = extract['schema'].schema()
                scrape_params['extract'] = extract

            # Include any other params directly at the top level of scrape_params
            for key, value in params.items():
                if key not in ['extract']:
                    scrape_params[key] = value

        endpoint = f'/v1/scrape'
        # Make the POST request with the prepared headers and JSON data
        response = requests.post(
            f'{self.api_url}{endpoint}',
            headers=headers,
            json=scrape_params,
        )
        if response.status_code == 200:
            response = response.json()
            if response['success'] and 'data' in response:
                return response['data']
            elif "error" in response:
                raise Exception(f'Failed to scrape URL. Error: {response["error"]}')
            else:
                raise Exception(f'Failed to scrape URL. Error: {response}')
        else:
            self._handle_error(response, 'scrape URL')

    def search(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Perform a search using the Pyrip engine API.

        Args:
            query (str): The search query.
            params (Optional[Dict[str, Any]]): Additional parameters for the search request.

        Returns:
            Any: The search results if the request is successful.

        Raises:
            NotImplementedError: If the search request is attempted on API version v1.
            Exception: If the search request fails.
        """
        raise NotImplementedError("Search is not supported in v1.")

    def crawl_url(self, url: str,
                  params: Optional[Dict[str, Any]] = None,
                  poll_interval: Optional[int] = 2,
                  idempotency_key: Optional[str] = None) -> Any:
        """
        Initiate a crawl job for the specified URL using the Pyrip engine API.

        Args:
            url (str): The URL to crawl.
            params (Optional[Dict[str, Any]]): Additional parameters for the crawl request.
            poll_interval (Optional[int]): Time in seconds between status checks when waiting for job completion. Defaults to 2 seconds.
            idempotency_key (Optional[str]): A unique uuid key to ensure idempotency of requests.

        Returns:
            Dict[str, Any]: A dictionary containing the crawl results. The structure includes:
                - 'success' (bool): Indicates if the crawl was successful.
                - 'status' (str): The final status of the crawl job (e.g., 'completed').
                - 'completed' (int): Number of scraped pages that completed.
                - 'total' (int): Total number of scraped pages.
                - 'creditsUsed' (int): Estimated number of API credits used for this crawl.
                - 'expiresAt' (str): ISO 8601 formatted date-time string indicating when the crawl data expires.
                - 'data' (List[Dict]): List of all the scraped pages.

        Raises:
            Exception: If the crawl job initiation or monitoring fails.
        """
        endpoint = f'/v1/crawl'
        headers = self._prepare_headers(idempotency_key)
        json_data = {'url': url}
        if params:
            json_data.update(params)
        response = self._post_request(f'{self.api_url}{endpoint}', json_data, headers)
        if response.status_code == 200:
            id = response.json().get('id')
            return self._monitor_job_status(id, headers, poll_interval)

        else:
            self._handle_error(response, 'start crawl job')

    # (Remaining code has similar replacements, not shown for brevity)

class CrawlWatcher:
    def __init__(self, id: str, app: PyripApp):
        self.id = id
        self.app = app
        self.data: List[Dict[str, Any]] = []
        self.status = "scraping"
        self.ws_url = f"{app.api_url.replace('http', 'ws')}/v1/crawl/{id}"
        self.event_handlers = {
            'done': [],
            'error': [],
            'document': []
        }

    # (Remaining CrawlWatcher methods continue with similar replacements)
