import requests
import time
import json
from datetime import datetime, timedelta


class FabricPowerBIClient:
    """
    A client for interacting with the Microsoft Fabric and Power BI REST APIs.

    Attributes:
        BASE_URLS (dict): Base URLs for Fabric and Power BI API endpoints.
        tenant_id (str, optional): Azure tenant ID for authentication.
        client_id (str, optional): Client ID for authentication.
        client_secret (str, optional): Client secret for authentication.
        username (str, optional): Username for authentication.
        password (str, optional): Password for authentication.
        client_type (str): The type of client, either 'fabric' or 'powerbi'.
        access_token (str, optional): Cached access token for API requests.
        token_expiration (datetime, optional): Expiration time for the access token.
        is_custom_client (bool): Indicates if a custom client is used.
        client (object, optional): Instance of the sempy client if used.

    Methods:
        __init__: Initializes the client with optional authentication parameters.
        _initialize_sempy_client: Initializes the sempy client if custom client credentials are not provided.
        _get_access_token: Obtains a new access token using client credentials.
        _make_request_with_retry: Makes an API request with retry logic for certain status codes.
        request: Makes an API request using the custom client or sempy client.
        request_with_client: Class method to make a request with an existing client instance.
    """

    BASE_URLS = {
        "fabric": "https://api.fabric.microsoft.com",
        "powerbi": "https://api.powerbi.com",
    }

    def __init__(
        self,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        username=None,
        password=None,
        client_type="fabric",
    ):
        """
        Initializes the FabricPowerBIClient with optional authentication parameters.

        Args:
            tenant_id (str, optional): Azure tenant ID.
            client_id (str, optional): Client ID for OAuth authentication.
            client_secret (str, optional): Client secret for OAuth authentication.
            username (str, optional): Username for OAuth password grant flow.
            password (str, optional): Password for OAuth password grant flow.
            client_type (str, optional): Type of client, either 'fabric' or 'powerbi'. Defaults to 'fabric'.
        """
        self.client_type = client_type.lower()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.access_token = None
        self.token_expiration = None

        if tenant_id and client_id and (client_secret or (username and password)):
            self.is_custom_client = True
            self.client = requests.Session()
        else:
            self.is_custom_client = False
            self._initialize_sempy_client()

    def _initialize_sempy_client(self):
        """
        Initializes the sempy client if custom client credentials are not provided.

        Raises:
            ImportError: If the sempy library or the requested client type is not available.
        """
        try:
            from sempy import fabric

            self.client = getattr(
                fabric, f"{self.client_type.capitalize()}RestClient"
            )()
        except ImportError:
            raise ImportError(
                "The Semantic Link library is not installed. Ensure you are running in a Fabric environment with the library installed. "
                "If you are not in a Fabric environment, provide the tenant ID, client ID, and either client secret or username and password."
            )
        except AttributeError:
            raise ImportError(
                f"The requested client type '{self.client_type}' is not available in the 'sempy' library."
            )

    def _get_access_token(self):
        """
        Obtains a new access token using client credentials.

        Returns:
            str: The access token.

        Raises:
            ValueError: If neither client_secret nor username/password is provided.
            requests.exceptions.RequestException: If the token request fails.
        """
        # Check if the current token is still valid
        if self.access_token and datetime.now() < self.token_expiration:
            return self.access_token

        # Define constants
        DEFAULT_EXPIRES_IN = 3600
        TOKEN_URL = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        )

        # Determine scope from client type
        if self.client_type in self.BASE_URLS:
            base_url = self.BASE_URLS[self.client_type]
            scope = f"{base_url.rstrip('/')}/.default"
        else:
            raise ValueError("Invalid client_type. Must be 'fabric' or 'powerbi'.")

        # Determine payload based on credentials provided
        if self.client_secret:
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": scope,
            }
        elif self.username and self.password:
            payload = {
                "grant_type": "password",
                "client_id": self.client_id,
                "username": self.username,
                "password": self.password,
                "scope": scope,
            }
        else:
            raise ValueError(
                "Either client_secret or both username and password must be provided"
            )

        # Request the access token
        response = self.client.post(TOKEN_URL, data=payload)
        response.raise_for_status()
        token_data = response.json()

        # Set the access token and its expiration
        self.access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", DEFAULT_EXPIRES_IN)
        self.token_expiration = datetime.now() + timedelta(seconds=expires_in)

        return self.access_token

    def _make_request_with_retry(self, request_func, *args, **kwargs):
        """
        Makes a request with retry logic for specific error codes.

        Args:
            request_func (callable): The function to make the request.
            *args: Positional arguments to pass to the request function.
            **kwargs: Keyword arguments to pass to the request function.

        Returns:
            requests.Response: The response object.

        Raises:
            Exception: If the maximum number of retries is reached or an unrecoverable error occurs.
        """
        max_retries = 5
        retry_delay = 10  # Initial delay in seconds
        retried_401 = False  # Flag to track 401 retry

        for attempt in range(max_retries):
            response = request_func(*args, **kwargs)

            if response.status_code < 400:
                return response

            if response.status_code == 401 and not retried_401:
                # Unauthorized, try refreshing the token once
                retried_401 = True
                self.access_token = None
                kwargs["headers"]["Authorization"] = f"Bearer {self._get_access_token()}"
                continue  # Retry the request with the new token

            if response.status_code == 429 and attempt < max_retries - 1:
                # Too Many Requests, use Retry-After header if available
                retry_after = response.headers.get("Retry-After")
                retry_delay = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else retry_delay
                )
                print(f"Rate limit exceeded. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue

            # Raise an exception for other status codes indicating an error
            response.raise_for_status()

        raise Exception("Max retries reached. Request failed.")

    def request(self, method, url, return_json=False, **kwargs):
        """
        Makes an HTTP request using either the custom client or the sempy client.
        Automatically handles pagination if a continuationToken is present in the response.
        Returns the final response object with only consolidated data in the content.

        Args:
            method (str): The HTTP method (e.g., 'GET', 'POST').
            url (str): The URL endpoint (relative or full).
            return_json (bool): If True, returns the JSON data directly instead of the response object.
            **kwargs: Additional arguments passed to the request function.

        Returns:
            requests.Response or dict: The response object or JSON data, depending on return_json.
        """
        # Determine the base URL based on the client type
        base_url = self.BASE_URLS.get(self.client_type)
        if not base_url:
            raise ValueError(
                f"Invalid client_type '{self.client_type}'. Must be 'fabric' or 'powerbi'."
            )

        # Prepend base URL if it's not already included in the URL
        if not url.startswith(base_url):
            url = f"{base_url.rstrip('/')}/{url.lstrip('/')}"

        if self.is_custom_client:
            # Set the Authorization header for custom clients
            headers = kwargs.setdefault("headers", {})
            headers["Authorization"] = f"Bearer {self._get_access_token()}"

        request_func = getattr(self.client, method.lower())
        all_items = []
        params = kwargs.get("params", {})

        while True:
            response = self._make_request_with_retry(request_func, url, **kwargs)

            try:
                data = response.json()
            except ValueError:
                data = {}

            continuation_token = data.get("continuationToken")

            # Extract and accumulate items
            items = data.get("value") or data.get("data") or []
            all_items.extend(items)

            if not continuation_token:
                break

            # Set continuationToken in params for the next request
            params["continuationToken"] = continuation_token
            kwargs["params"] = params

        if return_json:
            return all_items if all_items else data
        else:
            if all_items:
                response._content = json.dumps(all_items).encode("utf-8")
            return response

    @classmethod
    def request_with_client(cls, method, url, return_json=False, client=None, **kwargs):
        """
        Class method to make a request with an existing client instance.

        Args:
            method (str): HTTP method (e.g., 'GET', 'POST').
            url (str): The endpoint URL.
            client (FabricPowerBIClient, optional): An existing client instance. Defaults to None.
            **kwargs: Additional arguments to pass to the request function.

        Returns:
            requests.Response: The response from the API request.
        """
        if client is None:
            client = cls()
        return client.request(method, url, return_json, **kwargs)