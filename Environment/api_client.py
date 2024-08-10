import requests
import time
import json
from datetime import datetime, timedelta
from threading import Lock


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
        client_type (str): The type of client, either 'FabricRestClient' or 'PowerBIRestClient'.
        access_token (str, optional): Cached access token for API requests.
        token_expiration (datetime, optional): Expiration time for the access token.
        token_lock (Lock): A threading lock to manage concurrent access to the token.
        client (requests.Session or sempy client): Instance of the requests.Session or sempy client.

    Methods:
        __init__: Initializes the client with optional authentication parameters.
        _initialize_custom_client: Initializes the custom client and sets up the Authorization header.
        _initialize_sempy_client: Initializes the sempy client if custom client credentials are not provided.
        _initialize_access_token: Obtains and initializes the access token during client initialization or when needed.
        _ensure_token_valid: Ensures the access token is still valid or refreshes it if close to expiration.
        _fetch_access_token: Fetches the access token from the OAuth2 endpoint.
        _build_token_payload: Builds the payload for the token request based on provided credentials.
        _get_scope: Determines the scope based on the client type.
        _generate_invalid_client_type_message: Generates the error message for an invalid client type.
        _make_request_with_retry: Makes an API request with retry logic for certain status codes.
        _update_authorization_header: Updates the Authorization header with the new access token.
        request: Makes an HTTP request using the custom client or sempy client, handling pagination if needed.
        request_with_client: Class method to make a request with an existing client instance.
    """

    BASE_URLS = {
        "FabricRestClient": "https://api.fabric.microsoft.com",
        "PowerBIRestClient": "https://api.powerbi.com",
    }

    def __init__(
        self,
        tenant_id=None,
        client_id=None,
        client_secret=None,
        username=None,
        password=None,
        client_type="FabricRestClient",
    ):
        """
        Initializes the FabricPowerBIClient with optional authentication parameters.

        Args:
            tenant_id (str, optional): Azure tenant ID for authentication.
            client_id (str, optional): Client ID for OAuth authentication.
            client_secret (str, optional): Client secret for OAuth authentication.
            username (str, optional): Username for OAuth password grant flow.
            password (str, optional): Password for OAuth password grant flow.
            client_type (str, optional): Type of client, either 'FabricRestClient' or 'PowerBIRestClient'. Defaults to 'FabricRestClient'.
        """
        self.client_type = client_type
        self.__tenant_id = tenant_id
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__username = username
        self.__password = password
        self.__access_token = None
        self.token_expiration = None
        self.token_lock = Lock()  # Lock to manage concurrent access to the toke

        if tenant_id and client_id and (client_secret or (username and password)):
            self._initialize_custom_client()
        else:
            self._initialize_sempy_client()

    def _initialize_custom_client(self):
        """
        Initializes the custom client, obtains the access token, and sets up the Authorization header.
        """
        self.client = requests.Session()
        self._initialize_access_token()

    def _initialize_sempy_client(self):
        """
        Initializes the sempy client if custom client credentials are not provided.

        Raises:
            ImportError: If the sempy library is not available or the requested client type is invalid.
        """
        try:
            from sempy import fabric

            self.client = getattr(fabric, self.client_type)()
        except ImportError:
            raise ImportError(
                "Please provide the tenant ID, client ID, and either client secret or username and password."
                "Otherwise, execute in Fabric environment having Semantic Link library installed."
            )
        except AttributeError:
            raise ImportError(self._generate_invalid_client_type_message())

    def _initialize_access_token(self):
        """
        Fetches a new access token, updates the Authorization header,
        and records the token's expiration time.
        """
        with self.token_lock:
            token_data = self._fetch_access_token()
            self.__access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 3600)
            self.token_expiration = datetime.now() + timedelta(seconds=expires_in)
            self._update_authorization_header()

    def _ensure_token_valid(self, threshold_seconds=300):
        """
        Checks if the token is nearing expiration and refreshes it if necessary.

        Args:
            threshold_seconds (int): Time in seconds before token expiration to refresh the token. Defaults to 300 seconds.
        """
        if hasattr(self, "client") and isinstance(self.client, requests.Session):
            with self.token_lock:
                if datetime.now() >= (
                    self.token_expiration - timedelta(seconds=threshold_seconds)
                ):
                    self._initialize_access_token()

    def _fetch_access_token(self):
        """
        Fetches the access token from the OAuth2 endpoint.

        Returns:
            dict: The token response containing the access token and its expiration time.
        """
        response = self.client.post(
            f"https://login.microsoftonline.com/{self.__tenant_id}/oauth2/v2.0/token",
            data=self._build_token_payload(),
        )
        response.raise_for_status()
        return response.json()

    def _build_token_payload(self):
        """
        Builds the payload for the token request based on provided credentials.

        Returns:
            dict: The payload for the token request.

        Raises:
            ValueError: If neither client_secret nor both username and password are provided.
        """
        if self.__client_secret:
            return {
                "grant_type": "client_credentials",
                "client_id": self.__client_id,
                "client_secret": self.__client_secret,
                "scope": self._get_scope(),
            }
        elif self.__username and self.__password:
            return {
                "grant_type": "password",
                "client_id": self.__client_id,
                "username": self.__username,
                "password": self.__password,
                "scope": self._get_scope(),
            }
        else:
            raise ValueError(
                "Either client_secret or both username and password must be provided"
            )

    def _get_scope(self):
        """
        Determines the scope based on the client type.

        Returns:
            str: The scope for the token request.
        """
        base_url = self.BASE_URLS[self.client_type]
        return f"{base_url.rstrip('/')}/.default"
    
    def _update_authorization_header(self):
        """
        Updates the Authorization header with the new access token.
        """
        self.client.headers.update({"Authorization": f"Bearer {self.__access_token}"})

    def _generate_invalid_client_type_message(self):
        """
        Generates the error message for an invalid client type.

        Returns:
            str: The error message indicating the invalid client type.
        """
        valid_types = ", ".join(self.BASE_URLS.keys())
        return f"Invalid client_type '{self.client_type}'. Must be {valid_types}."

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
        retry_delay = 10
        retried_401 = False  # Flag to track 401 retry

        for attempt in range(max_retries):
            self._ensure_token_valid()
            response = request_func(*args, **kwargs)

            if response.status_code < 400:
                return response

            if response.status_code == 401 and not retried_401:
                # Unauthorized, try refreshing the token once
                retried_401 = True
                self._initialize_access_token()
                continue  # Retry the request with the new token

            if response.status_code == 429 and attempt < max_retries - 1:
                # Too Many Requests, use Retry-After header if available
                retry_after = response.headers.get("Retry-After")
                retry_delay = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else retry_delay
                )
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
            raise ValueError(self._generate_invalid_client_type_message())

        # Prepend base URL if it's not already included in the URL
        if not url.startswith(base_url):
            url = f"{base_url.rstrip('/')}/{url.lstrip('/')}"

        request_func = getattr(self.client, method.lower())
        all_items, params = [], kwargs.get("params", {})

        while True:
            response = self._make_request_with_retry(request_func, url, **kwargs)

            try:
                data = response.json() or {}
            except ValueError:
                data = {}

            # Extract and accumulate items
            data_key = next((key for key in ["value", "data"] if key in data), None)
            if data_key:
                all_items.extend(data[data_key])

            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break

            # Set continuationToken in params for the next request
            params["continuationToken"] = continuation_token
            kwargs["params"] = params

        if return_json:
            # If all_items is empty but data had "value" or "data", return an empty array
            return all_items or ([] if data_key else data)
        else:
            if all_items:
                # Update the original response content
                data[data_key] = all_items
                response._content = json.dumps(data).encode("utf-8")

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