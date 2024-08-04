import requests
import time

class FabricPowerBIClient:
    def __init__(self, tenant_id=None, client_id=None, client_secret=None, username=None, password=None, client_type='fabric'):
        self.client_type = client_type.lower()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        
        if tenant_id and client_id and (client_secret or (username and password)):
            # Initialize using OAuth2 client credentials or password auth
            self.access_token = self._get_access_token()
            self.base_urls = {
                'fabric': "https://api.fabric.microsoft.com",
                'powerbi': "https://api.powerbi.com"
            }
            self.is_custom_client = True
        else:
            # Try to import sempy and initialize the client
            try:
                from sempy import fabric
                self.client = fabric.FabricRestClient() if self.client_type == 'fabric' else fabric.PowerBIRestClient()
                self.is_custom_client = False
            except ImportError:
                raise ImportError(
                    "The Semantic Link library is not installed. Ensure you are running in a Fabric environment with the library installed. "
                    "If you are not in a Fabric environment, provide the tenant ID, client ID, and either client secret or username and password."
                )
            except AttributeError:
                raise ImportError(f"The requested client type '{self.client_type}' is not available in the 'sempy' library.")

    def _get_access_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        scope = f"https://api.fabric.microsoft.com/.default" if self.client_type == 'fabric' else "https://analysis.windows.net/powerbi/api/.default"
        
        if self.client_secret:
            # Client credentials flow
            payload = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': scope
            }
        elif self.username and self.password:
            # Resource Owner Password Credentials (ROPC) flow
            payload = {
                'grant_type': 'password',
                'client_id': self.client_id,
                'username': self.username,
                'password': self.password,
                'scope': scope
            }
        else:
            raise ValueError("Either client_secret or both username and password must be provided")

        response = self._make_request_with_retry(requests.post, url, data=payload)
        return response.json().get('access_token')

    def _make_request_with_retry(self, request_func, *args, **kwargs):
        max_retries = 5
        retry_delay = 10  # seconds

        for attempt in range(max_retries):
            response = request_func(*args, **kwargs)
            
            if response.status_code == 429 and attempt < max_retries - 1:
                print(f"Received 429 response. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue  # Skip the rest of the loop and retry

            # Raise an HTTPError for bad responses (4xx and 5xx)
            response.raise_for_status()

            # Return the response if the request was successful
            return response


    def request(self, method, url, **kwargs):
        if self.is_custom_client:
            # Prepend the base URL if it's not already included
            base_url = self.base_urls[self.client_type]
            if not url.startswith(base_url):
                url = base_url.rstrip('/') + '/' + url.lstrip('/')
            
            headers = kwargs.get('headers', {})
            headers['Authorization'] = f"Bearer {self.access_token}"
            kwargs['headers'] = headers
            
            response = self._make_request_with_retry(requests.request, method, url, **kwargs)
        else:
            # Use the sempy client
            client_method = getattr(self.client, method.lower())
            response = self._make_request_with_retry(client_method, url, **kwargs)
        return response