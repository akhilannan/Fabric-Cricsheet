import requests

class FabricPowerBIClient:
    def __init__(self, tenant_id=None, client_id=None, client_secret=None, client_type='fabric'):
        self.client_type = client_type.lower()
        
        if tenant_id and client_id and client_secret:
            # Initialize using OAuth2 client credentials
            self.tenant_id = tenant_id
            self.client_id = client_id
            self.client_secret = client_secret
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
                    "If you are not in a Fabric environment, provide the tenant ID, client ID, and client secret."
                )
            except AttributeError:
                raise ImportError(f"The requested client type '{self.client_type}' is not available in the 'sempy' library.")

    def _get_access_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': "https://api.fabric.microsoft.com/.default" if self.client_type == 'fabric'
            else "https://analysis.windows.net/powerbi/api/.default"
        }
        response = requests.post(url, data=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json().get('access_token')

    def request(self, method, url, **kwargs):
        if self.is_custom_client:
            # Prepend the base URL if it's not already included
            base_url = self.base_urls[self.client_type]
            if not url.startswith(base_url):
                url = base_url.rstrip('/') + '/' + url.lstrip('/')
            
            headers = kwargs.get('headers', {})
            headers['Authorization'] = f"Bearer {self.access_token}"
            kwargs['headers'] = headers
            
            response = requests.request(method, url, **kwargs)
        else:
            # Use the sempy client
            client_method = getattr(self.client, method.lower())
            response = client_method(url, **kwargs)
        response.raise_for_status()
        return response