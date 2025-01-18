"""CommerceTools authentication utilities."""

import requests
from requests.auth import HTTPBasicAuth
from typing import Literal


class Auth:
    """Handles authentication with CommerceTools API."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        project_key: str,
        region: Literal['us-central1', 'europe-west1'] = 'us-central1'
    ):
        """
        Initialize CommerceTools authentication.

        Args:
            client_id: CommerceTools API client ID
            client_secret: CommerceTools API client secret
            project_key: CommerceTools project key
            region: CommerceTools API region:
                   - us-central1: United States
                   - europe-west1: Europe
        """
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__project_key = project_key
        self.__region = region
        self.__token = None
        self.__auth_url = f'https://auth.{self.__region}.gcp.commercetools.com/oauth/token'
        self.__api_url = f'https://api.{self.__region}.gcp.commercetools.com/{self.__project_key}'
    
    def authenticate(self):
        """
        Get an access token from CommerceTools.
                    
        Raises:
            requests.exceptions.RequestException: If authentication fails
        """
        auth = HTTPBasicAuth(self.__client_id, self.__client_secret)
        data = {'grant_type': 'client_credentials', 'scope': f'manage_project:{self.__project_key}'}
        response = requests.post(self.__auth_url, auth=auth, data=data)
        response.raise_for_status()
        self.__token = response.json()['access_token']
    
    @property
    def api_url(self) -> str:
        """Get the CommerceTools API URL for this project."""
        return self.__api_url
    
    @property
    def token(self) -> str:
        """Get the current access token."""
        return self.__token