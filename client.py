import requests
import json
import time
import threading
from random import randrange


class RestClient:
    """RestClient class to handle API requests"""
    
    def __init__(self, username, password):
        """Initialize the client with username and password"""
        self.username = username
        self.password = password
        self.base_url = "https://api.dataforseo.com"

    def post(self, path, data):
        """Make a POST request to the API"""
        return self.request(path, data, "POST")

    def get(self, path, data=None):
        """Make a GET request to the API"""
        return self.request(path, data, "GET")

    def request(self, path, data=None, method="GET"):
        """Make a request to the API"""
        # Ensure path starts with a slash but remove any leading slashes from path to avoid double slashes
        if not path.startswith('/'):
            path = '/' + path
        
        url = f"{self.base_url}{path}"
        
        try:
            if method == "POST":
                response = requests.post(
                    url,
                    auth=(self.username, self.password),
                    json=data
                )
            else:
                response = requests.get(
                    url,
                    auth=(self.username, self.password),
                    params=data
                )
            
            # Raise an exception for bad status codes
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.HTTPError as err:
            print(f"HTTP Error: {err}")
            print(f"Response content: {err.response.text}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {str(e)}")
            raise
