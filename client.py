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

    def post(self, path, data):
        """Make a POST request to the API"""
        return self.request(path, data, "POST")

    def get(self, path, data=None):
        """Make a GET request to the API"""
        return self.request(path, data, "GET")

    def request(self, path, data=None, method="GET"):
        """Make a request to the API"""
        url = "https://api.dataforseo.com/" + path
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
            
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {str(e)}")
            raise
