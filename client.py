import requests
import json
import time
import threading
from random import randrange
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('dataforseo_client')

class RestClient:
    """RestClient class to handle API requests"""
    
    def __init__(self, username, password):
        """Initialize the client with username and password"""
        self.username = username
        self.password = password
        # Ensure base_url doesn't have trailing slash
        self.base_url = "https://api.dataforseo.com"
        logger.info(f"RestClient initialized with username: {username}")

    def post(self, path, data):
        """Make a POST request to the API"""
        # Ensure path has leading slash per DataForSEO examples
        if not path.startswith('/'):
            path = '/' + path
            
        logger.info(f"Making POST request to path: {path}")
        return self.request(path, data, "POST")

    def get(self, path, data=None):
        """Make a GET request to the API"""
        # Ensure path has leading slash per DataForSEO examples
        if not path.startswith('/'):
            path = '/' + path
            
        logger.info(f"Making GET request to path: {path}")
        return self.request(path, data, "GET")

    def request(self, path, data=None, method="GET"):
        """Make a request to the API"""
        # Path should already have leading slash from post/get methods
        url = f"{self.base_url}{path}"
        logger.info(f"Making {method} request to URL: {url}")
        
        if method == "POST" and data:
            logger.info(f"Request payload: {json.dumps(data, indent=2)}")
        
        try:
            if method == "POST":
                logger.info(f"Sending POST request with auth: {self.username}")
                response = requests.post(
                    url,
                    auth=(self.username, self.password),
                    json=data
                )
            else:
                logger.info(f"Sending GET request with auth: {self.username}")
                response = requests.get(
                    url,
                    auth=(self.username, self.password),
                    params=data
                )
            
            logger.info(f"Response status code: {response.status_code}")
            
            # Log response headers for debugging
            logger.info(f"Response headers: {dict(response.headers)}")
            
            # Raise an exception for bad status codes
            response.raise_for_status()
            
            response_json = response.json()
            
            # Log a shortened version of the response to avoid overwhelming logs
            if isinstance(response_json, dict):
                status_info = {
                    "status_code": response_json.get("status_code"),
                    "status_message": response_json.get("status_message"),
                    "tasks_count": len(response_json.get("tasks", [])),
                }
                logger.info(f"Response summary: {json.dumps(status_info, indent=2)}")
            else:
                logger.info(f"Response not in expected format: {type(response_json)}")
            
            return response_json
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error: {e}")
            # Try to parse and log the response content if possible
            try:
                error_content = e.response.json()
                logger.error(f"Error response content: {json.dumps(error_content, indent=2)}")
            except:
                logger.error(f"Error response content: {e.response.text}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection Error: {e}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout Error: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Exception: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
