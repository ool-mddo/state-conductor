from urllib.parse import urljoin
import requests
import json
from typing import Dict, Any
from logging import getLogger

logger = getLogger("main")

class PrometheusClient:

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
    
    def query_instant_metrics(self, query: str, timestamp: int) -> list:
        url = f"{self.base_url}/api/v1/query"
        params = {
            "query": query,
            "time": timestamp,
        }
        logger.info(f"querying: {query}")
        response = requests.get(url=url, params=params)
        
        if response.status_code != 200:
            logger.error("Failed to query metrics.")
            raise Exception(f"Error {response.status_code}: {response.text}")
        
        return response.json()['data']['result']
