from urllib.parse import urljoin
import requests
import json
from typing import Dict, Any
from logging import getLogger

logger = getLogger("main")

class PrometheusClient:

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
    
    def query_metrics(self, query: str, start: str, end: str, step: str) -> list:
        url = f"{self.base_url}/api/v1/query_range"
        params = {
            "query": query,
            "start": start,
            "end": end,
            "step": step
        }
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            logger.error("Failed to query metrics.")
            raise Exception(f"Error {response.status_code}: {response.text}")
        
        return response.json()['data']['result']
