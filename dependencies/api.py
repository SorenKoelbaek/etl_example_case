import requests

class APIClient:
    """A simple API client to interact with RESTful APIs."""
    _instance = None

    def __new__(cls, base_url: str):
        if cls._instance is None:
            cls._instance = super(APIClient, cls).__new__(cls)
            cls._instance.base_url = base_url
        return cls._instance

    def get(self, endpoint: str, params: dict = None):
        """Make a GET request to the specified endpoint with optional query parameters."""
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            return response.json()
        else:
            return {"error": response.status_code, "message": response.text}
