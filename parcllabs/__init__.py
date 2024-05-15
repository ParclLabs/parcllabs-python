from typing import Optional
from parcllabs.__version__ import VERSION as __version__

# Constants
DEFAULT_API_BASE = "https://api.parcllabs.com"

api_key: Optional[str] = None
api_base = DEFAULT_API_BASE


from parcllabs.parcllabs_client import ParclLabsClient
