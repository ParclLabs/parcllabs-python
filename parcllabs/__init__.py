from parcllabs.__version__ import VERSION as __version__  # noqa: F401, N811

# Constants
DEFAULT_API_BASE = "https://api.parcllabs.com"

api_key: str | None = None
api_base = DEFAULT_API_BASE

from parcllabs.parcllabs_client import ParclLabsClient  # noqa: E402, F401
