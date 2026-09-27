"""python -m app  ->  config 의 server.host / server.port 로 기동."""
import uvicorn

from .main import app  # PXA_CONFIG 기본값을 먼저 지정하도록 main 을 먼저 import
from .settings import get_server_config

if __name__ == "__main__":
    server = get_server_config()
    uvicorn.run(app, host=server.host, port=server.port, log_level="warning")
