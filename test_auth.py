from pathlib import Path

from dotenv import dotenv_values
from loguru import logger
from src.client import TidlClient
from src.exceptions import AuthError
from src.setup_logging import setup_logging

setup_logging()

env_path = Path("../.env")
env=dotenv_values(dotenv_path=env_path)
TDL_CLIENT_ID = env.get("tdl_client_id")
TDL_CLIENT_SECRET = env.get("tdl_client_secret")



def test_authentication() -> None:  # noqa: D103
    logger.info("Creating Client...")
    client = TidlClient()

    logger.info("Initial authentication status: {}", client.is_authenticated())
    logger.info("Starting authentication...")
    try:
        success = client.authenticate()
        logger.info("Authentication success: {}", success)
        logger.info("Post-authentication status: {}", client.is_authenticated())

        if success:
            uid = client.get_user_id()
            logger.info("Authenticated user ID: {}", uid)

    except AuthError as e:
        logger.error("An error occurred during authentication: {}", e)


if __name__ == "__main__":
    test_authentication()
