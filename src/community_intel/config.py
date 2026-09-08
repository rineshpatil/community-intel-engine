from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # protected_namespaces=() so future MODEL_* settings do not collide with
    # pydantic's reserved "model_" prefix.
    model_config = SettingsConfigDict(
        env_prefix="CIE_",
        extra="ignore",
        protected_namespaces=(),
        # Local convenience only. Lambda has no .env and pydantic-settings
        # skips a missing file silently, so this is inert in deployment.
        # Precedence: shell environment wins over .env.
        env_file=".env",
        env_file_encoding="utf-8",
    )

    table_name: str
    queue_url: str

    github_webhook_secret: str = ""
    github_webhook_secret_param: str = ""   # SSM parameter name, preferred
    github_bot_id: int = 0
    github_bot_login: str = ""

    reddit_subreddits: list[str] = []
    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    # Reddit throttles generic user agents. Convention is
    # platform:app-id:version (by /u/username) — add the username
    # before any real use.
    reddit_user_agent: str = "python:community-intel-engine:0.1.0"


@lru_cache
def get_settings() -> Settings:
    return Settings()
