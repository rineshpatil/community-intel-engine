import pytest

from community_intel.config import Settings


def test_settings_read_from_env(monkeypatch):
    monkeypatch.setenv("CIE_TABLE_NAME", "items")
    monkeypatch.setenv("CIE_QUEUE_URL", "https://sqs/q")
    monkeypatch.setenv("CIE_GITHUB_WEBHOOK_SECRET", "s3cret")
    monkeypatch.setenv("CIE_GITHUB_BOT_ID", "12345")
    monkeypatch.setenv("CIE_REDDIT_SUBREDDITS", '["python","aws"]')

    s = Settings()

    assert s.table_name == "items"
    assert s.github_bot_id == 12345
    assert s.reddit_subreddits == ["python", "aws"]


def test_missing_required_setting_raises(tmp_path, monkeypatch):
    # chdir to an empty dir: a real .env in the repo root would otherwise
    # supply these values and this test would silently stop testing anything.
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CIE_TABLE_NAME", raising=False)
    monkeypatch.delenv("CIE_QUEUE_URL", raising=False)
    with pytest.raises(Exception):
        Settings()


def test_settings_load_from_dotenv_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("CIE_TABLE_NAME", "CIE_QUEUE_URL"):
        monkeypatch.delenv(var, raising=False)
    (tmp_path / ".env").write_text(
        "CIE_TABLE_NAME=from-dotenv\nCIE_QUEUE_URL=https://sqs/from-dotenv\n"
    )

    s = Settings()

    assert s.table_name == "from-dotenv"


def test_shell_env_overrides_dotenv_file(tmp_path, monkeypatch):
    # Precedence matters: this is how you override a single value for one run
    # without editing .env.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "CIE_TABLE_NAME=from-dotenv\nCIE_QUEUE_URL=https://sqs/from-dotenv\n"
    )
    monkeypatch.setenv("CIE_TABLE_NAME", "from-shell")

    assert Settings().table_name == "from-shell"
