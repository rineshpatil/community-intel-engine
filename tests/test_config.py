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


def test_missing_required_setting_raises(monkeypatch):
    monkeypatch.delenv("CIE_TABLE_NAME", raising=False)
    monkeypatch.delenv("CIE_QUEUE_URL", raising=False)
    with pytest.raises(Exception):
        Settings()
