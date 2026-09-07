from datetime import datetime, timezone

import pytest

from community_intel.models import NormalizedItem
from community_intel.prefilter import rejection_reason

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)


def item(**kw) -> NormalizedItem:
    base = dict(
        source="github", source_kind="issue", external_id="1",
        channel="acme/widget", url="https://example.invalid/1",
        author="octocat", title="Crash on startup",
        body="The application crashes every time I open the settings panel.",
        created_at=NOW, ingested_at=NOW, raw={},
    )
    base.update(kw)
    return NormalizedItem(**base)


def test_normal_item_is_accepted():
    assert rejection_reason(item(), bot_id=999, bot_login="cie[bot]") is None


def test_our_own_bot_is_rejected_by_login():
    # The self-loop guard. Our own issues and comments fire webhooks; without
    # this the system ingests its own output forever.
    assert rejection_reason(
        item(author="cie[bot]"), bot_id=999, bot_login="cie[bot]"
    ) == "self"


def test_our_own_bot_is_rejected_by_sender_id():
    assert rejection_reason(
        item(raw={"sender": {"id": 999}}), bot_id=999, bot_login="cie[bot]"
    ) == "self"


def test_other_bots_are_rejected():
    assert rejection_reason(
        item(author="dependabot[bot]"), bot_id=999, bot_login="cie[bot]"
    ) == "bot"


def test_automoderator_is_rejected():
    assert rejection_reason(
        item(source="reddit", author="AutoModerator"),
        bot_id=999, bot_login="cie[bot]",
    ) == "bot"


@pytest.mark.parametrize("body", ["+1", "\U0001F44D", "   ", "same", ""])
def test_contentless_bodies_are_rejected(body):
    assert rejection_reason(
        item(body=body, title=None), bot_id=999, bot_login="cie[bot]"
    ) == "too_short"


def test_closed_via_commit_reference_is_rejected():
    assert rejection_reason(
        item(body="Closed via #123", title=None),
        bot_id=999, bot_login="cie[bot]",
    ) == "structural_noise"


def test_short_body_with_long_title_is_accepted():
    # An issue whose signal lives in the title must survive.
    assert rejection_reason(
        item(title="Segfault when parsing empty config file", body="see title"),
        bot_id=999, bot_login="cie[bot]",
    ) is None
