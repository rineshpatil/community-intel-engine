from datetime import datetime, timezone

from community_intel.models import NormalizedItem


def make_item(**overrides) -> NormalizedItem:
    defaults = dict(
        source="github",
        source_kind="issue",
        external_id="998",
        parent_external_id=None,
        channel="acme/widget",
        url="https://github.com/acme/widget/issues/1",
        author="octocat",
        title="Crash on startup",
        body="It crashes when I launch it.",
        created_at=datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 9, 1, 12, 5, tzinfo=timezone.utc),
        raw={"any": "payload"},
    )
    defaults.update(overrides)
    return NormalizedItem(**defaults)


def test_item_id_is_derived_from_source_and_external_id():
    item = make_item()
    assert item.item_id == "github#998"


def test_item_id_is_stable_across_construction():
    assert make_item().item_id == make_item().item_id


def test_make_id_helper_matches_field():
    assert NormalizedItem.make_id("reddit", "t1_abc") == "reddit#t1_abc"


def test_to_dynamo_item_sets_keys_and_json_safe_values():
    d = make_item().to_dynamo_item()
    assert d["pk"] == "ITEM#github#998"
    assert d["sk"] == "META"
    # datetimes must be strings, not datetime objects, for DynamoDB
    assert isinstance(d["created_at"], str)
    assert d["created_at"].startswith("2026-09-01T12:00:00")
