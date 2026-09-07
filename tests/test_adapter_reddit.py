from types import SimpleNamespace

from community_intel.adapters.reddit import from_comment, from_submission


def fake_submission():
    return SimpleNamespace(
        name="t3_abc123",
        title="App crashes on 2.1",
        selftext="Every launch segfaults after upgrading.",
        permalink="/r/widget/comments/abc123/app_crashes/",
        created_utc=1788000000.0,
        author=SimpleNamespace(name="ada"),
        subreddit=SimpleNamespace(display_name="widget"),
    )


def fake_comment():
    return SimpleNamespace(
        name="t1_def456",
        body="Same here on 2.1, reproducible every time.",
        permalink="/r/widget/comments/abc123/app_crashes/def456/",
        created_utc=1788003600.0,
        author=SimpleNamespace(name="grace"),
        subreddit=SimpleNamespace(display_name="widget"),
        link_id="t3_abc123",
    )


def test_submission_maps_with_fullname_as_external_id():
    item = from_submission(fake_submission())
    assert item.item_id == "reddit#t3_abc123"
    assert item.source_kind == "submission"
    assert item.channel == "r/widget"
    assert item.parent_external_id is None
    assert item.url.startswith("https://www.reddit.com/")


def test_comment_links_to_parent_submission():
    item = from_comment(fake_comment())
    assert item.item_id == "reddit#t1_def456"
    assert item.parent_external_id == "t3_abc123"


def test_deleted_author_does_not_crash():
    sub = fake_submission()
    sub.author = None
    assert from_submission(sub).author == "[deleted]"


def test_created_at_comes_from_platform_epoch():
    item = from_submission(fake_submission())
    assert item.created_at.year == 2026


def test_cursors_are_tracked_per_listing_kind(monkeypatch):
    """Submissions and comments must not share a cursor.

    Their fullname prefixes are disjoint (t3_ vs t1_), so one shared cursor
    could never match the second listing and it would re-scan every poll.
    """
    import boto3
    from moto import mock_aws

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("CIE_TABLE_NAME", "items")
    monkeypatch.setenv("CIE_QUEUE_URL", "u")

    with mock_aws():
        boto3.client("dynamodb", region_name="us-east-1").create_table(
            TableName="items",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        from community_intel.config import get_settings

        get_settings.cache_clear()
        from community_intel.handlers.reddit_poll import read_cursor, write_cursor

        write_cursor("widget", "submissions", "t3_abc")
        write_cursor("widget", "comments", "t1_def")

        assert read_cursor("widget", "submissions") == "t3_abc"
        assert read_cursor("widget", "comments") == "t1_def"
        assert read_cursor("widget", "other") is None
        get_settings.cache_clear()
