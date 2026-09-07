from community_intel.adapters.github import from_webhook


def issue_payload(**over):
    p = {
        "action": "opened",
        "issue": {
            "id": 500, "number": 7, "title": "Crash on startup",
            "body": "It crashes when I launch it on 2.1.",
            "html_url": "https://github.com/acme/widget/issues/7",
            "created_at": "2026-09-01T12:00:00Z",
            "user": {"login": "octocat", "email": "leak@example.invalid"},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": 42},
    }
    p.update(over)
    return p


def comment_payload():
    return {
        "action": "created",
        "issue": {"id": 500, "number": 7},
        "comment": {
            "id": 900, "body": "Same here, also on 2.1.",
            "html_url": "https://github.com/acme/widget/issues/7#issuecomment-900",
            "created_at": "2026-09-02T09:00:00Z",
            "user": {"login": "hubot"},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": 43},
    }


def test_issue_maps_to_normalized_item():
    item = from_webhook("issues", issue_payload())
    assert item.item_id == "github#500"
    assert item.source_kind == "issue"
    assert item.channel == "acme/widget"
    assert item.title == "Crash on startup"
    assert item.parent_external_id is None
    assert item.created_at.year == 2026


def test_author_never_contains_email():
    item = from_webhook("issues", issue_payload())
    assert item.author == "octocat"
    assert "@" not in item.author


def test_comment_links_to_parent_issue_id():
    # This is what makes structural dedup work: the parent's external_id here
    # must equal the external_id the "issues" event stored for issue 7.
    item = from_webhook("issue_comment", comment_payload())
    assert item.item_id == "github#900"
    assert item.parent_external_id == "500"


def test_raw_payload_is_retained():
    item = from_webhook("issues", issue_payload())
    assert item.raw["sender"]["id"] == 42


def test_edited_and_deleted_actions_are_ignored():
    assert from_webhook("issues", issue_payload(action="deleted")) is None
    assert from_webhook("issues", issue_payload(action="labeled")) is None


def test_unknown_event_type_returns_none():
    # Valid action, unrecognised event type: must fall through to None.
    assert from_webhook("push", {"action": "opened"}) is None


def test_discussion_and_discussion_comment():
    d = {
        "action": "created",
        "discussion": {
            "id": 700, "title": "Feature idea",
            "body": "Would love dark mode.",
            "html_url": "https://github.com/acme/widget/discussions/3",
            "created_at": "2026-09-03T08:00:00Z",
            "user": {"login": "ada"},
        },
        "repository": {"full_name": "acme/widget"},
        "sender": {"id": 44},
    }
    item = from_webhook("discussion", d)
    assert item.item_id == "github#700"
    assert item.source_kind == "discussion"

    dc = dict(d)
    dc["comment"] = {
        "id": 800, "body": "Agreed, dark mode please.",
        "html_url": "https://github.com/acme/widget/discussions/3#c800",
        "created_at": "2026-09-03T09:00:00Z",
        "user": {"login": "grace"},
    }
    citem = from_webhook("discussion_comment", dc)
    assert citem.item_id == "github#800"
    assert citem.parent_external_id == "700"
