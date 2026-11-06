"""Bind comma-separated report states to their explicitly named projects."""
from contextlib import closing

import pytest

from chatlog_keeper.bot.service import Service, required_report
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config


DAY, NOW = "2026-10-29", "2026-10-29T10:00:00+08:00"
TEXT = "今天娱乐首单已无，竞技首单可用，请报备"


def admit(store, text=TEXT, sender="wxid_manager"):
    event = {"event_id": "comma-report", "message_ref": "msg_comma_report", "group_ref": "group_a",
             "sender_id": sender, "text": text, "occurred_at": NOW}
    store.begin_request(event)
    return event, Service(store, {"event_id": event["event_id"], "writable": True, "now": NOW})


def test_comma_multi_project_floor_and_exactly_once_receipts(tmp_path):
    with closing(BotStore(tmp_path)) as store:
        store.configure(config()); event, service = admit(store)
        requirement = required_report(event, store.binding("group_a")["business"], current_day=DAY)
        assert requirement is not None
        assert requirement["items"] == [{"item_id": "fun", "status": "unavailable"},
                                        {"item_id": "rank", "status": "available"}]
        for item, status in (("fun", "unavailable"), ("rank", "available")):
            args = {"item": item, "status": status, "evidence": TEXT}
            first, replay = service.call("report_offer_status", args), service.call("report_offer_status", args)
            assert first["recorded"] is True and first["version"] == 1
            assert replay == {**first, "replayed": True}
        assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 2
        assert len([row for row in store.audit() if row["kind"] == "report"]) == 2


@pytest.mark.parametrize("text,evidence", [
    ("今天娱乐首单已无，竞技首单可用，不要报备", "今天娱乐首单已无"),
    ("如果今天娱乐首单已无，竞技首单可用，请报备", "今天娱乐首单已无"),
    ("今天娱乐首单已无，如果竞技首单可用，请报备", "今天娱乐首单已无"),
    ("有人说‘娱乐首单已无’，竞技首单可用，请报备", "娱乐首单已无"),
    ("今天娱乐首单已无，还是可用", "今天娱乐首单已无"),
    ("今天娱乐首单已无，娱乐首单可用", "今天娱乐首单已无"),
    ("今天娱乐首单已无；娱乐首单可用", "今天娱乐首单已无"),
    ("今天娱乐首单可用，还是已无", "还是已无"),
])
def test_comma_does_not_authorize_negated_conditional_quoted_or_conflicting_source(tmp_path, text, evidence):
    with closing(BotStore(tmp_path)) as store:
        store.configure(config()); event, service = admit(store, text)
        assert required_report(event, store.binding("group_a")["business"], current_day=DAY) is None
        for status in ("available", "unavailable"):
            result = service.call("report_offer_status", {"item": "fun", "status": status, "evidence": evidence})
            assert result["error"] == "explicit_report_evidence_required"
        assert store.receipt(event["event_id"]) is None
        assert not [row for row in store.audit() if row["kind"] == "report"]


def test_comma_does_not_authorize_unapproved_sender(tmp_path):
    with closing(BotStore(tmp_path)) as store:
        store.configure(config()); event, service = admit(store, sender="wxid_visitor")
        assert required_report(event, store.binding("group_a")["business"], current_day=DAY) is None
        for item, status in (("fun", "unavailable"), ("rank", "available")):
            result = service.call("report_offer_status", {"item": item, "status": status, "evidence": TEXT})
            assert result["error"] == "report_not_authorized"
        assert store.receipt(event["event_id"]) is None


@pytest.mark.parametrize("text", ["今天娱乐首单已无，竞技首单可用，请报备", "今天娱乐首单已无；竞技首单可用，请报备"])
def test_item_scoped_evidence_keeps_other_project_status_separate(tmp_path, text):
    with closing(BotStore(tmp_path)) as store:
        store.configure(config()); _, service = admit(store, text)
        assert service.call("report_offer_status", {"item": "fun", "status": "unavailable", "evidence": "娱乐首单已无"})["recorded"]
        assert service.call("report_offer_status", {"item": "rank", "status": "available", "evidence": "竞技首单可用"})["recorded"]
