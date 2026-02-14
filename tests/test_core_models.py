"""core.models 行为测试。"""

from datetime import UTC, datetime

from src.core.models import PendingCommentScan, PendingThreadScan


def test_pending_thread_scan_to_dict_omits_none_created_at():
    """created_at 未设置时，to_dict 不应包含该字段。"""
    model = PendingThreadScan(tid=1, fid=10, fname="bar", backfill=False)

    payload = model.to_dict()

    assert "created_at" not in payload
    assert payload["tid"] == 1
    assert payload["fid"] == 10
    assert payload["fname"] == "bar"
    assert payload["backfill"] is False


def test_pending_comment_scan_to_dict_keeps_created_at_when_set():
    """created_at 已设置时，to_dict 应包含该字段。"""
    created_at = datetime.now(UTC)
    model = PendingCommentScan(
        tid=1,
        pid=2,
        backfill=False,
        task_kind="full",
        created_at=created_at,
    )

    payload = model.to_dict()

    assert payload["created_at"] == created_at
    assert payload["task_kind"] == "full"
