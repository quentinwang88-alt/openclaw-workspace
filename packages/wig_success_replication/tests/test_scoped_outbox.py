from unittest.mock import Mock

from wig_success_replication.feishu_outbox import FeishuOutboxService
from wig_success_replication.repository import InMemoryRepository, make_outbox_record


def test_explicit_export_only_delivers_requested_outbox():
    repository, writer = InMemoryRepository(), Mock()
    requested = make_outbox_record("revision", "revision_one", "upsert_script_pool", {"full_prompt": "revision"})
    other = make_outbox_record("prompt", "prompt_other", "create_prompt_row", {"full_prompt": "other"})
    for row in (requested, other):
        repository.enqueue_outbox(row)
    service = FeishuOutboxService(repository, writer)
    assert service.retry_one(requested.outbox_id)["status"] == "delivered"
    writer.apply.assert_called_once_with(requested.operation, requested.payload)
    assert [row.outbox_id for row in repository.pending_outbox()] == [other.outbox_id]
    assert service.retry_one(requested.outbox_id)["status"] == "already_delivered"
    assert writer.apply.call_count == 1


def test_export_failure_retries_writeback_without_a_compiler():
    repository, writer = InMemoryRepository(), Mock()
    requested = make_outbox_record("revision", "revision_one", "upsert_script_pool", {"full_prompt": "frozen"})
    repository.enqueue_outbox(requested)
    writer.apply.side_effect = [TimeoutError("writeback timeout"), {"record_id": "recNew"}]
    service = FeishuOutboxService(repository, writer)
    assert service.retry_one(requested.outbox_id)["status"] == "writeback_pending"
    assert service.retry_one(requested.outbox_id)["delivery"] == {"record_id": "recNew"}
    assert writer.apply.call_count == 2
    assert repository.pending_outbox() == []
