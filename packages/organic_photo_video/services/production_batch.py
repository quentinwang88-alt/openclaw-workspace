"""Owner-fenced durable scanner leases; expiration never authorizes regeneration."""
from __future__ import annotations

import threading
import uuid


class BatchLeaseBusy(RuntimeError):
    pass


class ProjectionPendingError(RuntimeError):
    """Business state is committed; replay its durable UI projection only."""
    pass


class BatchLease:
    def __init__(self, repository, batch_id, *, seconds=120):
        self.repository, self.batch_id = repository, batch_id
        self.owner, self.seconds = uuid.uuid4().hex, seconds
        self.stopped, self.lost = threading.Event(), threading.Event()
        if not repository.claim_batch_run(batch_id, owner=self.owner, lease_seconds=seconds):
            raise BatchLeaseBusy("该批次仍有运行租约，请勿重复执行")
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.thread.start()

    def _heartbeat(self):
        while not self.stopped.wait(self.seconds / 3):
            try:
                valid = self.repository.heartbeat_batch_run(self.batch_id, owner=self.owner, lease_seconds=self.seconds)
            except Exception:
                valid = False
            if not valid:
                self.lost.set()
                return

    def check(self):
        if self.lost.is_set():
            raise RuntimeError("批次租约已丢失，停止后续副作用；需要核对在途请求")

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=1)
        self.repository.finish_batch_run(self.batch_id, owner=self.owner, status="needs_attention" if self.lost.is_set() else "waiting")
