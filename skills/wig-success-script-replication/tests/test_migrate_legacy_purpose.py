from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from migrate_legacy_purpose import build_plan, classify, inventory, main  # noqa: E402


def fixtures():
    mother = {"mother_id": "m1", "version": 2, "name": "历史母版", "feishu_record_id": "rec-m1",
              "source_script": "不可改母版正文", "contract_json": '{"segments": []}'}
    batches = [
        {"batch_id": "b1", "mother_id": "m1", "mother_version": 2, "publish_purpose": "未分类",
         "cart_enabled": None, "product_ids_json": '["p1","p2"]', "status": "completed"},
        {"batch_id": "b-empty", "mother_id": "m1", "mother_version": 2, "publish_purpose": "未分类",
         "cart_enabled": None, "product_ids_json": '["p1"]', "status": "failed"},
    ]
    prompts = [{"prompt_id": "prompt1", "batch_id": "b1", "mother_id": "m1", "mother_version": 2,
                "product_id": "p1", "publish_purpose": "未分类", "cart_enabled": None,
                "full_prompt": "人物与产品执行正文，禁止改写。", "sequence_no": 1,
                "replication_mode": "high_fidelity", "creative_route": "H1", "handoff_context_json": None}]
    return mother, batches, prompts


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=()):
        c = self.connection
        c.statements.append((sql, params))
        self.rowcount = 0
        if sql.startswith("SELECT GET_LOCK"):
            self.rows = [{"acquired": c.lock_available}]
        elif sql.startswith("SELECT RELEASE_LOCK"):
            self.rows = [{"released": 1}]
        elif sql.startswith("SELECT * FROM wsr_mother_version") or sql.startswith("SELECT feishu_record_id"):
            self.rows = [deepcopy(c.mother)] if c.mother else []
        elif sql.startswith("SELECT * FROM wsr_replication_batch"):
            self.rows = deepcopy(c.batches)
        elif sql.startswith("SELECT * FROM wsr_replication_prompt"):
            self.rows = deepcopy(c.prompts)
        elif sql.lstrip().startswith("SELECT p.mother_id"):
            self.rows = [{"mother_id": "m1", "mother_version": 2, "product_id": "p1",
                          "mother_name": "历史母版", "prompt_count": 1}]
        elif sql.lstrip().startswith("SELECT b.mother_id"):
            self.rows = [{**deepcopy(row), "mother_name": "历史母版", "prompt_count": 0 if row["batch_id"] == "b-empty" else 1}
                         for row in c.batches]
        elif sql.startswith("UPDATE "):
            table = sql.split()[1]
            key = "prompt_id" if table == "wsr_replication_prompt" else "batch_id"
            rows = c.prompts if key == "prompt_id" else c.batches
            purpose, cart, row_id, mother_id, version = params
            if row_id == c.fail_id:
                raise RuntimeError("simulated SQL update failure")
            for row in rows:
                if row[key] == row_id and row["mother_id"] == mother_id and row["mother_version"] == version:
                    row["publish_purpose"] = purpose
                    row["cart_enabled"] = cart
                    self.rowcount = 1
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class Connection:
    def __init__(self):
        self.mother, self.batches, self.prompts = fixtures()
        self.statements = []
        self.commits = 0
        self.rollbacks = 0
        self.lock_available = True
        self.fail_id = None
        self.snapshot = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return Cursor(self)

    def begin(self):
        self.snapshot = deepcopy((self.mother, self.batches, self.prompts))

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1
        if self.snapshot:
            self.mother, self.batches, self.prompts = deepcopy(self.snapshot)


def scope(**kwargs):
    return {"mother_id": "m1", "mother_version": 2, "publish_purpose": "养号", "cart_setting": "按用途默认", **kwargs}


def test_plan_hash_is_order_stable_and_binds_body_and_settings():
    mother, batches, prompts = fixtures()
    args = {"publish_purpose": "养号", "cart_setting": "不挂车"}
    plan = build_plan(mother, batches, prompts, **args)
    assert plan["plan_hash"] == build_plan(mother, list(reversed(batches)), prompts, **args)["plan_hash"]
    assert plan["zero_result_batch_ids"] == ["b-empty"]
    assert plan["action_count"] == 3
    assert "不可改母版正文" not in json.dumps(plan, ensure_ascii=False)
    prompts[0]["full_prompt"] += "changed"
    assert plan["plan_hash"] != build_plan(mother, batches, prompts, **args)["plan_hash"]


def test_dry_run_is_read_only_and_does_not_lock_or_start_transaction():
    connection = Connection()
    result = classify(connection, **scope())
    assert result["mode"] == "dry-run"
    assert result["cart_enabled"] is False
    assert not connection.commits and connection.snapshot is None
    assert all(sql.startswith("SELECT *") for sql, _ in connection.statements)


def test_apply_updates_only_classification_including_zero_result_batch():
    connection = Connection()
    before = deepcopy((connection.mother, connection.batches, connection.prompts))
    plan = classify(connection, **scope())
    result = classify(connection, **scope(apply=True, confirm_plan=plan["plan_hash"]))
    assert result["updated_records"] == 3
    assert connection.commits == 1
    assert connection.mother == before[0]
    for old_rows, new_rows in [(before[1], connection.batches), (before[2], connection.prompts)]:
        for old, new in zip(old_rows, new_rows):
            assert new["publish_purpose"] == "养号" and new["cart_enabled"] is False
            assert {k: v for k, v in old.items() if k not in {"publish_purpose", "cart_enabled"}} == {
                k: v for k, v in new.items() if k not in {"publish_purpose", "cart_enabled"}}
    assert any("GET_LOCK" in sql and params == ("wsr:rec-m1",) for sql, params in connection.statements)
    assert any("RELEASE_LOCK" in sql for sql, _ in connection.statements)
    assert not any("outbox" in sql or "context" in sql for sql, _ in connection.statements)


def test_stale_plan_is_rejected_before_updates():
    connection = Connection()
    plan = classify(connection, **scope())
    connection.prompts[0]["sequence_no"] = 2
    with pytest.raises(ValueError, match="计划已变化"):
        classify(connection, **scope(apply=True, confirm_plan=plan["plan_hash"]))
    assert connection.rollbacks == 1
    assert not any(sql.startswith("UPDATE") for sql, _ in connection.statements)


def test_update_failure_rolls_back_every_classification():
    connection = Connection()
    before = deepcopy((connection.batches, connection.prompts))
    plan = classify(connection, **scope())
    connection.fail_id = "prompt1"
    with pytest.raises(RuntimeError, match="simulated"):
        classify(connection, **scope(apply=True, confirm_plan=plan["plan_hash"]))
    assert (connection.batches, connection.prompts) == before
    assert connection.rollbacks == 1 and connection.commits == 0


@pytest.mark.parametrize("purpose,cart", [("带货", True), ("养号", True), ("养号", None)])
def test_classified_conflict_fails_before_any_partial_write(purpose, cart):
    connection = Connection()
    connection.prompts[0]["publish_purpose"] = purpose
    connection.prompts[0]["cart_enabled"] = cart
    with pytest.raises(ValueError, match="已分类冲突"):
        classify(connection, **scope(apply=True, confirm_plan="a" * 64))
    assert not any(sql.startswith("UPDATE") for sql, _ in connection.statements)


def test_matching_classified_rows_are_not_modified():
    mother, batches, prompts = fixtures()
    prompts[0]["publish_purpose"] = "养号"
    prompts[0]["cart_enabled"] = False
    plan = build_plan(mother, batches, prompts, publish_purpose="养号", cart_setting="不挂车")
    assert plan["action_count"] == 2
    assert all(action["table"] == "wsr_replication_batch" for action in plan["actions"])


def test_running_production_task_blocks_apply_without_updates():
    connection = Connection()
    plan = classify(connection, **scope())
    connection.lock_available = False
    with pytest.raises(RuntimeError, match="任务正在执行"):
        classify(connection, **scope(apply=True, confirm_plan=plan["plan_hash"]))
    assert connection.snapshot is None
    assert not any(sql.startswith("UPDATE") for sql, _ in connection.statements)


def test_inventory_lists_unknown_products_and_zero_result_batches():
    result = inventory(Connection().cursor())
    assert result["mode"] == "read-only-inventory"
    assert result["unclassified_prompt_count"] == 1
    assert [row["product_id"] for row in result["groups"]] == ["p1", "p2"]
    assert result["groups"][0]["zero_result_batch_ids"] == ["b-empty"]


def test_main_default_only_uses_connection_factory(monkeypatch, capsys):
    connection = Connection()
    runtime = SimpleNamespace(_database_url=lambda: "redacted", _connection_factory=lambda url: lambda: connection)
    monkeypatch.setitem(sys.modules, "production_runtime", runtime)
    assert main([]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "read-only-inventory"
    assert all(sql.lstrip().startswith("SELECT") for sql, _ in connection.statements)


@pytest.mark.parametrize("argv", [["--apply"], ["--mother-id", "m1"], ["--confirm-plan", "a" * 64]])
def test_cli_requires_complete_explicit_scope_before_loading_runtime(argv, monkeypatch):
    monkeypatch.setitem(sys.modules, "production_runtime", None)
    with pytest.raises(SystemExit) as error:
        main(argv)
    assert error.value.code == 2


def test_unknown_cart_conflict_and_orphan_batch_fail_closed():
    mother, batches, prompts = fixtures()
    prompts[0]["cart_enabled"] = True
    with pytest.raises(ValueError, match="挂车设置冲突"):
        build_plan(mother, batches, prompts, publish_purpose="养号", cart_setting="不挂车")
    prompts[0]["cart_enabled"] = None
    prompts[0]["batch_id"] = "another-scope"
    with pytest.raises(ValueError, match="范围不一致"):
        build_plan(mother, batches, prompts, publish_purpose="养号", cart_setting="不挂车")


def test_classification_cannot_merge_colliding_cumulative_slots():
    mother, batches, prompts = fixtures()
    known = deepcopy(prompts[0])
    known.update(prompt_id="already-known", publish_purpose="养号", cart_enabled=False)
    prompts.append(known)
    with pytest.raises(ValueError, match="累计序号冲突"):
        build_plan(mother, batches, prompts, publish_purpose="养号", cart_setting="不挂车")


def test_classification_cannot_merge_colliding_content_hashes():
    mother, batches, prompts = fixtures()
    prompts[0]["prompt_hash"] = "identical-digest"
    known = deepcopy(prompts[0])
    known.update(prompt_id="already-known", sequence_no=2, publish_purpose="养号", cart_enabled=False)
    prompts.append(known)
    with pytest.raises(ValueError, match="内容哈希冲突"):
        build_plan(mother, batches, prompts, publish_purpose="养号", cart_setting="不挂车")
