from __future__ import annotations

from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/run_first_frame_tasks.py"
SPEC = importlib.util.spec_from_file_location("wsr_first_frame_runner_test", SCRIPT)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)
from core.wsr_first_frame import build_wsr_first_frame_contract, render_wsr_first_frame_prompt
from core.bitable import FeishuBitableClient, FeishuAPIError


def row(record_id="recWSR1", **changes):
    fields = {"脚本ID": "wsr_prompt_test", "脚本来源": "成功脚本复刻", "产品编码": "S260724029604",
              "生成首帧（需勾选）": True, "进入生产": True, "视频时长": 10, "视频形态（系统）": "短视频",
              "视频生成提示词": "前两张人物、后两张产品。0-3秒：独立假发放在支架上。3-6秒：360度环绕。6-10秒：切换真人佩戴揭晓。",
              "产品图片": [{"file_token": "product1"}, {"file_token": "product2"}],
              "人物参考图（系统）": [{"file_token": f"person{i}"} for i in range(1, 5)], **changes}
    return {"record_id": record_id, "fields": fields}


def request(value=None):
    return {"protocol": "first-frame-record-v1", "source_url": runner.DEFAULT_SCRIPT_URL, "record": value or row()}


class Client:
    def __init__(self):
        self.updates = []
        self.uploads = 0
        self.upload_fail = False
        self.write_fail = False
        self.records = []

    def list_records(self, page_size):
        assert page_size == 500
        return self.records

    def update_record_fields(self, record_id, fields):
        self.updates.append((record_id, deepcopy(fields)))
        if self.write_fail and "统一首帧（系统）" in fields:
            self.write_fail = False
            raise RuntimeError("synthetic final row write failure")

    def upload_attachment(self, **kwargs):
        self.uploads += 1
        if self.upload_fail:
            self.upload_fail = False
            raise RuntimeError("synthetic upload failure")
        return {"file_token": "generated-frame"}


@pytest.fixture
def isolated(tmp_path):
    client = Client()

    def download(client, assets, output_dir, **kwargs):
        paths = []
        for index, asset in enumerate(assets):
            path = output_dir / f"ref-{index}-{asset['file_token']}.png"
            path.write_bytes(b"reference")
            paths.append(str(path))
        return paths

    def generate(**kwargs):
        path = kwargs["output_dir"] / "generated.png"
        path.write_bytes(b"synthetic-image")
        return path

    with patch.object(runner, "DEFAULT_OUTPUT_ROOT", tmp_path / "frames"), patch.object(runner, "_client", return_value=client), \
         patch.object(runner, "_download_references", side_effect=download) as downloads, \
         patch.object(runner, "_generate_image", side_effect=generate) as images, \
         patch.object(runner, "_load_script", side_effect=AssertionError("WSR must never load original scripts")) as original:
        yield SimpleNamespace(client=client, downloads=downloads, images=images, original=original,
                              db=str(tmp_path / "first.sqlite3"), root=tmp_path)


def test_contract_freezes_manual_final_prompt_and_current_ordered_assets():
    value = row(**{"短视频提示词": "人工改稿：0秒仅独立假发，后半段才真人揭晓。"})
    contract = build_wsr_first_frame_contract(record_id=value["record_id"], fields=value["fields"])
    prompt = render_wsr_first_frame_prompt(contract)
    assert contract["source_prompt"] == value["fields"]["短视频提示词"]
    assert contract["image_model"] == "gpt-image-2"
    assert [asset["file_token"] for asset in contract["ordered_reference_assets"]] == ["product1", "product2", "person1", "person2", "person3", "person4"]
    assert "图片6：人物身份参考" in prompt
    assert "本次全部作废" in prompt
    assert "若t=0只有独立商品，就只拍独立商品" in prompt
    assert "真人佩戴/揭晓不能提前" in prompt
    changed = deepcopy(value)
    changed["fields"]["短视频提示词"] += "采用支架特写。"
    assert build_wsr_first_frame_contract(record_id=changed["record_id"], fields=changed["fields"])["asset_fingerprint"] != contract["asset_fingerprint"]
    changed = deepcopy(value)
    changed["fields"]["人物参考图（系统）"].reverse()
    assert build_wsr_first_frame_contract(record_id=changed["record_id"], fields=changed["fields"])["asset_fingerprint"] != contract["asset_fingerprint"]


def test_snapshot_never_scans_or_touches_other_stale_records(isolated):
    isolated.client.list_records = Mock(side_effect=AssertionError("no full-table scan"))
    storage = runner.FirstFrameStorage(isolated.db)
    storage.upsert_asset(fingerprint="other", asset_id="OTHER", status="GENERATING")
    storage.bind(script_id="other", source_record_id="recOther", fingerprint="other", asset_id="OTHER", status="GENERATING")
    with sqlite3.connect(isolated.db) as conn:
        conn.execute("UPDATE original_first_frame_asset SET updated_at='2000-01-01 00:00:00'")
    result = runner.run_record_snapshot(request(), db_path=isolated.db)
    assert result["status"] == "ready"
    isolated.original.assert_not_called()
    isolated.client.list_records.assert_not_called()
    assert {record_id for record_id, _ in isolated.client.updates} == {"recWSR1"}
    assert all("进入生产" not in fields and "生成首帧（需勾选）" not in fields for _, fields in isolated.client.updates)
    with sqlite3.connect(isolated.db) as conn:
        assert conn.execute("SELECT status FROM original_first_frame_asset WHERE asset_fingerprint='other'").fetchone()[0] == "GENERATING"
    args = isolated.images.call_args.kwargs
    assert args["reference_roles"] == ["PRODUCT_IDENTITY"] * 2 + ["PERSONA_IDENTITY"] * 4
    assert len(args["reference_paths"]) == 6


def test_upload_failure_reuses_durable_image_without_second_generation(isolated):
    isolated.client.upload_fail = True
    first = runner.run_record_snapshot(request(), db_path=isolated.db)
    assert first["status"] == "failed"
    assert isolated.client.updates[-1] == ("recWSR1", {"首帧准备状态（系统）": "生成失败"})
    with sqlite3.connect(isolated.db) as conn:
        state, path = conn.execute("SELECT status,local_path FROM original_first_frame_asset").fetchone()
        assert state == "IMAGE_READY" and Path(path).is_file()
    second = runner.run_record_snapshot(request(), db_path=isolated.db)
    assert second["status"] == "ready" and second["cached"]
    assert isolated.images.call_count == 1
    assert isolated.downloads.call_count == 1
    assert isolated.client.uploads == 2


def test_final_write_failure_reuses_uploaded_attachment_without_second_generation(isolated):
    isolated.client.write_fail = True
    assert runner.run_record_snapshot(request(), db_path=isolated.db)["status"] == "failed"
    result = runner.run_record_snapshot(request(), db_path=isolated.db)
    assert result["status"] == "ready" and result["cached"]
    assert isolated.images.call_count == 1
    assert isolated.client.uploads == 1
    assert result["fields"]["视觉参考模式（系统）"] == "GENERATED_FIRST_FRAME"


def test_legacy_ready_snapshot_is_verified_not_blindly_reused(isolated):
    value = row(**{"首帧准备状态（系统）": "已就绪", "统一首帧（系统）": [{"file_token": "approved"}]})
    result = runner.run_record_snapshot(request(value), db_path=isolated.db)
    assert result["status"] == "ready"
    assert isolated.images.call_count == 1


def test_ready_input_changes_and_same_bytes_new_token(isolated):
    value = row()
    first = runner.run_record_snapshot(request(value), db_path=isolated.db)
    value["fields"].update(first["fields"])
    # Changed upload identity, identical bytes: no second generation.
    value["fields"]["产品图片"][0]["file_token"] = "new-token-same-image"
    second = runner.run_record_snapshot(request(value), db_path=isolated.db)
    assert second["fingerprint"] == first["fingerprint"]
    assert isolated.images.call_count == 1
    value["fields"]["视频生成提示词"] += "开场改成侧面近景。"
    third = runner.run_record_snapshot(request(value), db_path=isolated.db)
    assert third["fingerprint"] != first["fingerprint"]
    assert isolated.images.call_count == 2


def test_same_named_changed_local_bytes_invalidate_ready_cache(isolated):
    image = isolated.root / "same-name.png"
    image.write_bytes(b"first-bytes")
    value = row(**{"产品图片": [{"file_token": "same", "local_path": str(image)}], "人物参考图（系统）": []})
    isolated.downloads.side_effect = lambda _client, assets, *_args, **_kwargs: [asset["local_path"] for asset in assets]
    first = runner.run_record_snapshot(request(value), db_path=isolated.db)
    value["fields"].update(first["fields"])
    image.write_bytes(b"replaced-bytes")
    second = runner.run_record_snapshot(request(value), db_path=isolated.db)
    assert second["fingerprint"] != first["fingerprint"]
    assert isolated.images.call_count == 2


def test_frozen_original_hash_survives_new_token_and_new_derivative(isolated):
    raw = hashlib.sha256(b"original-camera-file").hexdigest()
    def manifest(token, content):
        assets = [{"index": 1, "role": "product", "file_token": token,
                   "original_sha256": raw, "derived_sha256": hashlib.sha256(content).hexdigest()}]
        identity = [{"index": 1, "role": "product", "original_sha256": raw}]
        return {"schema_version": "2", "reference_assets": assets,
                "manifest_id": "sha256:" + hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
    value = row(**{"产品图片": [{"file_token": "product1"}], "人物参考图（系统）": [],
                   "_wsr_reference_manifest": manifest("product1", b"reference")})
    first = runner.run_record_snapshot(request(value), db_path=isolated.db)
    value["fields"].update(first["fields"])
    value["fields"]["产品图片"] = [{"file_token": "new-token"}]
    value["fields"]["_wsr_reference_manifest"] = manifest("new-token", b"new-jpeg-encoding")
    def download(_client, _assets, output_dir, **_kwargs):
        path = output_dir / "new-encoding.jpg"
        path.write_bytes(b"new-jpeg-encoding")
        return [str(path)]
    isolated.downloads.side_effect = download
    second = runner.run_record_snapshot(request(value), db_path=isolated.db)
    assert second["fingerprint"] == first["fingerprint"]
    assert isolated.images.call_count == 1


def test_execution_frozen_never_generates_or_patches_ready_frame(isolated):
    with patch.object(runner, "_wsr_execution_frozen", return_value=True):
        result = runner.run_record_snapshot(request(), db_path=isolated.db)
    assert result["status"] == "unsupported"
    assert "EXECUTION_FROZEN" in result["error"]
    isolated.images.assert_not_called()
    assert isolated.client.updates == []


@pytest.mark.parametrize("target_fields", [
    {"脚本ID": "wsr_prompt_test", "任务状态": "已提交", "MiniMax任务ID": "h3-existing"},
    {"脚本ID": "wsr_prompt_test"},
    {"脚本ID": "another-script", "任务状态": "待处理"},
])
def test_standalone_h3_submitted_or_unknown_without_publish_row_preserves_frame(isolated, target_fields):
    value = row(**{"运行任务ID": "recH3Task", "首帧准备状态（系统）": "已就绪",
                   "统一首帧（系统）": [{"file_token": "existing-frame"}]})
    isolated.client.records = [SimpleNamespace(**value)]
    isolated.client.get_record = Mock(return_value=SimpleNamespace(record_id="recH3Task", fields=target_fields))
    with patch.object(runner, "_wsr_execution_frozen", return_value=False):
        result = runner.run_tasks(record_id="recWSR1", db_path=isolated.db)
    assert result["skipped"] == 1 and result["failed"] == 0
    isolated.client.get_record.assert_called_once_with("recH3Task")
    isolated.images.assert_not_called()
    assert isolated.client.updates == []
    assert value["fields"]["首帧准备状态（系统）"] == "已就绪"


def test_unreachable_target_preserves_existing_frame(isolated):
    isolated.client.records = [SimpleNamespace(**row(**{"运行任务ID": "recH3Task"}))]
    isolated.client.get_record = Mock(side_effect=RuntimeError("target unavailable"))
    with patch.object(runner, "_wsr_execution_frozen", return_value=False):
        result = runner.run_tasks(record_id="recWSR1", db_path=isolated.db)
    assert result["skipped"] == 1
    isolated.images.assert_not_called()
    assert isolated.client.updates == []


def test_sync_snapshot_reuses_checked_target_state_without_another_read(isolated):
    value = request(row(**{"运行任务ID": "recH3Task"}))
    value["execution_target_snapshot"] = {"record_id": "recH3Task", "fields": {
        "脚本ID": "wsr_prompt_test", "任务状态": "待处理"}}
    isolated.client.get_record = Mock(side_effect=AssertionError("snapshot must avoid duplicate lookup"))
    result = runner.run_record_snapshot(value, db_path=isolated.db)
    assert result["status"] == "ready"
    isolated.client.get_record.assert_not_called()


def test_target_get_record_is_single_read_and_validates_identity():
    client = FeishuBitableClient.__new__(FeishuBitableClient)
    client.app_token, client.table_id = "app", "table"
    client._headers = Mock(return_value={})
    response = Mock()
    response.json.return_value = {"code": 0, "data": {"record": {"record_id": "recH3Task", "fields": {"任务状态": "已提交"}}}}
    client._request = Mock(return_value=response)
    record = client.get_record("recH3Task")
    assert record.fields["任务状态"] == "已提交"
    assert client._request.call_count == 1
    assert client._request.call_args.args[0] == "GET"
    assert client._request.call_args.args[1].endswith("/records/recH3Task")
    response.json.return_value["data"]["record"]["record_id"] = "recWrong"
    with pytest.raises(FeishuAPIError):
        client.get_record("recH3Task")


@pytest.mark.parametrize("changed", [
    {"生成首帧（需勾选）": False}, {"进入生产": False}, {"脚本来源": "原创生成"},
    {"视频时长": 20}, {"视频形态（系统）": "LONGFORM"},
    {"脚本ID": "not-wsr", "脚本来源": "视频复刻"},
])
def test_snapshot_requires_both_checked_short_supported_identity(isolated, changed):
    result = runner.run_record_snapshot(request(row(**changed)), db_path=isolated.db)
    assert result["status"] == "unsupported"
    assert isolated.client.updates == []
    isolated.images.assert_not_called()


def test_malformed_reference_fails_before_paid_generation(isolated):
    value = row(**{"人物参考图（系统）": [{"name": "missing-token"}]})
    result = runner.run_record_snapshot(request(value), db_path=isolated.db)
    assert result["status"] == "failed" and "REFERENCE_INVALID" in result["error"]
    isolated.images.assert_not_called()


def test_nurture_without_product_or_references_can_use_existing_text_design(isolated):
    value = row(**{"产品编码": "", "产品图片": [], "人物参考图（系统）": [], "视频生成提示词": "0-3秒：一只纸风车在窗边。3-10秒自然光线变化。"})
    assert runner.run_record_snapshot(request(value), db_path=isolated.db)["status"] == "ready"
    assert isolated.images.call_args.kwargs["reference_paths"] == []
    assert "不要强行加商品" in isolated.images.call_args.kwargs["prompt"]


def test_shared_record_lock_prevents_parallel_standalone_and_snapshot_payment(isolated):
    with runner._first_frame_lock(f"record:{runner.DEFAULT_SCRIPT_URL}:recWSR1"):
        result = runner.run_record_snapshot(request(), db_path=isolated.db)
    assert result["status"] == "failed" and "FIRST_FRAME_LOCKED" in result["error"]
    isolated.images.assert_not_called()
    assert isolated.client.updates == []


def test_ready_rows_do_not_starve_later_selected_standalone_records(isolated):
    ready = row("recReady", **{"进入生产": False, "首帧准备状态（系统）": "已就绪", "统一首帧（系统）": [{"file_token": "ready"}]})
    pending = row("recPending", **{"脚本ID": "wsr_prompt_pending"})
    isolated.client.records = [SimpleNamespace(**ready), SimpleNamespace(**pending)]
    result = runner.run_tasks(limit=1, db_path=isolated.db)
    assert result["selected"] == 1 and result["ready"] == 1
    assert {record_id for record_id, _ in isolated.client.updates} == {"recPending"}


def test_standalone_dry_run_does_not_create_database_or_write_fields(isolated):
    isolated.client.records = [SimpleNamespace(**row())]
    result = runner.run_tasks(limit=1, db_path=isolated.db, dry_run=True)
    assert result["selected"] == 1
    assert not Path(isolated.db).exists()
    assert isolated.client.updates == []
    isolated.images.assert_not_called()


def test_stage0_repeated_slot_always_validates_hash_identity(tmp_path):
    root = tmp_path / ".openclaw/shared/data/original_production_runs/test"
    root.mkdir(parents=True)
    artifact = root / "stage0_result.json"
    first = {"complete_script_id": "first", "reality_reference_provenance": {"creative_blueprint_id": "bp1"}}
    second = {"complete_script_id": "second", "reality_reference_provenance": {"creative_blueprint_id": "bp2"}}
    artifact.write_text(json.dumps({"products": [{"stage0_run_id": 42, "directions": [
        {"output_slot": "S1", "script": first}, {"output_slot": "S1", "script": second}]}]}))
    batch = "STAGE0_" + hashlib.sha256(str(artifact.resolve()).encode()).hexdigest()[:16].upper()
    expected_id = "SCSCRIPT_" + hashlib.sha256(json.dumps({"batch_id": batch, "slot": "S1", "blueprint_id": "bp2"},
                                                        ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()[:24].upper()
    with patch.object(runner.Path, "home", return_value=tmp_path):
        assert runner._load_stage0_script("SCSCRIPT_" + "0" * 24) is None
        assert runner._load_stage0_script(expected_id) == second
        assert runner._load_stage0_script("SCSCRIPT_STAGE0_42_S1") == first
    with patch.object(runner.Path, "home", side_effect=AssertionError("WSR must not scan stage0")):
        assert runner._load_stage0_script("wsr_prompt_test") is None


def test_snapshot_cli_emits_machine_result_even_for_unsupported_scope(isolated):
    source = isolated.root / "request.json"
    destination = isolated.root / "result.json"
    source.write_text(json.dumps(request(row(**{"进入生产": False})), ensure_ascii=False))
    assert runner.main(["--record-snapshot", str(source), "--result-json", str(destination), "--db-path", isolated.db]) == 1
    result = json.loads(destination.read_text())
    assert result["record_id"] == "recWSR1" and result["script_id"] == "wsr_prompt_test"
    assert result["status"] == "unsupported"
    isolated.images.assert_not_called()
