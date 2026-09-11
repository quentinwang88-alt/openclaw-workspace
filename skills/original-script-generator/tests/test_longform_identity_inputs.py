import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from core.longform.assets import freeze_reference_assets
from core.longform.product_identity import compile_longform_identity
from core.longform.voiceover import _closure_language_contract
from core.persona_selection import select_persona_contract
from scripts.run_feishu_operation_tasks import _cache_longform_persona_references
from scripts.run_longform_original import _k0_reference_paths, _reference_image_guidance


class IdentityInputsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.overrides = self.root / "corrections.json"
        self.env = patch.dict("os.environ", {"LONGFORM_IDENTITY_CORRECTIONS_PATH": str(self.overrides)})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_image_scoped_user_correction_not_product_wide_blacklist(self):
        self.overrides.write_text(json.dumps({"corrections": [{
            "correction_id": "R1", "product_code": "P1", "authority": "USER_CONFIRMED",
            "reference_sha256s": ["h1"], "closure_mechanisms": ["SNAP"], "exclusive": True,
        }]}))
        truth = {"canonical_product_type": "outerwear",
                 "identity_anchors": ["蓝色短款外套", "高立领，前中有拉链与按扣门襟"],
                 "approved_claims": [{"claim_key": "C1", "fact_text": "正面拉链"}]}
        original = copy.deepcopy(truth)
        context = {"product_code": "P1", "product_reference_assets": [{"sha256": "h1"}]}
        corrected, lock = compile_longform_identity(truth, context)
        self.assertNotIn("拉链", str(corrected))
        self.assertIn("高立领", str(corrected))
        self.assertNotIn("拉链替换", str(lock))
        self.assertEqual(lock["visible_closure_contract"]["mechanisms"], ["SNAP"])
        self.assertEqual(_closure_language_contract({"product_identity_lock": lock})["mode"], "SNAP_VERIFIED")
        context["product_reference_assets"] = [{"sha256": "other"}]
        other, lock = compile_longform_identity(truth, context)
        self.assertEqual(other, original)
        self.assertEqual(set(lock["visible_closure_contract"]["mechanisms"]), {"SNAP", "ZIPPER"})
        self.assertEqual(truth, original)

    def test_negative_zipper_is_not_positive_evidence(self):
        _, lock = compile_longform_identity({"identity_anchors": ["外套，无拉链，按扣闭合"]}, {})
        self.assertEqual(lock["visible_closure_contract"]["mechanisms"], ["SNAP"])
        lock["must_not_change"].append("禁止新增拉链")
        self.assertEqual(_closure_language_contract({"product_identity_lock": lock})["mode"], "SNAP_VERIFIED")

    def test_old_single_row_and_accessory_contracts_remain_compatible(self):
        _, lock = compile_longform_identity({"identity_anchors": ["单排五颗纽扣外套"]}, {})
        self.assertEqual(lock["visible_closure_contract"]["layout"], "SINGLE_VISIBLE_VERTICAL_ROW")
        _, lock = compile_longform_identity({"canonical_product_type": "silk_scarf",
                                             "identity_anchors": ["蓝色方巾"]}, {})
        self.assertEqual(lock["visible_closure_contract"]["status"], "NOT_APPLICABLE")

    def test_pack_cache_and_manifest_keep_primary_and_views(self):
        refs = []
        for i, view in enumerate(("FACE_SIDE", "FACE_FRONT_NEUTRAL")):
            path = self.root / f"person{i}.png"
            path.write_bytes(f"image{i}".encode())
            refs.append({"local_path": str(path), "role": view, "is_primary": i == 1,
                         "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        frozen = {"creative_diversity_contract": {"persona_selection_contract": {
            "persona_id": "P1", "reference_images": refs}}}
        client = Mock()
        assets = _cache_longform_persona_references(client, frozen, self.root / "cache")
        manifest = freeze_reference_assets(job_id="J1", asset_root=self.root,
            materials=[{"persona_reference_assets": assets}], persona_lock={"persona_id": "P1"})
        self.assertTrue(manifest["assets"][0]["is_primary"])
        self.assertEqual(manifest["assets"][0]["reference_view"], "FACE_FRONT_NEUTRAL")
        keyframes = {"frozen_reference_assets": manifest}
        paths = _k0_reference_paths(keyframes)
        self.assertEqual(len(paths), 2)
        self.assertIn("图1：主人物身份参考", _reference_image_guidance(keyframes, paths))
        client.download_attachment_bytes.assert_not_called()

    def test_persona_pack_preference_is_longform_opt_in_not_override(self):
        def persona(pid, refs, priority):
            return {"persona_id": pid, "markets": ["TH"], "reference_images": refs,
                    "reference_asset_ids": [pid], "priority": priority}
        single = persona("SINGLE", [{"file_token": "one"}], 100)
        pack = persona("PACK", [{"role": "FACE_FRONT_NEUTRAL"}, {"role": "FACE_SIDE"}], 0)
        with patch("core.persona_selection.load_persona_templates", return_value={
                "status": "AVAILABLE", "templates": [single, pack]}):
            kwargs = dict(product_type="外套", top_category="女装", country="泰国",
                          presentation_mode="PERSON_ON_CAMERA", capture_mode="CREATOR_SELF_SHOT",
                          demonstration_mode="", seed=1, recent_usage=[])
            self.assertEqual(select_persona_contract(**kwargs)[0]["persona_id"], "SINGLE")
            self.assertEqual(select_persona_contract(**kwargs, prefer_reference_pack=True)[0]["persona_id"], "PACK")
            self.assertEqual(select_persona_contract(**kwargs, prefer_reference_pack=True,
                             preferred_persona_ids=["SINGLE"])[0]["persona_id"], "SINGLE")


if __name__ == "__main__":
    unittest.main()
