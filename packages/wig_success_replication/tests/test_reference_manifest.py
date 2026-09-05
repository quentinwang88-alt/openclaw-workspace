import unittest
import base64

from wig_success_replication.reference_manifest import (
    build_reference_manifest, bytes_sha256, rebind_reference_manifest, validate_reference_manifest,
)
from wig_success_replication.core_points import mother_core_points, obvious_omissions
from wig_success_replication.deterministic_qa import _marker, inspect_compile_output
from wig_success_replication.models import Appearance, ProductFactCard, ReplicationCompileOutput
from wig_success_replication.variant_planner import VariantPlanner


class ReferenceManifestTest(unittest.TestCase):
    def assets(self):
        return [{"index": 1, "role": "person_identity", "file_token": "source-person", "original_sha256": bytes_sha256(b"person")},
                {"index": 2, "role": "product", "file_token": "source-product", "original_sha256": bytes_sha256(b"wig")}]

    def test_tokens_do_not_define_identity_but_order_role_and_bytes_do(self):
        assets = self.assets()
        manifest = build_reference_manifest(assets)
        assets[0]["file_token"] = "another-base-token"
        assets[0]["derived_sha256"] = bytes_sha256(b"compressed")
        self.assertEqual(manifest["manifest_id"], build_reference_manifest(assets)["manifest_id"])
        assets[0]["original_sha256"] = bytes_sha256(b"changed person")
        self.assertNotEqual(manifest["manifest_id"], build_reference_manifest(assets)["manifest_id"])
        assets = self.assets()
        assets[0]["role"] = "product"
        self.assertNotEqual(manifest["manifest_id"], build_reference_manifest(assets)["manifest_id"])

    def test_verified_transfer_changes_locator_only(self):
        manifest = build_reference_manifest(self.assets())
        tokens = {asset["file_token"]: {"file_token": "new-" + asset["file_token"], "original_sha256": asset["original_sha256"]}
                  for asset in self.assets()}
        copied = rebind_reference_manifest(manifest, tokens)
        self.assertEqual(copied["manifest_id"], manifest["manifest_id"])
        self.assertEqual(copied["reference_assets"][0]["file_token"], "new-source-person")
        self.assertEqual(manifest["reference_assets"][0]["file_token"], "source-person")
        tokens["source-person"]["original_sha256"] = bytes_sha256(b"wrong")
        with self.assertRaisesRegex(ValueError, "bytes changed"):
            rebind_reference_manifest(manifest, tokens)

    def test_tampered_manifest_rejected(self):
        manifest = build_reference_manifest(self.assets())
        manifest["reference_assets"][0]["original_sha256"] = bytes_sha256(b"other")
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            validate_reference_manifest(manifest)

    def test_six_reference_compilation_and_handoff_use_identical_manifest(self):
        from test_v1_1 import _seed_repository
        from test_script_pool_v1 import LLM
        from wig_success_replication.replication_service import ReplicationBatchService
        repo, llm = _seed_repository(["source"]), LLM()
        raw = [f"reference-{index}".encode() for index in range(6)]
        images = ["data:image/jpeg;base64," + base64.b64encode(value).decode() for value in raw]
        assets = [{"index": index + 1, "role": "person_identity" if index < 4 else "product", "file_token": f"t{index}",
                   "original_sha256": bytes_sha256(value), "derived_sha256": bytes_sha256(value)} for index, value in enumerate(raw)]
        manifest = build_reference_manifest(assets)
        kwargs = dict(mother_id="m", product_ids=["source"], per_product_count=1, publish_purpose="养号",
                      face_reference_image_urls=images[:4], product_image_urls={"source": images[4:]},
                      handoff_context_by_product={"source": {"reference_manifest": manifest}})
        result = ReplicationBatchService(repo, llm).generate(**kwargs)
        self.assertEqual(result.saved_prompts, 1)
        self.assertEqual(llm.calls[0]["image_urls"], images)
        self.assertEqual(llm.calls[0]["user_payload"]["attached_image_order"]["face_reference_count"], 4)
        row = repo.list_prompts("m", 1, "source", "养号")[0]
        self.assertEqual(row.handoff_context["reference_manifest"], manifest)
        self.assertEqual(row.handoff_context["prompt_qa"]["semantic_status"], "not_evaluated")
        self.assertEqual(len(row.handoff_context["mother_core_points"]), 4)
        # A changed subset cannot sneak through a persisted 6-image manifest.
        repo2, llm2 = _seed_repository(["source"]), LLM()
        kwargs["face_reference_image_urls"] = images[:2]
        failed = ReplicationBatchService(repo2, llm2).generate(**kwargs)
        self.assertEqual(failed.saved_prompts, 0)
        self.assertEqual(llm2.calls, [])


class MotherCoreChecksTest(unittest.TestCase):
    contract = {"core_mechanism": {
        "hook_engine": "同一女生双掌展示与假发对应的双色颜料",
        "reveal_engine": "真实摩擦形成融合色，完全覆盖镜头0.3秒再露假发与脸",
        "proof_engine": "同一人物展示假发，静止后转头触摸",
        "conversion_engine": "无口播无销售CTA，以微笑收尾",
    }}

    def test_legacy_contract_derived_points_are_transparently_marked(self):
        points = mother_core_points(self.contract)
        self.assertEqual(len(points), 4)
        self.assertTrue(all(point["evidence_source"] == "legacy_derived" for point in points))
        self.assertNotIn("core_points", self.contract)

    def test_coffee_without_person_or_wig_no_longer_passes_nurture_qa(self):
        product = ProductFactCard(product_id="audit", market=["MX"], appearance=Appearance(
            length="unknown", texture="unknown", color="unknown", bangs="unknown", layers="unknown", face_framing="unknown"),
            confirmed_selling_points=[], visual_proof_actions=[], forbidden_claims=[], uncertain_points=[], evidence_notes=[])
        plan = VariantPlanner().plan("same_product", 10, existing_sequences=range(1, 10), publish_purpose="养号")
        baseline = _marker("audit", "audit").prompt
        item = baseline.model_copy(update={
            **{key: getattr(plan[0], key) for key in ("sequence_no", "variant_type", "variant_key", "mutation_key", "replication_mode", "creative_route")},
            "full_prompt": "只参考脸部。产品图片。不上传参考视频。静态咖啡杯，不出现人物，不出现假发。",
            "creative_signature": baseline.creative_signature.model_copy(update={"voiceover_text": "", "changed_dimensions": plan[0].change_dimensions}),
        })
        output = ReplicationCompileOutput(batch_id="audit", mother_id="audit", mother_version=1, product_id="audit", relationship="same_product", outputs=[item])
        check = inspect_compile_output(output, plan, product, publish_purpose="养号", mother_contract=self.contract)[0]
        self.assertFalse(check.passed)
        self.assertIn("mother_core_contradiction:wig_removed", check.issues)
        self.assertIn("mother_core_contradiction:person_removed", check.issues)
        self.assertFalse(any("obvious_omission" in issue for issue in check.issues))
        self.assertEqual(check.semantic_status, "not_evaluated")

    def test_static_terms_do_not_certify_semantics_or_add_paint_to_other_mothers(self):
        text = "颜料慢快摩擦形成融合色，手掌完全覆盖镜头0.3秒。"
        self.assertEqual(obvious_omissions(text, self.contract), [])
        other = {"core_mechanism": {"hook_engine": "咖啡杯的惊喜", "reveal_engine": "咖啡杯", "proof_engine": "拉花", "conversion_engine": "微笑"}}
        self.assertEqual(obvious_omissions("咖啡杯", other), [])

    def test_negative_reference_attributes_do_not_mean_subject_removed(self):
        mechanism = "颜料慢快摩擦形成融合色，手掌完全覆盖镜头。"
        for text in ("不出现人物参考图里的粉黑长发。", "不出现假发图中的文字。",
                     "不出现假发漂浮或改变人物身份的情况。", "Before阶段不出现假发。"):
            with self.subTest(text=text):
                self.assertEqual(obvious_omissions(text + mechanism, self.contract), [])
        issues = obvious_omissions("全片不出现人物，不展示任何假发。" + mechanism, self.contract)
        self.assertEqual(set(issues), {"mother_core_contradiction:person_removed", "mother_core_contradiction:wig_removed"})

    def test_continuous_occlusion_does_not_require_a_literal_decimal_duration(self):
        for duration in ("短暂停留", "停留0.4秒", "连续遮挡直至切换完成"):
            text = "双色颜料摩擦后形成融合色，手掌完全覆盖镜头，" + duration + "。"
            self.assertEqual(obvious_omissions(text, self.contract), [])
        # Missing a preferred word is not proof of missing an action; this
        # static pass must not claim to certify the resulting visual sequence.
        self.assertEqual(obvious_omissions("双色颜料摩擦形成融合色，直接变发。", self.contract), [])

    def test_paint_mother_without_occlusion_and_occlusion_synonyms_are_not_blocked(self):
        paint_only = {"core_mechanism": {"hook_engine": "展示双色颜料", "reveal_engine": "摩擦混色后展示颜色"}}
        self.assertEqual(obvious_omissions("双色揉合后摊掌展示新颜色。", paint_only), [])
        self.assertEqual(obvious_omissions("颜料混色后，一只手100%覆盖整个画面，然后再展现新发型。", self.contract), [])
