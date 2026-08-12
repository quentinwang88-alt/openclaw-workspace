import unittest

from scripts.backfill_scarf_batch_handoff import patch_item_payloads


class ScarfBatchHandoffBackfillTest(unittest.TestCase):
    def test_adds_frozen_single_mode_without_rewriting_script_content(self):
        result = {
            "script": {
                "script_concept": {"one_sentence_idea": "原脚本"},
                "production_design": {"presentation_mode": "PERSON_ON_CAMERA"},
                "video_generation_brief": {},
            }
        }
        frozen, patched = patch_item_payloads(
            frozen={"simplified_creative_seed": {}},
            result=result,
            anchor_card={"hard_anchors": ["蓝色图案方巾"]},
            product_type="丝巾",
            top_category="配饰",
            selling_argument={
                "argument_theme": "HAIR_RESCUE",
                "primary_demonstration_mode": "HAIR_TIE",
                "supported_demonstration_modes": ["HAIR_TIE", "NECK_WORN"],
            },
        )
        profile = frozen["category_execution_extension"]["profile"]
        self.assertEqual("HAIR_TIE", profile["primary_demonstration_mode"])
        script = patched["script"]
        self.assertEqual("原脚本", script["script_concept"]["one_sentence_idea"])
        self.assertIn(
            "accessory_execution_brief", script["video_generation_brief"]
        )


if __name__ == "__main__":
    unittest.main()
