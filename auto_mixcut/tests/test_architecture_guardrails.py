from __future__ import annotations

from pathlib import Path
import unittest

from auto_mixcut.config.factory_config import FactoryConfig, env_flag
from auto_mixcut.domain.statuses import NextAction, PipelineStatus
from auto_mixcut.domain.source_types import LOW_TRUST_REFERENCE_SOURCE_TYPES, SourceType, TRUSTED_REAL_SOURCE_TYPES


ROOT = Path(__file__).resolve().parents[1]


class ArchitectureGuardrailTest(unittest.TestCase):
    def test_architecture_doc_lists_only_production_entry_points(self):
        text = (ROOT / "ARCHITECTURE.md").read_text(encoding="utf-8")

        self.assertIn("scripts/run_mixcut_task_scanner.py", text)
        self.assertIn("scripts/run_mixcut_guard.py", text)
        self.assertIn("scripts/run_ai_supplement_heartbeat.py", text)
        self.assertIn("scripts/run_ads_mixcut_unattended.py", text)
        self.assertIn("must not directly submit by default", text)

    def test_factory_config_defaults_are_conservative(self):
        cfg = FactoryConfig.from_env({})

        self.assertFalse(cfg.ads_fast_mode)
        self.assertFalse(cfg.allow_direct_submit)
        self.assertFalse(cfg.guard_submit_ai_packages)
        self.assertFalse(cfg.ads_allow_low_trust_first_slot)

    def test_factory_config_escape_hatches_are_explicit(self):
        cfg = FactoryConfig.from_env(
            {
                "AUTO_MIXCUT_ADS_FAST_MODE": "1",
                "AUTO_MIXCUT_ALLOW_DIRECT_SUBMIT": "true",
                "AUTO_MIXCUT_GUARD_SUBMIT_AI_PACKAGES": "yes",
                "AUTO_MIXCUT_ADS_ALLOW_LOW_TRUST_FIRST_SLOT": "on",
            }
        )

        self.assertTrue(cfg.ads_fast_mode)
        self.assertTrue(cfg.allow_direct_submit)
        self.assertTrue(cfg.guard_submit_ai_packages)
        self.assertTrue(cfg.ads_allow_low_trust_first_slot)

    def test_domain_constants_define_factory_boundaries(self):
        self.assertEqual(PipelineStatus.WAITING_AI_RETURN, "WAITING_AI_RETURN")
        self.assertEqual(NextAction.RUN_AI_SEGMENT_WORKER, "RUN_AI_SEGMENT_WORKER")
        self.assertIn(SourceType.SELF_SHOT, TRUSTED_REAL_SOURCE_TYPES)
        self.assertIn(SourceType.COMPETITOR, LOW_TRUST_REFERENCE_SOURCE_TYPES)
        self.assertTrue(env_flag({"X": "是"}, "X"))
        self.assertFalse(env_flag({"X": "off"}, "X", default=True))


if __name__ == "__main__":
    unittest.main()
