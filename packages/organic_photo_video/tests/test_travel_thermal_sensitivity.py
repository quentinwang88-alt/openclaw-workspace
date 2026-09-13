"""Travel temperature enhancement: optional article-level thermal_sensitivity.

The 0-15°C content returns to the live travel line as an *optional* variable
consumed only by the TEMPERATURE travel theme.  The five other travel themes
must behave exactly as before, legacy tasks must default to ``normal``, the
sensitivity must apply to the whole article (never per-Look), and the recipe /
policy / planning-prompt versions must stay independent of each other.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from services.feishu_workflow import (
    FIELD_THERMAL_SENSITIVITY,
    FeishuWorkflowError,
    build_travel_topic,
    resolve_travel_thermal_sensitivity,
)
from services.photo_reference_vision import (
    TRAVEL_PROMPT_VERSION,
    PhotoReferenceVisionService,
)
from services.photo_theme import (
    THEME_OPTIONS,
    resolve_photo_theme,
    travel_theme_templates,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
RECIPE_PATH = PACKAGE_ROOT / "config" / "recipes" / "PHOTO_TH_TRAVEL_OUTFIT_V2.json"
POLICY_PATH = (
    PACKAGE_ROOT / "config" / "photo_planning_policies" / "TH_TRAVEL_OUTFIT_V1.json"
)
TEMPLATE_PATH = PACKAGE_ROOT / "config" / "travel_theme_templates.json"

THERMAL_LABEL = "旅行·温度穿搭"
THERMAL_THEME_TYPE = "TEMPERATURE"
NON_THERMAL_LABELS = (
    "旅行·打卡穿搭", "旅行·环境协调", "旅行·拍照穿搭",
    "旅行·四选一", "旅行·配色参考",
)
LEGACY_PROFILE_VARIABLES = (
    "choice_axis", "destination", "temperature_band", "season", "style",
    "body_profile", "travel_goal",
)
PLANNING_BLOCK_HEADER = "【体感倾向"


def _recipe_payload() -> dict:
    return json.loads(RECIPE_PATH.read_text(encoding="utf-8"))


def _recipe_spec() -> dict:
    return dict(_recipe_payload()["recipe_spec"])


def _profile_variables() -> dict:
    return dict(_recipe_spec()["execution_profiles"][0]["variables"])


def _sensitivity_rule() -> dict:
    return dict(_recipe_spec()["variables_schema"]["thermal_sensitivity"])


def _sensitivity_planning() -> dict:
    payload = json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))
    return dict(
        payload["themes"][THERMAL_THEME_TYPE]["thermal_sensitivity_planning"]
    )


def _travel_contract() -> dict:
    return {"moments": [{
        "key": "old_town_walk", "label_zh": "老城", "evidence_zh": "街道",
        "label_th": "x", "forbidden_footwear_types": [],
    }]}


def _topic(label: str, sensitivity: str | None = None) -> dict:
    fields = {} if sensitivity is None else {FIELD_THERMAL_SENSITIVITY: sensitivity}
    return build_travel_topic(
        theme=resolve_photo_theme(label),
        travel_place="อาซากุสะ",
        travel_variables=_profile_variables(),
        fields=fields,
        recipe_spec=_recipe_spec(),
    )


def _prompt(topic: dict) -> str:
    return PhotoReferenceVisionService._travel_plan_prompt(
        analysis={}, travel_contract=_travel_contract(), variables={},
        content_requirement="", count=1, travel_topic=topic,
    )


class NonTemperatureThemesTest(unittest.TestCase):
    """其余五个旅行主题必须完全保持原行为。"""

    def test_non_temperature_travel_themes_ignore_thermal_sensitivity(self):
        for label in NON_THERMAL_LABELS:
            with self.subTest(label=label):
                baseline = _topic(label)
                hinted = _topic(label, "怕冷")
                self.assertNotEqual(baseline["theme_type"], THERMAL_THEME_TYPE)
                # The hint must not even enter the frozen brief…
                self.assertEqual(baseline, hinted)
                self.assertNotIn("thermal_sensitivity", hinted)
                self.assertNotIn("thermal_sensitivity_planning", hinted)
                # …so the planning prompt is byte-identical with and without it.
                self.assertEqual(_prompt(baseline), _prompt(hinted))
                self.assertNotIn(PLANNING_BLOCK_HEADER, _prompt(hinted))

    def test_non_temperature_prompt_keeps_v8_behaviour(self):
        # Legacy (no topic) travel prompt: still four distinct moments and no
        # publish copy — and no sensitivity guidance, since the hint only ever
        # enters the brief through the TEMPERATURE theme.
        legacy = PhotoReferenceVisionService._travel_plan_prompt(
            analysis={}, travel_contract=_travel_contract(), variables={},
            content_requirement="", count=1,
        )
        self.assertIn("每篇四个必须互不相同", legacy)
        self.assertIn("不要生成标题、正文或 CTA 文案", legacy)
        self.assertNotIn("【旅行主题联动】", legacy)
        self.assertNotIn(PLANNING_BLOCK_HEADER, legacy)
        # Topic-linked non-TEMPERATURE prompt keeps its linkage branch and still
        # gains nothing from the sensitivity column.
        linked = _prompt(_topic("旅行·环境协调", "怕冷"))
        self.assertIn("【旅行主题联动】", linked)
        self.assertIn("允许四套 Look 使用同一个 travel_moment", linked)
        self.assertNotIn(PLANNING_BLOCK_HEADER, linked)


class LegacyTaskDefaultTest(unittest.TestCase):
    """旧任务缺字段时默认 normal。"""

    def test_legacy_travel_task_defaults_to_normal_sensitivity(self):
        schema = _sensitivity_rule()
        self.assertEqual(schema["default"], "normal")
        self.assertFalse(schema["required"])
        # An empty column is "operator did not specify" → the recipe default.
        self.assertEqual(resolve_travel_thermal_sensitivity({}, {"thermal_sensitivity": schema}),
                         "normal")
        topic = _topic(THERMAL_LABEL)
        self.assertEqual(topic["thermal_sensitivity"], "normal")
        prompt = _prompt(topic)
        self.assertIn(PLANNING_BLOCK_HEADER, prompt)
        self.assertIn(_sensitivity_planning()["modifiers"]["normal"], prompt)
        self.assertNotIn(_sensitivity_planning()["modifiers"]["feels_cold"], prompt)

    def test_frozen_travel_profile_is_unchanged(self):
        # The shipped execution profile must not freeze a sensitivity value:
        # doing so would silently add the key to every travel theme's prompt.
        self.assertEqual(tuple(_profile_variables()), LEGACY_PROFILE_VARIABLES)
        self.assertNotIn("thermal_sensitivity", _profile_variables())

    def test_operator_labels_and_unknown_values(self):
        for label, expected in (("怕冷", "feels_cold"), ("正常体感", "normal"),
                                ("怕热", "feels_warm")):
            with self.subTest(label=label):
                self.assertEqual(
                    resolve_travel_thermal_sensitivity({FIELD_THERMAL_SENSITIVITY: label}),
                    expected,
                )
        with self.assertRaises(FeishuWorkflowError):
            resolve_travel_thermal_sensitivity({FIELD_THERMAL_SENSITIVITY: "很怕冷"})


class SensitivityModifierTest(unittest.TestCase):
    """温度主题按体感应用整篇调整，四套 Look 不分别代表怕冷/怕热。"""

    def test_temperature_travel_applies_sensitivity_modifier(self):
        modifiers = dict(_sensitivity_planning()["modifiers"])
        cold = _topic(THERMAL_LABEL, "怕冷")
        warm = _topic(THERMAL_LABEL, "怕热")
        self.assertEqual(cold["thermal_sensitivity"], "feels_cold")
        self.assertEqual(warm["thermal_sensitivity"], "feels_warm")
        # Guidance travels with the brief so the planner sees one authority.
        self.assertEqual(cold["thermal_sensitivity_planning"], _sensitivity_planning())
        cold_prompt, warm_prompt = _prompt(cold), _prompt(warm)
        self.assertIn(modifiers["feels_cold"], cold_prompt)
        self.assertNotIn(modifiers["feels_warm"], cold_prompt)
        self.assertIn(modifiers["feels_warm"], warm_prompt)
        self.assertNotIn(modifiers["feels_cold"], warm_prompt)

    def test_sensitivity_is_article_level_not_per_look(self):
        prompt = _prompt(_topic(THERMAL_LABEL, "怕冷"))
        for constraint in _sensitivity_planning()["constraints"]:
            self.assertIn(str(constraint), prompt)
        self.assertIn("四套 Look 必须使用同一体感", prompt)
        self.assertIn("不得分别代表怕冷或怕热", prompt)
        # The existing no-temperature-digit rule stays in force.
        self.assertIn("禁止出现任何具体温度数字", prompt)

    def test_theme_exposes_sensitivity_planning_only_for_temperature(self):
        for label in NON_THERMAL_LABELS:
            with self.subTest(label=label):
                self.assertFalse(
                    resolve_photo_theme(label).get("thermal_sensitivity_planning")
                )
        theme = resolve_photo_theme(THERMAL_LABEL)
        self.assertEqual(theme["travel_theme_type"], THERMAL_THEME_TYPE)
        self.assertEqual(theme["thermal_sensitivity_planning"], _sensitivity_planning())


class TravelThemeCountTest(unittest.TestCase):
    """六类旅行主题仍是六类，未因体感增强增删。"""

    def test_all_six_travel_themes_remain_six(self):
        templates = travel_theme_templates()
        self.assertEqual(len(templates), 6)
        labels = [str(value["label_zh"]) for value in templates.values()]
        self.assertEqual(len(set(labels)), 6)
        for label in labels:
            with self.subTest(label=label):
                self.assertIn(label, THEME_OPTIONS)
                theme = resolve_photo_theme(label)
                self.assertEqual(theme["theme_key"], "COOL_WEATHER_TRAVEL")
                self.assertTrue(theme["travel_theme_type"])
        self.assertEqual(
            [key for key, value in templates.items()
             if value.get("thermal_sensitivity_planning")],
            [THERMAL_THEME_TYPE],
        )

    def test_only_the_temperature_theme_version_moved(self):
        templates = travel_theme_templates()
        self.assertEqual(templates[THERMAL_THEME_TYPE]["version"], 2)
        others = {
            key: value["version"] for key, value in templates.items()
            if key != THERMAL_THEME_TYPE
        }
        self.assertTrue(all(version == 1 for version in others.values()), others)


class VersionIndependenceTest(unittest.TestCase):
    """配方版本、策略版本与提示词版本各自独立，互不派生。"""

    def test_travel_recipe_version_and_prompt_version_are_independent(self):
        recipe = _recipe_payload()
        policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        self.assertEqual(recipe["recipe_version"], 7)
        self.assertEqual(policy["policy_version"], 3)
        self.assertIn("v9", TRAVEL_PROMPT_VERSION)
        self.assertNotIn("v7", TRAVEL_PROMPT_VERSION)
        self.assertNotIn("v8", TRAVEL_PROMPT_VERSION)
        # Each artifact owns its own version number: the config payloads must
        # never embed the prompt version, and the prompt must not be derived
        # from the recipe version (that is how v8/recipe 6 drifted apart).
        recipe_text = json.dumps(recipe, ensure_ascii=False)
        policy_text = json.dumps(policy, ensure_ascii=False)
        self.assertNotIn(TRAVEL_PROMPT_VERSION, recipe_text)
        self.assertNotIn(TRAVEL_PROMPT_VERSION, policy_text)
        self.assertEqual(
            sorted({recipe["recipe_version"], policy["policy_version"]}),
            [3, 7],
        )
        # Bumping one must not force the other: the prompt version is a code
        # constant while the recipe/policy versions live in config.
        self.assertNotIn(f"v{recipe['recipe_version']}", TRAVEL_PROMPT_VERSION)
        self.assertIn("v9", TRAVEL_PROMPT_VERSION)

    def test_sensitivity_enum_shared_by_recipe_and_resolver(self):
        schema = _sensitivity_rule()
        self.assertEqual(schema["type"], "enum")
        self.assertEqual(
            sorted(schema["values"]), ["feels_cold", "feels_warm", "normal"]
        )
        for value in schema["values"]:
            with self.subTest(value=value):
                self.assertEqual(
                    resolve_travel_thermal_sensitivity({FIELD_THERMAL_SENSITIVITY: value}),
                    value,
                )
        # The declared default is a real member of the declared enum.
        self.assertIn(schema["default"], schema["values"])


if __name__ == "__main__":
    unittest.main()
