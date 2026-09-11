"""Compile closure identity once, with image-scoped operator corrections.

No model call and no change to the shared 15s identity compiler.
"""
import copy
import json
import os
import re
from pathlib import Path

from core.simplified_complete_script import build_product_identity_lock

LABELS = {"SNAP": "按扣", "BUTTON": "纽扣", "ZIPPER": "拉链"}
TOKENS = {"SNAP": r"按扣|暗扣|揿扣", "BUTTON": r"纽扣|扣子|圆扣", "ZIPPER": r"拉链"}


def compile_longform_identity(truth, context):
    truth = copy.deepcopy(truth)
    path = Path(os.environ.get("LONGFORM_IDENTITY_CORRECTIONS_PATH") or
                Path.home() / ".openclaw/shared/data/product_identity_corrections.json")
    corrections = json.loads(path.read_text()) if path.is_file() else {}
    refs = context.get("product_reference_assets") or []
    hashes = {r.get("sha256") for r in refs if isinstance(r, dict) and r.get("sha256")}
    correction = next((r for r in corrections.get("corrections", [])
                       if r.get("product_code") == context.get("product_code")
                       and r.get("authority") == "USER_CONFIRMED"
                       and hashes and hashes == set(r.get("reference_sha256s") or [])), None)
    evidence = "；".join(truth.get("identity_anchors") or [])
    mechanisms = []
    if correction:
        mechanisms = [m for m in correction.get("closure_mechanisms", []) if m in LABELS]
        if not mechanisms:
            raise ValueError("人工闭合结构修正缺少有效mechanism")
        description = "前襟采用" + "与".join(LABELS[m] for m in mechanisms) + "闭合"
        # Retract the conflicting clause, not every other detail in its anchor.
        def corrected_anchor(value):
            parts = re.split(r"[，,；;。]", str(value))
            result = [p for p in parts if p and not any(re.search(pat, p) for pat in TOKENS.values())]
            return "，".join(result)
        for key in ("identity_anchors", "visible_detail_anchors"):
            truth[key] = list(filter(None, (corrected_anchor(v) for v in truth.get(key) or [])))
        truth["identity_anchors"].append(description)
        # Withdraw conflicting optional visual facts, without editing operator copy.
        truth["approved_claims"] = [v for v in truth.get("approved_claims") or []
                                    if not any(re.search(TOKENS[m], str(v.get("fact_text", "")))
                                               for m in LABELS if m not in mechanisms)]
    else:
        for name, pattern in TOKENS.items():
            positive = re.sub(r"(?:无|没有|不带|未见|不含|并非|不是|不采用|非)[^，；。]{0,3}(?:" + pattern + r")", "", evidence)
            if re.search(pattern, positive):
                mechanisms.append(name)
        if "SNAP" in mechanisms and "BUTTON" in mechanisms:
            mechanisms.remove("BUTTON")  # generic 扣子 is not a second mechanism
        description = "前襟采用" + "与".join(LABELS[m] for m in mechanisms) + "闭合" if mechanisms else ""
    identity = build_product_identity_lock(truth)
    closure = dict(identity.get("visible_closure_contract") or {})
    if closure.get("status") == "NOT_APPLICABLE":
        return truth, identity
    if mechanisms:
        closure.update(status="AVAILABLE", mechanisms=mechanisms,
                       mechanism_description=description,
                       authority="USER_CONFIRMED" if correction else "APPROVED_ANCHOR")
        closure.setdefault("visible_description", description)
        closure.setdefault("layout", "UNSPECIFIED")
    if correction:
        closure["correction_id"] = correction["correction_id"]
        closure["reference_sha256s"] = sorted(hashes)
        closure["exclusive"] = bool(correction.get("exclusive"))
        identity["must_not_change"] = [v for v in identity.get("must_not_change", [])
                                       if "拉链替换" not in v]
        if correction.get("exclusive"):
            identity["must_not_change"].append("闭合结构仅为" + "与".join(LABELS[m] for m in mechanisms) + "，不新增其他闭合件")
    elif "ZIPPER" not in mechanisms:
        identity["must_not_change"] = [v for v in identity.get("must_not_change", []) if "拉链替换" not in v]
    identity["visible_closure_contract"] = closure
    identity["compiler_version"] = "longform-identity-v1-closure-authority"
    return truth, identity
