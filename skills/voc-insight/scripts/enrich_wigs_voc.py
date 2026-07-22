#!/usr/bin/env python3
"""Deterministic MX wigs VOC enrichment dry-run and gold-sample validation.

This script deliberately has no database write mode. It produces reviewable JSON
artifacts first; promotion into fastmoss_voc_enriched remains a separate, explicit
step after the quality gate passes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


SKILL_DIR = Path(__file__).resolve().parents[1]
TAXONOMY_PATH = SKILL_DIR / "references" / "wigs_voc_taxonomy_v1.json"
DEFAULT_GOLD_PATH = SKILL_DIR / "references" / "wigs_voc_gold_sample_v1.json"
RUNNER_PATH = SKILL_DIR / "scripts" / "run_voc_insight.py"


def normalize(text: Any) -> str:
    raw = unicodedata.normalize("NFKD", str(text or "").lower())
    ascii_text = "".join(ch for ch in raw if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", ascii_text).strip()


def load_json(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def load_runner_module():
    spec = importlib.util.spec_from_file_location("voc_insight_runner", str(RUNNER_PATH))
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to import run_voc_insight.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FORM_PATTERNS: Sequence[Tuple[str, Sequence[str]]] = (
    ("lace_wig", (r"\blace\b", r"frontal", r"encaje")),
    ("ponytail_extension", (r"coleta", r"ponytail", r"cola de caballo")),
    ("clip_in_extension", (r"con (?:\d+ )?clips?", r"clip[- ]?in", r"\b[2-9]\s*piezas\b", r"cortina")),
    ("braiding_hair", (r"trenzas? jumbo", r"jumbo.*trenz", r"cabello para trenz", r"crochet", r"preestirad")),
    ("hairpiece_extension", (r"postizo invisible", r"extension invisible", r"\bhalo\b", r"topper", r"pieza de cabello", r"pieza de \d+ pulgadas de flequillo", r"\bbangs?\b", r"claw clip attachment")),
    ("full_wig", (r"\bpeluca\b", r"\bwig\b")),
)


def classify_form(title: str) -> str:
    text = normalize(title)
    for form, patterns in FORM_PATTERNS:
        if any(re.search(pattern, text) for pattern in patterns):
            return form
    return "unknown_wig_extension"


def classify_attributes(title: str) -> Dict[str, Any]:
    text = normalize(title)
    material = "unknown"
    if "cabello humano" in text or "human hair" in text:
        material = "human_hair"
    elif "sintetic" in text or "fibra" in text:
        material = "synthetic"

    texture = "unknown"
    for candidate, patterns in (
        ("braided", (r"trenz", r"braid")),
        ("curly", (r"rizado", r"crespo", r"curly")),
        ("wavy", (r"ondulad", r"wavy")),
        ("straight", (r"liso", r"straight", r"yaki")),
    ):
        if any(re.search(pattern, text) for pattern in patterns):
            texture = candidate
            break

    lengths = [int(x) for x in re.findall(r"\b(\d{2,3})\s*(?:cm|centimetro)", text)]
    inches = [int(x) for x in re.findall(r"\b(\d{2})[- ]?(?:inch|pulgad)", text)]
    length_cm = max(lengths + [round(i * 2.54) for i in inches], default=0)
    if length_cm >= 60:
        length_band = "extra_long"
    elif length_cm >= 40:
        length_band = "long"
    elif length_cm >= 20:
        length_band = "medium"
    elif length_cm:
        length_band = "short"
    else:
        length_band = "unknown"

    form = classify_form(title)
    install_method = {
        "full_wig": "full_cap",
        "lace_wig": "lace",
        "clip_in_extension": "clip",
        "ponytail_extension": "drawstring",
        "braiding_hair": "crochet",
        "hairpiece_extension": "halo",
    }.get(form, "unknown")
    is_set = bool(re.search(r"\b[2-9]\s*(?:piezas|extensiones|pcs?)\b", text))
    return {
        "hair_material": material,
        "texture": texture,
        "length_band": length_band,
        "length_cm_hint": length_cm or None,
        "install_method": install_method,
        "product_pack_type": "bulk_or_assorted_set" if is_set else "single_or_unspecified",
        "heat_resistant": True if re.search(r"resistente.*(?:calor|temperatura)|herramientas? de calor|fibra termica", text) else None,
        "with_bangs": True if re.search(r"flequillo|bangs?", text) else None,
    }


INVALID_RULES: Sequence[Tuple[str, str]] = (
    ("variant_only", r"^item\s*:\s*.+$"),
    ("suspected_wrong_product", r"\blentes?\b"),
)

INVALID_PRODUCT_TITLE_RULES: Sequence[Tuple[str, str]] = (
    ("non_core_hair_styling_product", r"hair styling (?:wax|creme|cream|stick)|flyaway.*frizz"),
    ("non_core_hair_tinsel_accessory", r"\btinsel\b|cabello con brillos|mechas? de brillo"),
    ("non_core_wig_hat_or_cap", r"gorro.*peluca|wig cap|skull cap|gorro de calavera"),
    ("non_core_wig_tool", r"model head stand|wig rack|mannequin|tripod.*wig"),
)

POSITIVE_PATTERNS = (
    r"me encant", r"hermos", r"bonit", r"lind", r"excelente", r"super padre",
    r"buena calidad", r"10/10", r"recomiend", r"perfect", r"muy bien", r"genial",
    r"se ve[n]? natural", r"no (?:son )?brillos", r"casi no brilla", r"no se enred", r"no se enrred",
)
NEGATIVE_PATTERNS = (
    r"no es de muy buen", r"(?<!no )(?<!ni )se enred", r"(?<!no )(?<!ni )se enrred",
    r"(?<!no )(?<!casi no )brilla", r"brillosit",
    r"se ve algo sintet", r"no le atine", r"no saber poner", r"se vea falsa",
    r"solo.*un uso", r"bulto", r"fleco.*largo", r"no tan grande",
)


SIGNAL_RULES: Sequence[Tuple[str, Sequence[str]]] = (
    ("tangle_shedding_issue", (r"(?<!no )(?<!ni )se enred", r"(?<!no )(?<!ni )se enrred", r"se cae el cabello", r"suelta.*cabello")),
    ("synthetic_shine_issue", (r"(?<!no )(?<!casi no )brilla", r"brillosit", r"se ve algo sintet", r"parece sintet")),
    ("fit_concealment_issue", (r"bulto.*cabello real", r"se note.*cabello real", r"se note.*raiz")),
    ("length_fit_issue", (r"fleco.*largo", r"corte.*fleco", r"cortina no tan grande", r"estorba.*cara", r"corte.*arreglar")),
    ("color_match_issue", (r"no le atine.*tono", r"tono.*no.*coincid", r"mas claro.*(?:foto|muestra)")),
    ("installation_difficulty_issue", (r"no saber poner", r"no descubro como", r"miedo.*poner", r"para que no se vea falsa")),
    ("quality_durability_issue", (r"no es de muy buen material", r"solo.*un uso", r"una sola puesta")),
    ("natural_look", (r"se ve super real", r"pare[cs]e cabello natural", r"pare[cs]e pelo natural", r"parece cabello real", r"parece cabello humano", r"parece(?: que es)? mi raiz", r"se ve muy natural", r"se ven muy naturales", r"se siente super natural", r"rizos naturales", r"se disimula.*perfeccion", r"ni se notan", r"no se notan", r"ni se nota", r"no pare[cs]e sintet", r"no es sintet", r"no se ven sintet", r"no parece peluc", r"no se ve falsa", r"no se nota.*falsa", r"no (?:son )?brillos", r"casi no brilla")),
    ("appearance_style", (r"hermos", r"bonit", r"lind", r"preciosa", r"increible", r"padrisim", r"super padre", r"se ve bien", r"esta hermosa")),
    ("color_match", (r"tono.*lind", r"buen tono", r"mismo tono", r"color excelente", r"color muy vivo", r"otros tonos", r"color.*tal cual", r"color tal y como", r"me encanta el color")),
    ("texture_softness", (r"super suave", r"muy suave", r"se siente suave", r"textura se siente genial", r"textura luce muy bien")),
    ("volume_length", (r"super larg", r"muy larg", r"lo larga", r"extra largo", r"largo.*perfect", r"buen volumen", r"mayor volumen", r"demasiado volumen", r"trae.*volumen", r"mucho pelo", r"abundant", r"tamano (?:que dicen|justo|gusto|como)")),
    ("easy_install", (r"facil de poner", r"faciles de poner", r"practica de poner", r"practicas y faciles", r"facil de trenz")),
    ("hold_stability", (r"no se mueve", r"se mantienen firmes", r"mantiene firme", r"mejor agarre", r"clips funcionan", r"calidad en los broches")),
    ("comfort_weight", (r"no se sienten? tan pesad", r"nada pesad", r"liger", r"comod")),
    ("styleability", (r"puedes? planchar", r"se puede planchar", r"se puede cepillar", r"cepillar", r"facil de peinar", r"se maneja", r"al peinar", r"bien acomodad.*peinad")),
    ("tangle_resistance", (r"no se enred", r"no se enrred", r"no se les cae", r"no se cae", r"no se rompe ni se cae")),
    ("quality_durability", (r"buena calidad", r"excelente material", r"calidad 10/10", r"calidad.*excelente", r"calidad bien", r"calida.*bien", r"super buena", r"muy buena calidad", r"calidad del producto: 10 de 10")),
    ("value_price", (r"por el precio", r"buen precio", r"bajo costo", r"economicas?", r"calidad y precio", r"precio bien", r"super barata", r"no caras")),
    ("fast_shipping_mx", (r"antes de lo estimado", r"llego en tiempo", r"no tardo mucho", r"tiempo de llegada", r"fecha estipulada", r"llegan excelente", r"entrega en tiempo", r"llega rapido")),
    ("repeat_purchase", (r"volver[ei].*(?:compr|ped|encarg)", r"lo volveria", r"pedido dos veces", r"segunda que compro", r"me voy a pedir otras", r"la recomiendo", r"las recomiendo", r"recomendable", r"recomiendo mucho")),
)


def matching_tags(text: str) -> Tuple[List[str], List[Dict[str, str]]]:
    normalized = normalize(text)
    tags: List[str] = []
    segments: List[Dict[str, str]] = []
    for tag, patterns in SIGNAL_RULES:
        match: Optional[re.Match[str]] = None
        for pattern in patterns:
            match = re.search(pattern, normalized)
            if match:
                break
        if not match:
            continue
        if tag == "repeat_purchase" and re.search(
            r"\bno\s+(?:(?:la|las|lo|los)\s+)?recomiend|\bno\s+recomendable", normalized
        ):
            continue
        tags.append(tag)
        segments.append({
            "signal_tag": tag,
            "polarity": "negative" if tag.endswith("_issue") else "positive",
            "evidence_text": match.group(0),
        })
    return tags, segments


def overall_sentiment(text: str) -> str:
    normalized = normalize(text)
    negated_recommendation = bool(re.search(
        r"\bno\s+(?:(?:la|las|lo|los)\s+)?recomiend|\bno\s+recomendable", normalized
    ))
    positive_source = re.sub(
        r"\bno\s+(?:(?:la|las|lo|los)\s+)?recomiend\w*|\bno\s+recomendable", "", normalized
    )
    positive = any(re.search(p, positive_source) for p in POSITIVE_PATTERNS)
    negative = negated_recommendation or any(re.search(p, normalized) for p in NEGATIVE_PATTERNS)
    if positive and negative:
        return "mixed"
    if negative:
        return "negative"
    if positive:
        return "positive"
    return "neutral"


def enrich_row(row: Dict[str, Any], taxonomy: Dict[str, Any]) -> Dict[str, Any]:
    voc_text = str(row.get("voc_text") or "")
    normalized = normalize(voc_text)
    title = str(row.get("product_title") or "")
    normalized_title = normalize(title)
    invalid_reason = ""
    for reason, pattern in INVALID_PRODUCT_TITLE_RULES:
        if re.search(pattern, normalized_title):
            invalid_reason = reason
            break
    for reason, pattern in INVALID_RULES:
        if invalid_reason:
            break
        if re.search(pattern, normalized):
            invalid_reason = reason
            break
    tags, segments = matching_tags(voc_text) if not invalid_reason else ([], [])
    form = classify_form(title)
    attributes = classify_attributes(title)
    signal_labels = taxonomy.get("signals") or {}
    zh_topics = [signal_labels.get(tag, {}).get("label_zh", tag) for tag in tags]
    evidence_id = "{}:{}:{:03d}".format(
        row.get("batch_id") or "", row.get("fastmoss_product_id") or "", int(row.get("voc_rank") or 0)
    )
    return {
        "evidence_id": evidence_id,
        "batch_id": row.get("batch_id"),
        "market": row.get("market") or "MX",
        "category_key": row.get("category_key") or "wigs",
        "fastmoss_product_id": str(row.get("fastmoss_product_id") or ""),
        "voc_rank": int(row.get("voc_rank") or 0),
        "pool_type": row.get("pool_type") or "",
        "product_title": title,
        "voc_text": voc_text,
        "normalized_voc_text": normalized,
        "is_valid_voc": not bool(invalid_reason),
        "invalid_reason": invalid_reason,
        "language": "es-MX",
        "sentiment": "neutral" if invalid_reason else overall_sentiment(voc_text),
        "product_form": form,
        "product_form_label": (taxonomy.get("product_forms") or {}).get(form, form),
        "product_pack_type": attributes["product_pack_type"],
        "product_attributes": attributes,
        "signal_tags": tags,
        "attribute_segments": segments,
        "translation_zh_hint": "；".join(zh_topics),
    }


def fetch_rows(batch_id: str, database_url: Optional[str] = None) -> List[Dict[str, Any]]:
    DB = load_runner_module().DB
    db = DB.connect(database_url)
    try:
        cur = db.cursor()
        cur.execute(
            "SELECT r.batch_id, r.fastmoss_product_id, r.voc_rank, r.voc_text, r.pool_type, "
            "s.market, s.category_key, s.product_title "
            "FROM fastmoss_voc_raw r JOIN fastmoss_voc_product_snapshot s "
            "ON s.batch_id=r.batch_id AND s.fastmoss_product_id=r.fastmoss_product_id "
            "WHERE r.batch_id=%s ORDER BY r.fastmoss_product_id, r.voc_rank",
            (batch_id,),
        )
        return list(cur.fetchall())
    finally:
        db.close()


def fetch_batch_quality(batch_id: str, database_url: Optional[str] = None) -> Dict[str, Any]:
    DB = load_runner_module().DB
    db = DB.connect(database_url)
    try:
        cur = db.cursor()
        cur.execute("SELECT * FROM fastmoss_voc_export_batch WHERE batch_id=%s", (batch_id,))
        export = cur.fetchone() or {}
        for key in ("manifest_json", "quality_notes_json", "run_report_json"):
            value = export.get(key)
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("utf-8", "replace")
            if isinstance(value, str):
                try:
                    export[key] = json.loads(value)
                except json.JSONDecodeError:
                    export[key] = None
        cur.execute(
            "SELECT COUNT(*) AS raw_voc_count, COUNT(DISTINCT fastmoss_product_id) AS voc_product_count, "
            "COUNT(DISTINCT CASE WHEN pool_type='classic' THEN fastmoss_product_id END) AS classic_voc_products, "
            "COUNT(DISTINCT CASE WHEN pool_type='new' THEN fastmoss_product_id END) AS new_voc_products, "
            "COUNT(DISTINCT COALESCE(NULLIF(voc_text_hash,''), normalized_voc_text, voc_text)) AS distinct_voc_count, "
            "SUM(CASE WHEN voc_text IS NULL OR TRIM(voc_text)='' THEN 1 ELSE 0 END) AS blank_voc_count "
            "FROM fastmoss_voc_raw WHERE batch_id=%s",
            (batch_id,),
        )
        raw = cur.fetchone() or {}

        def json_safe(value: Any) -> Any:
            if isinstance(value, dict):
                return {str(key): json_safe(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [json_safe(item) for item in value]
            if hasattr(value, "as_integer_ratio") and value.__class__.__name__ == "Decimal":
                return int(value) if value == value.to_integral_value() else float(value)
            if isinstance(value, (dt.datetime, dt.date)):
                return value.isoformat()
            return value

        return json_safe({"export_batch": export, "raw_quality": raw})
    finally:
        db.close()


def build_draft_insights(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    runner = load_runner_module()
    valid = [r for r in records if r["is_valid_voc"]]
    form_to_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in valid:
        form_to_rows[record["product_form"]].append({
            "evidence_id": record["evidence_id"],
            "fastmoss_product_id": record["fastmoss_product_id"],
            "product_form": record["product_form"],
            "product_form_label": record["product_form_label"],
            "product_pack_type": record["product_pack_type"],
            "pool_type": record["pool_type"],
            "sentiment": record["sentiment"],
            "signal_tags": record["signal_tags"],
            "translation_zh_hint": record["translation_zh_hint"],
            "voc_text": record["voc_text"],
            "source_url": "",
        })
    classic_only = bool(valid) and all(r.get("pool_type") == "classic" for r in valid)
    confidences: Dict[str, str] = {}
    form_artifacts: List[Dict[str, Any]] = []
    for form, rows in sorted(form_to_rows.items()):
        product_count = len({r["fastmoss_product_id"] for r in rows})
        voc_count = len(rows)
        confidence = runner.form_confidence(product_count, voc_count)
        confidences[form] = confidence
        insights = runner.generate_form_insights(
            rows, form, rows[0]["product_form_label"], confidence, classic_only
        )
        form_artifacts.append({
            "product_form": form,
            "product_count": product_count,
            "voc_count": voc_count,
            "confidence_level": confidence,
            "insights": insights,
        })
    category_insights, category_summary = runner.generate_category_insights(
        form_to_rows, confidences, "wigs", classic_only
    )
    category_confidence = runner.FORM_OBSERVE_ONLY
    for candidate in (runner.CATEGORY_ADS_CANDIDATE, runner.CATEGORY_CANDIDATE, runner.FORM_PARTIAL):
        if any(insight["confidence"] == candidate for insight in category_insights):
            category_confidence = candidate
            break
    return {
        "classic_only": classic_only,
        "form_artifacts": form_artifacts,
        "category_artifact": {
            "confidence_level": category_confidence,
            "covered_forms": category_summary["covered_forms"],
            "product_count": category_summary["total_products"],
            "voc_count": category_summary["total_voc"],
            "insights": category_insights,
        },
    }


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def persist_enrichment(batch_id: str, records: Sequence[Dict[str, Any]], result: Dict[str, Any],
                       taxonomy: Dict[str, Any], database_url: Optional[str] = None) -> Dict[str, Any]:
    """Publish only the evidence/form-summary layer; never publish ADS artifacts here."""
    if not result["gold_validation"]["passed"]:
        raise RuntimeError("gold quality gate failed; refusing to write enriched VOC")
    runner = load_runner_module()
    db = runner.DB.connect(database_url)
    valid = [record for record in records if record["is_valid_voc"]]
    timestamp = now_iso()
    export_version = "wigs_voc_taxonomy_v1"
    insight_pack_id = "{}__{}".format(batch_id, export_version)
    quality_status = "warning" if result["draft_insights"]["classic_only"] else "ok"
    quality_payload = {
        "status": quality_status,
        "gold_validation": result["gold_validation"],
        "summary": result["summary"],
        "category_confidence": result["draft_insights"]["category_artifact"]["confidence_level"],
        "ads_blocked": True,
        "ads_block_reason": "category evidence is observe_only and valid VOC is classic-only",
    }
    try:
        cur = db.cursor()
        cur.execute(
            "INSERT INTO fastmoss_voc_insight_pack "
            "(insight_pack_id,batch_id,export_version,source_export_version,market,category_key,source_dir,"
            "generated_at,source_quality_status,quality_json,product_anchor_card_json,flow_issue_count,"
            "raw_pack_json,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE generated_at=VALUES(generated_at),source_quality_status=VALUES(source_quality_status),"
            "quality_json=VALUES(quality_json),raw_pack_json=VALUES(raw_pack_json),updated_at=VALUES(updated_at)",
            (
                insight_pack_id, batch_id, export_version, "fastmoss_voc_raw", "MX", "wigs",
                "skills/voc-insight/scripts/enrich_wigs_voc.py", timestamp, quality_status,
                json.dumps(quality_payload, ensure_ascii=False), json.dumps({}, ensure_ascii=False), 0,
                json.dumps({"taxonomy": taxonomy, "summary": result["summary"]}, ensure_ascii=False),
                timestamp, timestamp,
            ),
        )
        enriched_written = 0
        for record in valid:
            enriched_voc_id = "{}__{}".format(insight_pack_id, record["evidence_id"])
            sentiment_score = {"positive": 90, "mixed": 55, "neutral": 50, "negative": 15}.get(record["sentiment"], 50)
            cur.execute(
                "INSERT INTO fastmoss_voc_enriched "
                "(enriched_voc_id,insight_pack_id,batch_id,evidence_id,fastmoss_product_id,source_url,pool_type,"
                "market,category_key,voc_rank,voc_text,clean_comment_text,semantic_text,primary_language,detected_script,"
                "language_mix_json,script_counts_json,sentiment,sentiment_score,sentiment_reasons_json,"
                "attribute_segments_json,is_attribute_only,signal_tags_json,translation_zh_hint,raw_enriched_json,"
                "created_at,updated_at,product_form,product_form_label,product_pack_type,product_style_tags_json,"
                "product_form_candidates_json) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE sentiment=VALUES(sentiment),sentiment_score=VALUES(sentiment_score),"
                "attribute_segments_json=VALUES(attribute_segments_json),signal_tags_json=VALUES(signal_tags_json),"
                "translation_zh_hint=VALUES(translation_zh_hint),raw_enriched_json=VALUES(raw_enriched_json),"
                "updated_at=VALUES(updated_at),product_form=VALUES(product_form),"
                "product_form_label=VALUES(product_form_label),product_pack_type=VALUES(product_pack_type),"
                "product_style_tags_json=VALUES(product_style_tags_json),"
                "product_form_candidates_json=VALUES(product_form_candidates_json)",
                (
                    enriched_voc_id, insight_pack_id, batch_id, record["evidence_id"], record["fastmoss_product_id"],
                    "", record["pool_type"], "MX", "wigs", record["voc_rank"], record["voc_text"],
                    record["normalized_voc_text"], record["normalized_voc_text"], "es-MX", "latin",
                    json.dumps({"es-MX": 1.0}, ensure_ascii=False), json.dumps({"latin": len(record["voc_text"])}, ensure_ascii=False),
                    record["sentiment"], sentiment_score, json.dumps(record["attribute_segments"], ensure_ascii=False),
                    json.dumps(record["attribute_segments"], ensure_ascii=False), 0,
                    json.dumps(record["signal_tags"], ensure_ascii=False), record["translation_zh_hint"],
                    json.dumps(record, ensure_ascii=False), timestamp, timestamp, record["product_form"],
                    record["product_form_label"], record["product_pack_type"],
                    json.dumps(record["product_attributes"], ensure_ascii=False),
                    json.dumps([{"product_form": record["product_form"], "confidence": 1.0}], ensure_ascii=False),
                ),
            )
            enriched_written += 1

        form_written = 0
        for artifact in result["draft_insights"]["form_artifacts"]:
            form = artifact["product_form"]
            form_records = [record for record in valid if record["product_form"] == form]
            product_ids = sorted({record["fastmoss_product_id"] for record in form_records})
            signal_counts = Counter(tag for record in form_records for tag in record["signal_tags"])
            sentiment_counts = Counter(record["sentiment"] for record in form_records)
            pack_counts = Counter(record["product_pack_type"] for record in form_records)
            style_counts = Counter(
                "{}:{}".format(key, value)
                for record in form_records for key, value in record["product_attributes"].items()
                if value not in (None, "unknown", False)
            )
            examples = []
            seen_products = set()
            for record in form_records:
                if record["fastmoss_product_id"] in seen_products:
                    continue
                seen_products.add(record["fastmoss_product_id"])
                examples.append({
                    "product_id": record["fastmoss_product_id"],
                    "product_title": record["product_title"],
                })
            summary_id = "{}__form__{}".format(insight_pack_id, form)
            raw_summary = {
                "product_form": form,
                "confidence": artifact["confidence_level"],
                "insight_count": len(artifact["insights"]),
                "source": "wigs_voc_taxonomy_v1",
            }
            notes = ["classic_only_evidence"] if result["draft_insights"]["classic_only"] else []
            cur.execute(
                "INSERT INTO fastmoss_voc_product_form_summary "
                "(form_summary_id,insight_pack_id,batch_id,product_form,product_form_label,product_count,voc_count,"
                "sentiment_counts_json,pack_type_counts_json,style_tag_counts_json,top_signal_tags_json,"
                "product_ids_json,product_examples_json,raw_summary_json,created_at,updated_at,"
                "products_with_voc_count,form_sample_status,form_sample_notes_json) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE product_count=VALUES(product_count),voc_count=VALUES(voc_count),"
                "sentiment_counts_json=VALUES(sentiment_counts_json),pack_type_counts_json=VALUES(pack_type_counts_json),"
                "style_tag_counts_json=VALUES(style_tag_counts_json),top_signal_tags_json=VALUES(top_signal_tags_json),"
                "product_ids_json=VALUES(product_ids_json),product_examples_json=VALUES(product_examples_json),"
                "raw_summary_json=VALUES(raw_summary_json),updated_at=VALUES(updated_at),"
                "products_with_voc_count=VALUES(products_with_voc_count),form_sample_status=VALUES(form_sample_status),"
                "form_sample_notes_json=VALUES(form_sample_notes_json)",
                (
                    summary_id, insight_pack_id, batch_id, form, form_records[0]["product_form_label"],
                    len(product_ids), len(form_records), json.dumps(dict(sentiment_counts), ensure_ascii=False),
                    json.dumps(dict(pack_counts), ensure_ascii=False), json.dumps(dict(style_counts), ensure_ascii=False),
                    json.dumps([{"tag": tag, "count": count} for tag, count in signal_counts.most_common()], ensure_ascii=False),
                    json.dumps(product_ids, ensure_ascii=False), json.dumps(examples, ensure_ascii=False),
                    json.dumps(raw_summary, ensure_ascii=False), timestamp, timestamp, len(product_ids),
                    artifact["confidence_level"], json.dumps(notes, ensure_ascii=False),
                ),
            )
            form_written += 1
        db.commit()
        return {
            "written": True,
            "insight_pack_id": insight_pack_id,
            "quality_status": quality_status,
            "enriched_rows_written": enriched_written,
            "form_summaries_written": form_written,
            "ads_published": False,
        }
    except Exception:
        db.conn.rollback()
        raise
    finally:
        db.close()


def summarize(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    valid = [r for r in records if r["is_valid_voc"]]
    invalid = [r for r in records if not r["is_valid_voc"]]
    return {
        "raw_voc_count": len(records),
        "valid_voc_count": len(valid),
        "invalid_voc_count": len(invalid),
        "products_with_raw_voc": len({r["fastmoss_product_id"] for r in records}),
        "products_with_valid_voc": len({r["fastmoss_product_id"] for r in valid}),
        "form_counts": dict(Counter(r["product_form"] for r in valid)),
        "form_product_counts": {
            form: len({r["fastmoss_product_id"] for r in valid if r["product_form"] == form})
            for form in sorted({r["product_form"] for r in valid})
        },
        "sentiment_counts": dict(Counter(r["sentiment"] for r in valid)),
        "signal_counts": dict(Counter(tag for r in valid for tag in r["signal_tags"])),
        "invalid_reason_counts": dict(Counter(r["invalid_reason"] for r in invalid)),
        "untagged_valid_count": sum(1 for r in valid if not r["signal_tags"]),
    }


def validate_gold(records: Sequence[Dict[str, Any]], gold_payload: Dict[str, Any], taxonomy: Dict[str, Any]) -> Dict[str, Any]:
    by_key = {(r["fastmoss_product_id"], r["voc_rank"]): r for r in records}
    samples = gold_payload.get("samples") or []
    form_correct = validity_correct = 0
    tp = fp = fn = 0
    missing: List[str] = []
    mismatches: List[Dict[str, Any]] = []
    risk_ids = {
        tag for tag, meta in (taxonomy.get("signals") or {}).items()
        if meta.get("insight_role") == "risk_guard"
    }
    risk_as_selling = 0
    for sample in samples:
        key = (str(sample["fastmoss_product_id"]), int(sample["voc_rank"]))
        actual = by_key.get(key)
        if not actual:
            missing.append("{}:{}".format(*key))
            continue
        expected_valid = bool(sample.get("is_valid_voc", True))
        validity_correct += int(actual["is_valid_voc"] == expected_valid)
        form_correct += int(actual["product_form"] == sample["product_form"])
        expected_tags = set(sample.get("signal_tags") or [])
        actual_tags = set(actual.get("signal_tags") or [])
        tp += len(expected_tags & actual_tags)
        fp += len(actual_tags - expected_tags)
        fn += len(expected_tags - actual_tags)
        expected_risks = expected_tags & risk_ids
        actual_risks = actual_tags & risk_ids
        risk_as_selling += len(expected_risks - actual_risks)
        if (actual["is_valid_voc"] != expected_valid or actual["product_form"] != sample["product_form"]
                or expected_tags != actual_tags):
            mismatches.append({
                "key": "{}:{}".format(*key),
                "expected": {"valid": expected_valid, "form": sample["product_form"], "tags": sorted(expected_tags)},
                "actual": {"valid": actual["is_valid_voc"], "form": actual["product_form"], "tags": sorted(actual_tags)},
            })
    evaluated = max(1, len(samples) - len(missing))
    precision = tp / (tp + fp) if tp + fp else 1.0
    recall = tp / (tp + fn) if tp + fn else 1.0
    targets = taxonomy.get("quality_targets") or {}
    metrics = {
        "sample_count": len(samples),
        "evaluated_count": evaluated,
        "form_accuracy": round(form_correct / evaluated, 4),
        "validity_accuracy": round(validity_correct / evaluated, 4),
        "signal_precision": round(precision, 4),
        "signal_recall": round(recall, 4),
        "signal_f1": round((2 * precision * recall / (precision + recall)) if precision + recall else 0.0, 4),
        "risk_as_selling_count": risk_as_selling,
    }
    passed = (
        metrics["form_accuracy"] >= float(targets.get("form_accuracy_min", 0.9))
        and metrics["validity_accuracy"] >= float(targets.get("validity_accuracy_min", 0.95))
        and metrics["signal_precision"] >= float(targets.get("signal_precision_min", 0.85))
        and metrics["signal_recall"] >= float(targets.get("signal_recall_min", 0.8))
        and risk_as_selling <= int(targets.get("risk_as_selling_max", 0))
        and not missing
    )
    return {"passed": passed, "metrics": metrics, "targets": targets, "missing": missing, "mismatches": mismatches}


def markdown_report(result: Dict[str, Any]) -> str:
    summary = result["summary"]
    validation = result["gold_validation"]
    metrics = validation["metrics"]
    draft = result["draft_insights"]
    lines = [
        "# MX 假发 VOC V1 验证报告",
        "",
        "- 批次：`{}`".format(result["batch_id"]),
        "- 运行模式：{}".format(
            "证据富化层已写入（未发布 ADS）" if result.get("written") else "dry-run（未写入正式 VOC 表）"
        ),
        "- 质量门：**{}**".format("通过" if validation["passed"] else "未通过"),
        "",
        "## 数据概况",
        "",
        "- 原始 VOC：{}".format(summary["raw_voc_count"]),
        "- 有效 VOC：{}".format(summary["valid_voc_count"]),
        "- 无效 VOC：{}".format(summary["invalid_voc_count"]),
        "- 有效但未命中主题：{}".format(summary["untagged_valid_count"]),
        "- 有效 VOC 商品：{}".format(summary["products_with_valid_voc"]),
        "",
        "## 金标指标",
        "",
        "- 形态准确率：{:.1%}".format(metrics["form_accuracy"]),
        "- 有效性准确率：{:.1%}".format(metrics["validity_accuracy"]),
        "- 标签精确率：{:.1%}".format(metrics["signal_precision"]),
        "- 标签召回率：{:.1%}".format(metrics["signal_recall"]),
        "- 标签 F1：{:.1%}".format(metrics["signal_f1"]),
        "- 风险误作卖点：{}".format(metrics["risk_as_selling_count"]),
        "",
        "## 形态分布",
        "",
    ]
    for form, count in sorted(summary["form_counts"].items(), key=lambda x: (-x[1], x[0])):
        lines.append("- `{}`：{} 条 / {} 个商品".format(form, count, summary["form_product_counts"].get(form, 0)))
    lines.extend(["", "## 洞察置信度", ""])
    for artifact in sorted(draft["form_artifacts"], key=lambda x: x["product_form"]):
        lines.append("- `{}`：{}（{} 商品 / {} VOC）".format(
            artifact["product_form"], artifact["confidence_level"],
            artifact["product_count"], artifact["voc_count"]
        ))
    lines.append("- 类目级：{}（当前不进入 ADS）".format(
        draft["category_artifact"]["confidence_level"]
    ))
    lines.extend(["", "## 信号分布", ""])
    for tag, count in sorted(summary["signal_counts"].items(), key=lambda x: (-x[1], x[0])):
        lines.append("- `{}`：{}".format(tag, count))
    lines.extend([
        "",
        "## 放行建议",
        "",
        "- 当前结果仅用于 taxonomy 和富化规则验证。",
        "- 只有质量门通过后，才允许进入 enriched/form-summary 预发布写入。",
        "- 即使质量门通过，本批仍以 classic VOC 为主，不直接放行类目级 ADS。",
    ])
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MX wigs VOC deterministic enrichment dry-run")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--taxonomy", default=str(TAXONOMY_PATH))
    parser.add_argument("--gold-sample", default=str(DEFAULT_GOLD_PATH))
    parser.add_argument("--output-dir", default=str(SKILL_DIR / "output"))
    parser.add_argument("--write-enriched", action="store_true", help="write evidence/form summaries only; never ADS")
    parser.add_argument("--pretty", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    taxonomy = load_json(Path(args.taxonomy))
    gold = load_json(Path(args.gold_sample))
    rows = fetch_rows(args.batch_id, args.database_url)
    records = [enrich_row(row, taxonomy) for row in rows]
    result = {
        "batch_id": args.batch_id,
        "market": taxonomy.get("market"),
        "category_key": taxonomy.get("category_key"),
        "taxonomy_version": taxonomy.get("version"),
        "dry_run": not args.write_enriched,
        "written": False,
        "summary": summarize(records),
        "gold_validation": validate_gold(records, gold, taxonomy),
        "draft_insights": build_draft_insights(records),
        "records": records,
    }
    if args.write_enriched:
        result["persistence"] = persist_enrichment(
            args.batch_id, records, result, taxonomy, args.database_url
        )
        result["written"] = True
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "{}_wigs_voc_dryrun.json".format(args.batch_id)
    report_path = output_dir / "{}_wigs_voc_validation.md".format(args.batch_id)
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(markdown_report(result), encoding="utf-8")
    result["artifacts"] = {"dryrun_json": str(json_path), "validation_report": str(report_path)}
    compact = {
        "batch_id": result["batch_id"],
        "market": result["market"],
        "category_key": result["category_key"],
        "taxonomy_version": result["taxonomy_version"],
        "dry_run": result["dry_run"],
        "written": result["written"],
        "summary": result["summary"],
        "gold_validation": result["gold_validation"],
        "draft_insight_summary": {
            "classic_only": result["draft_insights"]["classic_only"],
            "forms": [
                {
                    "product_form": item["product_form"],
                    "product_count": item["product_count"],
                    "voc_count": item["voc_count"],
                    "confidence_level": item["confidence_level"],
                    "insight_count": len(item["insights"]),
                }
                for item in result["draft_insights"]["form_artifacts"]
            ],
            "category_confidence": result["draft_insights"]["category_artifact"]["confidence_level"],
            "category_insight_count": len(result["draft_insights"]["category_artifact"]["insights"]),
        },
        "artifacts": result["artifacts"],
    }
    if result.get("persistence"):
        compact["persistence"] = result["persistence"]
    print(json.dumps(result if args.pretty else compact, ensure_ascii=False, indent=2))
    return 0 if result["gold_validation"]["passed"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
