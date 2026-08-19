from wig_success_replication.models import *


def mother_contract(mother_id="m", version=1, digest="h"):
    slot = Slot(
        slot_id="S1", segment_id="SEG1", time_start=0, time_end=5,
        source_content="展示造型并揭示", narrative_function="悬念和揭示", traffic_role="hook",
        lock_level="hard_lock", product_dependency="low", must_keep=["5秒前揭示"],
        allowed_mutations=["钩子表达"], forbidden_mutations=["延迟揭示"],
        same_product_rule="保留", cross_product_rule="保留功能", inference_confidence="high", evidence_type="observed",
    )
    return MotherContract(
        mother_id=mother_id, mother_version=version,
        source_integrity=SourceIntegrity(source_script_hash=digest),
        validation_evidence=ValidationEvidence(description="已验证", causality_limit="不能证明每个元素"),
        core_mechanism=CoreMechanism(summary="悬念反转证明", hook_engine="价格", reveal_engine="假发", proof_engine="动作", conversion_engine="利益"),
        segments=[Segment(segment_id="SEG1", duration_seconds=15, slots=[slot], end_frame_anchor="", next_segment_start_requirement="")],
        global_rules=GlobalRules(hard_locks=["首帧成品"], conditional_locks=[], free_variables=["服装"], forbidden_mutations=["编造卖点"]),
        asset_contract=AssetContract(face_reference_rule="只参考脸部", product_image_rule="产品图片定义头发", reference_video_rule="不上传参考视频", minimum_product_images=1),
        continuity_contract=ContinuityContract(cross_segment_requirements=[]),
        claim_contract=ClaimContract(allowed_claim_sources=["confirmed_product_fact", "human_input"], forbidden_claim_sources=["visual_imagination", "template_assumption"], source_script_claim_risks=[]),
        localization_contract=LocalizationContract(tone="自然口语", timing_risks=[]),
        variant_policy=VariantPolicy(), risks=[], human_summary="摘要",
    )


def mother_review(contract):
    return MotherReview(final_contract=contract, issues=[], human_summary=contract.human_summary)
