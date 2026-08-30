"""Organic, non-shoppable seeding branch."""
from __future__ import annotations

from dataclasses import dataclass

from ..contracts import PublishPolicyContract, SEEDING_ORGANIC


@dataclass(frozen=True)
class OrganicSeedingBranch:
    key: str = SEEDING_ORGANIC
    script_type: str = "种草脚本"

    def publish_policy(self) -> PublishPolicyContract:
        policy = PublishPolicyContract(
            branch_key=self.key,
            script_type=self.script_type,
            publish_purpose="种草",
            cart_policy="FORBIDDEN",
            product_id_transport="INTERNAL_ANALYTICS_ONLY",
            shoppable_endpoint_allowed=False,
            policy_version="organic-seeding-publish-v1-fail-closed",
        )
        policy.assert_safe_for_sync()
        return policy

    def runtime_db_name(self) -> str:
        return "organic_seeding_generator.sqlite3"


from .pipeline import OrganicSeedingPipeline  # noqa: E402

__all__ = ["OrganicSeedingBranch", "OrganicSeedingPipeline"]
