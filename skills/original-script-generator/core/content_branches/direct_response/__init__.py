"""Compatibility adapter for the existing original/direct-response pipeline."""
from __future__ import annotations

from dataclasses import dataclass

from ..contracts import DIRECT_RESPONSE, PublishPolicyContract


@dataclass(frozen=True)
class DirectResponseBranch:
    key: str = DIRECT_RESPONSE
    script_type: str = "原创脚本"

    def publish_policy(self) -> PublishPolicyContract:
        return PublishPolicyContract(
            branch_key=self.key,
            script_type=self.script_type,
            publish_purpose="带货",
            cart_policy="ALLOWED",
            product_id_transport="PUBLISHING_REQUIRED",
            shoppable_endpoint_allowed=True,
            policy_version="direct-response-publish-v1",
        )

    def runtime_db_name(self) -> str:
        return "original_script_generator.sqlite3"
