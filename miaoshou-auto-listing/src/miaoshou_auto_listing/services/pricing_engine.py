from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from typing import Any, Dict


class PricingEngine:
    def __init__(self, rules: Dict[str, Dict[str, Any]]) -> None:
        self.rules = rules

    def calculate(self, purchase_cost: Decimal, rule_id: str) -> Decimal:
        rule = self.rules[rule_id]
        multiplier = Decimal(str(rule.get("purchase_cost_multiplier", 1)))
        fixed_cost = Decimal(str(rule.get("fixed_cost", 0)))
        minimum = Decimal(str(rule.get("min_price", 0)))
        raw = max(purchase_cost * multiplier + fixed_cost, minimum)
        rounding = str(rule.get("rounding", "CENT")).upper()
        if rounding == "X9":
            # Smallest number ending in 9 that is >= raw (99 -> 99, 110 -> 119).
            base = ((raw + Decimal("1")) / Decimal("10")).to_integral_value(
                rounding=ROUND_CEILING
            )
            return base * Decimal("10") - Decimal("1")
        if rounding == "INTEGER":
            return raw.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return raw.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
