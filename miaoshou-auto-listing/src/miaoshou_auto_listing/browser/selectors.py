from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import yaml

from ..errors import SelectorNotFound


class SelectorRegistry:
    """Loads all DOM selectors from YAML and resolves them by stable priority."""

    def __init__(self, selector_dir: Path) -> None:
        self.selector_dir = selector_dir.resolve()
        self._selectors: Dict[str, List[Dict[str, Any]]] = {}
        for path in sorted(self.selector_dir.glob("*.yaml")):
            with path.open("r", encoding="utf-8") as handle:
                values = yaml.safe_load(handle) or {}
            for key, candidates in values.items():
                if key in self._selectors:
                    raise ValueError(f"Duplicate selector key {key!r} in {path}")
                if not isinstance(candidates, list) or not candidates:
                    raise ValueError(f"Selector {key!r} must contain candidates")
                self._selectors[key] = candidates

    @property
    def keys(self) -> Iterable[str]:
        return self._selectors.keys()

    def candidates(self, key: str, **params: str) -> List[Dict[str, Any]]:
        if key not in self._selectors:
            raise KeyError(f"Unknown selector registry key: {key}")
        rendered: List[Dict[str, Any]] = []
        for candidate in self._selectors[key]:
            rendered.append(
                {
                    field: value.format(**params) if isinstance(value, str) else value
                    for field, value in candidate.items()
                }
            )
        return rendered

    def build(self, root: Any, candidate: Mapping[str, Any]) -> Any:
        kind = candidate["kind"]
        exact = bool(candidate.get("exact", False))
        if kind == "testid":
            return root.get_by_test_id(candidate["value"])
        if kind == "id":
            return root.locator(f"#{candidate['value']}")
        if kind == "role":
            return root.get_by_role(
                candidate["role"], name=candidate.get("name"), exact=exact
            )
        if kind == "label":
            return root.get_by_label(candidate["value"], exact=exact)
        if kind == "text":
            return root.get_by_text(candidate["value"], exact=exact)
        if kind == "css":
            return root.locator(candidate["value"])
        if kind == "xpath":
            return root.locator(f"xpath={candidate['value']}")
        raise ValueError(f"Unsupported selector kind: {kind}")

    async def resolve(
        self,
        root: Any,
        key: str,
        *,
        visible: bool = True,
        **params: str,
    ) -> Any:
        for candidate in self.candidates(key, **params):
            locator = self.build(root, candidate)
            try:
                if await locator.count() == 0:
                    continue
                first = locator.first
                if not visible or await first.is_visible():
                    return first
            except Exception:
                continue
        raise SelectorNotFound(key)

    async def resolve_all(self, root: Any, key: str, **params: str) -> Any:
        for candidate in self.candidates(key, **params):
            locator = self.build(root, candidate)
            try:
                if await locator.count() > 0:
                    return locator
            except Exception:
                continue
        raise SelectorNotFound(key)

    async def exists(self, root: Any, key: str, **params: str) -> bool:
        try:
            await self.resolve(root, key, **params)
            return True
        except SelectorNotFound:
            return False
