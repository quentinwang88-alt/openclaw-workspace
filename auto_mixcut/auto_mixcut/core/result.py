from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class MixcutError:
    code: str
    message: str
    detail: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "message": self.message, "detail": self.detail}


@dataclass
class Result:
    success: bool
    error: Optional[MixcutError] = None
    data: Optional[Any] = None

    @classmethod
    def ok(cls, data: Optional[Any] = None) -> "Result":
        return cls(success=True, error=None, data=data if data is not None else {})

    @classmethod
    def fail(cls, code: str, message: str, detail: Optional[Dict[str, Any]] = None) -> "Result":
        return cls(success=False, error=MixcutError(code=code, message=message, detail=detail or {}), data=None)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "error": None if self.error is None else self.error.to_dict(),
            "data": self.data,
        }
