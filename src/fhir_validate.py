"""FHIR R4 검증 단계 (적재 1단계: 검증기를 주입받는다).

sourceVerification.validated=true 는 "백엔드가 실제로 FHIR 검증을 했다"는 약속이라,
검증기를 정하지 않으면 적재가 진행되지 않게 했다(NotConfiguredValidator).

- JavaValidatorCLI: HL7 공식 validator_cli.jar 실행 (Java 필요. DB 팀은 7.0.3 사용).
  Python Lambda에는 Java가 없으므로, 적재 전 별도 단계에서 돌리거나 컨테이너 이미지가 필요하다(팀 결정 사항).
- PrecomputedResult: 다른 단계에서 이미 검증한 결과를 넘겨받을 때.
"""
from __future__ import annotations

import json
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


@dataclass
class ValidationReport:
    ok: bool
    method: str
    errors: int = 0
    warnings: int = 0
    messages: list[str] = field(default_factory=list)  # 오류 메시지 일부 (의료 원문은 넣지 않음)


class Validator(Protocol):
    def validate(self, bundle_bytes: bytes, name: str) -> ValidationReport: ...


class ValidatorNotConfigured(Exception):
    pass


class NotConfiguredValidator:
    def validate(self, bundle_bytes, name):
        raise ValidatorNotConfigured("FHIR 검증기가 설정되지 않아 적재하지 않습니다 (sourceVerification을 만들 수 없음)")


@dataclass
class PrecomputedResult:
    """별도 단계에서 검증한 결과. 검증한 바이트의 sha256을 함께 받아, 다른 파일에 결과를 잘못 쓰지 않게 한다."""
    ok: bool
    validated_sha256: str
    method: str = "precomputed"

    def validate(self, bundle_bytes, name):
        import hashlib

        if hashlib.sha256(bundle_bytes).hexdigest() != self.validated_sha256:
            return ValidationReport(False, self.method, messages=[f"{name}: 검증한 파일과 적재할 파일이 다릅니다"])
        return ValidationReport(self.ok, self.method)


def parse_operation_outcome(outcome: dict) -> ValidationReport:
    """validator가 낸 OperationOutcome(또는 그 Bundle)에서 error/fatal 개수를 센다."""
    outcomes = [outcome]
    if outcome.get("resourceType") == "Bundle":
        outcomes = [e.get("resource", {}) for e in outcome.get("entry", [])]
    errors = warnings = 0
    messages: list[str] = []
    for oo in outcomes:
        for issue in oo.get("issue", []):
            severity = issue.get("severity")
            if severity in ("error", "fatal"):
                errors += 1
                if len(messages) < 20:
                    where = ",".join(issue.get("expression") or issue.get("location") or [])
                    messages.append(f"{severity} {issue.get('code', '')} {where}".strip())
            elif severity == "warning":
                warnings += 1
    return ValidationReport(errors == 0, "operation-outcome", errors, warnings, messages)


class JavaValidatorCLI:
    def __init__(self, jar_path: str | Path, java: str = "java", terminology: str = "n/a", timeout: int = 600):
        self._jar = str(jar_path)
        self._java = java
        self._tx = terminology
        self._timeout = timeout

    def validate(self, bundle_bytes, name):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "input.json"
            out = Path(tmp) / "outcome.json"
            src.write_bytes(bundle_bytes)
            proc = subprocess.run(
                [self._java, "-jar", self._jar, str(src), "-version", "4.0.1", "-tx", self._tx, "-output", str(out)],
                capture_output=True, timeout=self._timeout, check=False,
            )
            if not out.exists():
                return ValidationReport(False, "validator_cli", messages=[f"{name}: validator 결과 없음 (exit {proc.returncode})"])
            report = parse_operation_outcome(json.loads(out.read_text(encoding="utf-8")))
            report.method = "validator_cli"
            return report
