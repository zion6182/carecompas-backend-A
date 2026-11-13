"""FHIR Bundle → (검증) → S3 원문 저장 → DB 쓰기 Lambda 적재. 적재 1단계: AWS 없이 테스트 가능.

    report = ingest_patient(bundle_bytes, fhir_patient_id="A1", patient_key="A1",
                            validator=..., store=S3ObjectStore(...), writer=DbWriter(...))

순서와 멈추는 조건
1. 파싱 (fhir_parser) → 이 환자의 행만 고른다. Patient 행이 없으면 멈춤(DB가 받지 않음).
2. patientKey를 계정에 연결된 값으로 바꾼다(DB는 로그인 토큰의 연결 정보로 환자를 판단하므로 같아야 한다).
3. FHIR 검증 → 실패하면 멈춤. S3·DB는 건드리지 않는다.
4. S3: 이 환자 행이 가리키는 원문(+병원 등 공유 원문)을 올리고 다시 읽어 해시 확인 → 하나라도 실패하면 멈춤.
   이미 다른 내용의 파일이 있으면 덮어쓰지 않고 멈춘다(allow_overwrite=True일 때만 덮어씀).
5. 3·4가 모두 통과해야 sourceVerification을 만든다.
6. get_write_state → 50행 단위로 upsert_records. 각 요청의 새 stateVersion을 다음 요청에 쓴다.
dry_run=True면 3까지만 하고(S3는 읽기만) 보낼 내용을 보고한다.
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import Any

from db_writer import DbWriter, source_verification
from fhir_parser import DEFAULT_SOURCE, parse_bundle, upsert_batches
from fhir_validate import ValidationReport, Validator
from s3_store import S3ObjectStore, UploadReport


class IngestError(Exception):
    def __init__(self, step: str, message: str):
        super().__init__(f"[{step}] {message}")
        self.step = step


@dataclass
class IngestReport:
    patient_key: str
    dry_run: bool
    records: int = 0
    followups: int = 0
    batches: list[dict] = field(default_factory=list)      # 요청별 결과(requestId, stateVersion, created/updated/unchanged)
    parser_errors: list[dict] = field(default_factory=list)  # 변환하지 않은 리소스 (errors 그대로)
    validation: ValidationReport | None = None
    s3_objects: int = 0
    s3: UploadReport | None = None
    s3_states: dict[str, str] = field(default_factory=dict)  # dry_run 때 S3 상태(missing/same/different)
    state_version_before: int | None = None
    state_version_after: int | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "patientKey": self.patient_key, "dryRun": self.dry_run,
            "records": self.records, "followups": self.followups, "s3Objects": self.s3_objects,
            "batches": len(self.batches), "parserErrors": len(self.parser_errors),
            "validationOk": self.validation.ok if self.validation else None,
            "s3": ({"uploaded": len(self.s3.uploaded), "unchanged": len(self.s3.unchanged),
                    "overwritten": len(self.s3.overwritten)} if self.s3 else None),
            "stateVersion": [self.state_version_before, self.state_version_after],
        }


def _select_patient(result, fhir_patient_id: str, patient_key: str) -> tuple[list[dict], list[dict]]:
    records = [copy.deepcopy(r) for r in result.records if r["patientKey"] == fhir_patient_id]
    followups = [copy.deepcopy(f) for f in result.followups if f["patientKey"] == fhir_patient_id]
    if not any(r["resourceType"] == "Patient" for r in records):
        raise IngestError("parse", f"번들에 Patient/{fhir_patient_id} 행이 없어 적재할 수 없습니다")
    for row in records + followups:
        row["patientKey"] = patient_key
    return records, followups


def _objects_for(records: list[dict], all_objects: dict[str, bytes]) -> dict[str, bytes]:
    keys = set()
    for r in records:
        keys.add(r["s3Key"])
        keys.update(s["s3Key"] for s in r["sharedReferences"])
    missing = keys - set(all_objects)
    if missing:
        raise IngestError("s3", f"원문 바이트가 없는 key: {sorted(missing)[:3]}")
    return {k: all_objects[k] for k in keys}


def ingest_patient(
    bundle_bytes: bytes,
    *,
    fhir_patient_id: str,
    patient_key: str,
    validator: Validator,
    store: S3ObjectStore | None,
    writer: DbWriter | None,
    source: str = DEFAULT_SOURCE,
    encounter_info: dict[str, dict] | None = None,
    is_synthetic: bool = True,
    bundle_s3_key: str | None = None,
    allow_overwrite: bool = False,
    dry_run: bool = False,
    batch_size: int = 50,
) -> IngestReport:
    report = IngestReport(patient_key=patient_key, dry_run=dry_run)

    # 1~2. 파싱, 이 환자만, patientKey 교체
    try:
        bundle = json.loads(bundle_bytes.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IngestError("parse", f"Bundle JSON을 읽을 수 없습니다: {type(exc).__name__}") from None
    result = parse_bundle(bundle, source, encounter_info=encounter_info, is_synthetic=is_synthetic,
                          bundle_s3_key=bundle_s3_key)
    report.parser_errors = result.errors
    records, followups = _select_patient(result, fhir_patient_id, patient_key)
    report.records, report.followups = len(records), len(followups)
    objects = _objects_for(records, result.object_bytes())
    report.s3_objects = len(objects)
    batches = upsert_batches(records, followups, patient_key, size=batch_size)

    # 3. FHIR 검증
    report.validation = validator.validate(bundle_bytes, f"Patient/{fhir_patient_id}")
    if not report.validation.ok:
        raise IngestError("validate", f"FHIR 검증 실패 (오류 {report.validation.errors}건): {report.validation.messages[:3]}")

    if dry_run:
        if store is not None:
            report.s3_states = store.check(objects)
        report.batches = [{"records": len(b["records"]), "followups": len(b["followups"])} for b in batches]
        return report

    if store is None or writer is None:
        raise IngestError("config", "실제 적재에는 S3 저장소와 DB writer가 모두 필요합니다")

    # 4. S3 업로드 + 다시 읽어 해시 확인 (실패 시 예외 → DB로 넘어가지 않음)
    report.s3 = store.put_verified(objects, allow_overwrite=allow_overwrite)
    if set(report.s3.verified) != set(objects):
        raise IngestError("s3", "확인되지 않은 원문이 있어 DB 적재를 하지 않습니다")

    # 5. attestation
    verification = source_verification(validated=True, objects_verified=True)

    # 6. DB 적재
    state = writer.get_write_state()
    expected = int(state.get("stateVersion", 0))
    report.state_version_before = expected
    for batch in batches:
        res = writer.upsert(patient_key, batch["records"], batch["followups"], verification, expected)
        expected = res.state_version
        report.batches.append({
            "requestId": res.request_id, "stateVersion": res.state_version, "attempts": res.attempts,
            "conflictsResolved": res.conflicts_resolved,
            "records": res.data.get("records"), "followups": res.data.get("followups"),
            "idempotentReplay": res.data.get("idempotentReplay"),
        })
    report.state_version_after = expected
    return report
