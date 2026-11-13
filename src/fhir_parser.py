"""FHIR R4 Bundle → CareCompass 조회 모델 schemaVersion 2 변환 (AWS 의존성 없음).

기준: DB 팀 DB Lambda 전달본 1.1 (2026-11-03) SPECIFICATION.md §5.2 Record / §5.3 Followup query model.
출력은 쓰기 Lambda `upsert_records`의 records / followups 배열에 그대로 넣을 수 있는 형태다.
DB 쪽 검증 코드(tests/db_contract/model_validation.py)를 통과하는지 tests/ 에서 확인한다.

규칙
- Records: 환자 소유 리소스(Patient, Encounter, Condition, Observation, DiagnosticReport,
  MedicationRequest, Appointment, CarePlan) 1건당 1행. Organization·Location·CodeSystem은 행을 만들지 않고
  각 환자 행의 sharedReferences에 위치만 남긴다(원문은 S3).
- 날짜: 원래 값(sourceDateValue)과 정밀도를 보존한다. 시각+시간대 → DATETIME(UTC 'Z'),
  날짜만 → DATE, 연·연월만 → UNKNOWN(YEAR/MONTH 원문 보존), 없음 → UNKNOWN. 날짜를 지어내지 않는다.
  시간대가 없는 일시는 시간대를 추정하지 않고 DATE로 낮춘다.
- 참조: 번들에서 찾은 같은 환자 리소스만 linkedResourceKeys에 넣고 근거(linkEvidence)를 남긴다.
  번들에서 찾을 수 없는 참조는 unresolvedReferences에 넣고 linkageStatus=PARTIAL.
  다른 환자의 리소스는 연결하지 않는다. 조건부 참조('?' 포함)는 번들 밖을 가리키므로 무시한다.
- 진료과: 외부 매핑(encounter_timeline.json)에서만 받는다(DEMO_MAPPING + 근거 객체). 없으면 UNKNOWN.
  Encounter.class(외래·입원)는 진료과로 쓰지 않는다.
- sourceSha256: serialize_resource()로 만든 바이트의 SHA-256. S3에는 반드시 이 바이트를 그대로 올려야 한다.
- 같은 번들을 다시 파싱하면 같은 결과가 나오고, resourceKey / followupKey로 중복을 제거할 수 있다.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlsplit

from specialty_map import UNKNOWN

SCHEMA_VERSION = 2
DEFAULT_SOURCE = "carecompass-demo-v1"
CLINIC_KEY_SYSTEM = "https://carecompass.example/identifier/hira-clinic-key"
SPECIALTY_MAPPING_VERSION = "carecompass-demo-specialty-v1"
S3_PREFIX_SYNTHETIC = "carecompass/v1/data/synthetic/fhir/"
S3_PREFIX_REAL = "carecompass/v1/data/fhir/"

PATIENT_OWNED_TYPES = frozenset(
    {"Patient", "Encounter", "Observation", "DiagnosticReport", "Condition", "MedicationRequest", "Appointment", "CarePlan"}
)
SHARED_TYPES = frozenset({"Organization", "Location", "CodeSystem"})
MAX_LINKS = 200
MAX_SHARED = 100
MAX_EVIDENCE = 300

# Imported from the 2026-11-06 partner handoff; local ownership checks are stricter.
_NON_PATIENT_ACTOR_TYPES = frozenset({
    "Practitioner", "PractitionerRole", "RelatedPerson", "Device", "HealthcareService", "Location"
})


def _reference_resource_type(reference: dict) -> tuple[str | None, bool]:
    """Return the explicit/inferred target type and whether those hints conflict.

    URI inspection only classifies references; it never fetches a remote resource
    or treats an external Patient with the same id as a verified local patient.
    """
    declared = reference.get("type")
    literal = reference.get("reference")
    if declared is not None and (not isinstance(declared, str) or not declared):
        return None, True
    if literal is not None and not isinstance(literal, str):
        return None, True
    try:
        declared_type = urlsplit(declared).path.rstrip("/").rsplit("/", 1)[-1] if declared else None
        parsed = urlsplit(literal or "")
    except ValueError:
        return None, True
    match = re.search(
        r"(?:^|/)([A-Za-z][A-Za-z0-9]*)/[A-Za-z0-9.-]+(?:/_history/[A-Za-z0-9.-]+)?$",
        parsed.path,
    ) if parsed.scheme in ("", "http", "https") else None
    inferred_type = match.group(1) if match else None
    if inferred_type is None and parsed.query and parsed.scheme in ("", "http", "https"):
        # A conditional non-patient reference remains non-patient even when it
        # cannot be resolved inside this bundle.
        inferred_type = parsed.path.rstrip("/").rsplit("/", 1)[-1] or None
    conflict = bool(declared_type and inferred_type and declared_type != inferred_type)
    return (None if conflict else declared_type or inferred_type), conflict


# --------------------------------------------------------------------------- 식별자

def resource_ref(res: dict) -> str:
    """'Encounter/A1-01' 형태의 FHIR 상대 참조"""
    return f"{res['resourceType']}/{res['id']}"


def resource_key(source: str, ref: str) -> str:
    """resourceKey = <source>|<ResourceType>/<id>  (예: carecompass-demo-v1|Encounter/A1-01)"""
    return f"{source}|{ref}"


def s3_prefix(is_synthetic: bool) -> str:
    return S3_PREFIX_SYNTHETIC if is_synthetic else S3_PREFIX_REAL


def s3_key(res: dict, is_synthetic: bool = True) -> str:
    return f"{s3_prefix(is_synthetic)}resources/{res['resourceType']}-{res['id']}.json"


def serialize_resource(res: dict) -> bytes:
    """S3에 올릴 원문 바이트. sourceSha256은 이 바이트로 계산한다(업로드도 이 바이트 그대로).

    DB 팀 패키지(repro/build_package.py writej)와 같은 형식: 들여쓰기 2칸 JSON + 끝 줄바꿈,
    줄바꿈은 CRLF(Windows에서 생성), UTF-8(BOM 없음). 이 형식이어야 manifest.json의 sha256,
    즉 이미 S3에 올라간 원문 파일의 해시와 같아진다.
    """
    text = json.dumps(res, ensure_ascii=False, indent=2) + "\n"
    return text.replace("\n", "\r\n").encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encode_id(key: str) -> str:
    """API용 recordId: resourceKey를 base64url(패딩 제거)로 인코딩한다."""
    return base64.urlsafe_b64encode(key.encode("utf-8")).decode("ascii").rstrip("=")


def decode_id(token: str) -> str:
    return base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode("utf-8")


def followup_key(source_resource_key: str, source_element_key: str) -> str:
    return hashlib.sha256(f"{source_resource_key}\n{source_element_key}".encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- 날짜

_DATETIME_RE = re.compile(
    r"^(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<fraction>\.\d+)?"
    r"(?P<zone>Z|[+-](?:(?:0\d|1[0-3]):[0-5]\d|14:00))?$"
)


def _unknown_date() -> dict:
    return {"occurredAt": None, "datePrecision": "UNKNOWN", "sourceDateValue": None, "sourceDatePrecision": "UNKNOWN"}


def date_model(value: Any) -> dict:
    """FHIR date / dateTime / instant → DB v2 날짜 4필드.

    반환: {occurredAt, datePrecision, sourceDateValue, sourceDatePrecision}
    - '2025-01-16T09:00:00+09:00' → occurredAt '2025-01-16T00:00:00Z', DATETIME (소수 초는 그대로 보존)
    - '2025-01-16'                → occurredAt '2025-01-16', DATE
    - '2025-01' / '2025'          → occurredAt None, UNKNOWN, 원문은 sourceDateValue에 MONTH / YEAR로 보존
    - 시간대 없는 일시('2025-01-16T09:00:00') → 시간대를 추정하지 않고 DATE('2025-01-16')로 낮춘다
    - 없음·형식 오류                → 모두 UNKNOWN / None
    """
    if not isinstance(value, str) or not value:
        return _unknown_date()
    if "T" in value:
        m = _DATETIME_RE.match(value)
        if not m:
            return _unknown_date()
        if not m.group("zone"):
            return date_model(value[:10])
        zone = "+00:00" if m.group("zone") == "Z" else m.group("zone")
        try:
            base = datetime.fromisoformat(m.group("base") + zone)
        except ValueError:  # 윤초(:60) 등 표현할 수 없는 값은 보정하지 않는다
            return _unknown_date()
        utc = base.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + (m.group("fraction") or "") + "Z"
        return {"occurredAt": utc, "datePrecision": "DATETIME", "sourceDateValue": value, "sourceDatePrecision": "DATETIME"}
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            return _unknown_date()
        return {"occurredAt": value, "datePrecision": "DATE", "sourceDateValue": value, "sourceDatePrecision": "DATE"}
    if re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value) and value[:4] != "0000":
        return {"occurredAt": None, "datePrecision": "UNKNOWN", "sourceDateValue": value, "sourceDatePrecision": "MONTH"}
    if re.fullmatch(r"\d{4}", value) and value != "0000":
        return {"occurredAt": None, "datePrecision": "UNKNOWN", "sourceDateValue": value, "sourceDatePrecision": "YEAR"}
    return _unknown_date()


def _occurred(res: dict) -> tuple[Any, str | None]:
    """리소스 종류별 '발생 시점' 원문 값과 그 경로. Patient·CarePlan은 발생 시점이 없다고 본다."""
    rtype = res["resourceType"]
    candidates: list[tuple[str, Any]] = {
        "Encounter": [("Encounter.period.start", (res.get("period") or {}).get("start"))],
        "Observation": [("Observation.effectiveDateTime", res.get("effectiveDateTime")),
                        ("Observation.effectivePeriod.start", (res.get("effectivePeriod") or {}).get("start")),
                        ("Observation.issued", res.get("issued"))],
        "DiagnosticReport": [("DiagnosticReport.effectiveDateTime", res.get("effectiveDateTime")),
                             ("DiagnosticReport.effectivePeriod.start", (res.get("effectivePeriod") or {}).get("start")),
                             ("DiagnosticReport.issued", res.get("issued"))],
        "Condition": [("Condition.onsetDateTime", res.get("onsetDateTime")),
                      ("Condition.recordedDate", res.get("recordedDate"))],
        "MedicationRequest": [("MedicationRequest.authoredOn", res.get("authoredOn"))],
        "Appointment": [("Appointment.start", res.get("start"))],
    }.get(rtype, [])
    for path, value in candidates:
        if value:
            return value, path
    return None, None


# --------------------------------------------------------------------------- 참조 해석

def _iter_references(node: Any, path: str) -> Iterator[tuple[str, str]]:
    """리소스 안의 모든 {"reference": "..."} → (참조 문자열, 경로). 경로 예: 'Encounter.diagnosis[0].condition'"""
    if isinstance(node, dict):
        ref = node.get("reference")
        if isinstance(ref, str):
            yield ref, path
        for key, value in node.items():
            if not (key == "reference" and isinstance(value, str)):  # CarePlan.activity.reference 는 Reference 객체
                yield from _iter_references(value, f"{path}.{key}")
    elif isinstance(node, list):
        for i, value in enumerate(node):
            yield from _iter_references(value, f"{path}[{i}]")


class _Index:
    """번들 안 참조 해석: 'Type/id'와 fullUrl(절대 URL, urn:uuid) 둘 다 지원한다."""

    def __init__(self, entries: list[dict]) -> None:
        self.resources: dict[str, dict] = {}  # 'Type/id' → resource (같은 내용의 중복은 하나로)
        self.full_url: dict[str, str] = {}    # 'Type/id' → entry.fullUrl
        self._by_url: dict[str, dict] = {}
        self.issues: list[dict] = []          # 모호해서 쓰지 않은 항목 (ParseResult.errors로 전달)
        self.conflicting_refs: set[str] = set()
        self._conflicting_urls: set[str] = set()
        for entry in entries:
            res = entry.get("resource")
            if not isinstance(res, dict) or "resourceType" not in res or "id" not in res:
                continue
            ref = resource_ref(res)
            if ref not in self.resources:
                self.resources[ref] = res
                if entry.get("fullUrl"):
                    self.full_url[ref] = entry["fullUrl"]
            elif self.resources[ref] != res and ref not in self.conflicting_refs:
                # 같은 Type/id인데 내용이 다르면 어느 쪽이 맞는지 알 수 없다 → 둘 다 쓰지 않는다
                self.conflicting_refs.add(ref)
                self.issues.append({"resource": ref, "code": "DUPLICATE_RESOURCE_ID",
                                    "error": "같은 Type/id에 서로 다른 내용이 있어 변환하지 않음"})
            url = entry.get("fullUrl")
            if url:
                if url in self._by_url and resource_ref(self._by_url[url]) != ref and url not in self._conflicting_urls:
                    # 같은 fullUrl이 서로 다른 리소스를 가리키면 그 주소로는 참조를 풀지 않는다
                    self._conflicting_urls.add(url)
                    self.issues.append({"resource": url, "code": "DUPLICATE_FULL_URL",
                                        "error": "같은 fullUrl이 서로 다른 리소스를 가리켜 이 주소의 참조를 풀지 않음"})
                self._by_url.setdefault(url, res)
        for ref in self.conflicting_refs:
            self.resources.pop(ref, None)

    def resolve(self, ref: str | None) -> dict | None:
        if not ref or "?" in ref:  # 조건부 참조는 번들 밖 리소스를 가리킨다
            return None
        if ref in self._conflicting_urls or ref in self.conflicting_refs:
            return None
        target = self._by_url.get(ref) or self.resources.get(ref)
        if target is not None and resource_ref(target) in self.conflicting_refs:
            return None
        return target

    def owner_resolution(self, res: dict) -> tuple[set[str], bool]:
        """Return resolved patients plus an unresolved ownership flag.

        A missing/conflicting Patient reference must never disappear from the
        ownership decision just because another patient is present in the bundle.
        Unresolved actors with a clear non-patient type do not establish ownership.
        """
        if res["resourceType"] == "Patient":
            identifier = res.get("id")
            return ({identifier}, False) if isinstance(identifier, str) and identifier else (set(), True)
        references: list[tuple[dict, bool]] = []
        for name in ("subject", "patient"):
            if isinstance(res.get(name), dict):
                references.append((res[name], True))
            elif res.get(name) is not None:
                references.append(({}, True))
        for part in res.get("participant") or []:
            if isinstance(part, dict) and isinstance(part.get("actor"), dict):
                references.append((part["actor"], False))
        found: set[str] = set()
        unresolved = False
        for reference, owner_field in references:
            target_type, conflict = _reference_resource_type(reference)
            if conflict:
                unresolved = True
                continue
            literal = reference.get("reference")
            target = self.resolve(literal) if isinstance(literal, str) else None
            if target is not None:
                actual_type = target["resourceType"]
                if target_type and target_type != actual_type:
                    unresolved = True
                elif actual_type == "Patient":
                    identifier = target.get("id")
                    if isinstance(identifier, str) and identifier:
                        found.add(identifier)
                    else:
                        unresolved = True
                elif owner_field:
                    unresolved = True
            elif owner_field or target_type not in _NON_PATIENT_ACTOR_TYPES:
                unresolved = True
        return found, unresolved

    def owner_candidates(self, res: dict) -> set[str]:
        """Resolved candidates; use owner_resolution/owner_of to decide ownership."""
        return self.owner_resolution(res)[0]

    def owner_of(self, res: dict) -> str | None:
        """리소스를 소유한 Patient id. 환자를 못 찾거나 2명 이상이면 None (어느 환자에게도 붙이지 않는다)."""
        found, unresolved = self.owner_resolution(res)
        return next(iter(found)) if len(found) == 1 and not unresolved else None


# --------------------------------------------------------------------------- 결과 타입

@dataclass
class ParseResult:
    records: list[dict] = field(default_factory=list)      # upsert_records.records (환자 소유 리소스 1건당 1행)
    followups: list[dict] = field(default_factory=list)    # upsert_records.followups
    resources: dict[str, dict] = field(default_factory=dict)  # s3Key → FHIR 리소스 원문 (번들의 모든 리소스)
    errors: list[dict] = field(default_factory=list)       # 변환 실패 기록 (해당 건만 빠지고 나머지는 계속)

    def object_bytes(self) -> dict[str, bytes]:
        """S3에 올릴 {s3Key: 바이트}. sourceSha256과 일치하는 바이트다."""
        return {key: serialize_resource(res) for key, res in self.resources.items()}

    def patients(self) -> list[str]:
        return sorted({r["patientKey"] for r in self.records})


# --------------------------------------------------------------------------- Records

# DB 쓰기 Lambda(model_validation.py)가 거부하는 값. 한 행이라도 거부되면 같은 요청(최대 50행) 전체가
# 저장되지 않으므로, 파서 단계에서 그 행만 빼고 errors에 남긴다.
_FHIR_ID_RE = re.compile(r"^[A-Za-z0-9.-]{1,64}$")
_SOURCE_RE = re.compile(r"^[^|\s/]{1,128}$")
_ALLOWED_STATUS = {
    "Encounter": {"planned", "arrived", "triaged", "in-progress", "onleave", "finished", "cancelled", "entered-in-error", "unknown"},
    "Observation": {"registered", "preliminary", "final", "amended", "corrected", "cancelled", "entered-in-error", "unknown"},
    "DiagnosticReport": {"registered", "partial", "preliminary", "final", "amended", "corrected", "appended", "cancelled",
                         "entered-in-error", "unknown"},
    "Condition": {"active", "recurrence", "relapse", "inactive", "remission", "resolved"},
    "MedicationRequest": {"active", "on-hold", "cancelled", "completed", "entered-in-error", "stopped", "draft", "unknown"},
    "Appointment": {"proposed", "pending", "booked", "arrived", "fulfilled", "cancelled", "noshow", "entered-in-error",
                    "checked-in", "waitlist"},
    "CarePlan": {"draft", "active", "on-hold", "revoked", "completed", "entered-in-error", "unknown"},
}


def _db_precheck(res: dict, owner: str, source: str) -> str | None:
    """DB가 거부할 값이면 이유, 아니면 None."""
    if not _SOURCE_RE.match(source):
        return f"sourceSystem 형식 오류: {source!r}"
    if not isinstance(res.get("id"), str) or not _FHIR_ID_RE.match(res["id"]):
        return f"FHIR id 형식 오류: {res.get('id')!r}"
    if not _FHIR_ID_RE.match(owner):
        return f"환자 id 형식 오류: {owner!r}"
    rtype = res["resourceType"]
    status = _fhir_status(res)
    if rtype == "Patient" or (rtype == "Condition" and status is None):
        return None
    if status not in _ALLOWED_STATUS[rtype]:
        return f"{rtype} 상태값이 FHIR R4 허용값이 아님: {status!r}"
    return None


def _fhir_status(res: dict) -> str | None:
    if res["resourceType"] == "Patient":
        return None
    if res["resourceType"] == "Condition":  # Condition은 status 대신 clinicalStatus
        for coding in (res.get("clinicalStatus") or {}).get("coding", []):
            if coding.get("code"):
                return coding["code"]
        return None
    return res.get("status")


def _codeable_text(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    if value.get("text"):
        return value["text"]
    for coding in value.get("coding") or []:
        if coding.get("display"):
            return coding["display"]
    return None


def _title(res: dict, info: dict) -> str | None:
    """화면 표시용 제목. Encounter는 DB가 필수로 요구한다."""
    rtype = res["resourceType"]
    if rtype == "Patient":
        return None
    if rtype == "Encounter":
        if info.get("summary"):
            return info["summary"]
        for value in (res.get("type") or []) + (res.get("reasonCode") or []):
            text = _codeable_text(value)
            if text:
                return text
        return "진료 방문"
    if rtype == "CarePlan":
        return res.get("title") or res.get("description")
    if rtype == "Appointment":
        return res.get("description") or _codeable_text((res.get("serviceType") or [None])[0])
    if rtype == "MedicationRequest":
        return _codeable_text(res.get("medicationCodeableConcept"))
    return _codeable_text(res.get("code"))


def _specialty(info: dict) -> dict:
    """외부 진료과 매핑 → DB v2 진료과 3필드. 근거가 없으면 UNKNOWN."""
    code = info.get("specialtyCode")
    source = info.get("specialtySource")
    if not code or code == UNKNOWN or not re.fullmatch(r"\d{2}", str(code)) or source not in ("DEMO_MAPPING", "SOURCE_MAPPING"):
        return {"specialtyCode": UNKNOWN, "specialtySource": UNKNOWN, "specialtyEvidence": None}
    description = info.get("specialtyEvidence")
    return {
        "specialtyCode": code,
        "specialtySource": source,
        "specialtyEvidence": {
            "path": info.get("evidencePath") or f"scenario.visits.{info.get('encounterId', 'unknown')}",
            "sourceValue": info.get("specialtyName"),
            "mappingVersion": SPECIALTY_MAPPING_VERSION,
            "description": description[:512] if isinstance(description, str) else None,
        },
    }


def _organization(res: dict, index: _Index, info: dict, source: str) -> dict:
    out = {"clinicKey": None, "organizationResourceKey": None, "organizationName": None, "organizationNameSource": UNKNOWN}
    if res["resourceType"] != "Encounter":
        return out
    org = index.resolve((res.get("serviceProvider") or {}).get("reference"))
    if org is not None and org["resourceType"] == "Organization":
        out["organizationResourceKey"] = resource_key(source, resource_ref(org))
        for ident in org.get("identifier", []):
            if ident.get("system") == CLINIC_KEY_SYSTEM and ident.get("value"):
                out["clinicKey"] = ident["value"]
        if org.get("name"):
            out["organizationName"], out["organizationNameSource"] = org["name"], "FHIR"
    out["clinicKey"] = out["clinicKey"] or info.get("clinicKey")
    if out["organizationName"] is None and info.get("clinicName"):
        out["organizationName"], out["organizationNameSource"] = info["clinicName"], "CLINIC_MAPPING"
    return out


def _build_record(
    res: dict,
    owner: str,
    owners: dict[str, str],
    children: dict[str, list[tuple[str, str, str]]],
    index: _Index,
    info_by_encounter: dict[str, dict],
    source: str,
    is_synthetic: bool,
    bundle_s3_key: str | None,
) -> dict:
    self_ref = resource_ref(res)
    rtype = res["resourceType"]

    linked: dict[str, None] = {}          # 순서 유지 set
    evidence: list[dict] = []
    shared: dict[str, dict] = {}
    unresolved: list[str] = []
    for ref, path in _iter_references(res, rtype):
        target = index.resolve(ref)
        if target is None:
            if "?" not in ref and ref not in unresolved:
                unresolved.append(ref)  # 번들에서 찾을 수 없는 참조 → 부분 연결
            continue
        target_ref = resource_ref(target)
        if target_ref == self_ref:
            continue
        if target["resourceType"] in SHARED_TYPES:
            key = resource_key(source, target_ref)
            shared.setdefault(key, {"resourceKey": key, "resourceType": target["resourceType"],
                                    "s3Key": s3_key(target, is_synthetic)})
        elif target["resourceType"] in PATIENT_OWNED_TYPES and owners.get(target_ref) == owner:
            # 다른 환자 리소스는 연결하지 않는다
            key = resource_key(source, target_ref)
            linked[key] = None
            evidence.append({"targetResourceKey": key, "reference": ref, "path": path})
    if rtype == "Encounter":  # 이 Encounter를 가리키는 같은 환자 리소스 (역방향)
        for child_ref, ref, path in children.get(self_ref, []):
            key = resource_key(source, child_ref)
            if key not in linked:
                linked[key] = None
            evidence.append({"targetResourceKey": key, "reference": ref, "path": f"{path} (reverse)"})

    if rtype == "Encounter":
        encounter_ref: str | None = self_ref
    else:
        enc = index.resolve((res.get("encounter") or {}).get("reference"))
        encounter_ref = resource_ref(enc) if enc is not None and owners.get(resource_ref(enc)) == owner else None
    info = info_by_encounter.get(encounter_ref or "", {})
    if info:
        info = {**info, "encounterId": (encounter_ref or "").split("/", 1)[-1]}

    raw_date, date_path = _occurred(res)
    dates = date_model(raw_date)
    serialized = serialize_resource(res)

    record = {
        "schemaVersion": SCHEMA_VERSION,
        "itemType": "RESOURCE",
        "resourceKey": resource_key(source, self_ref),
        "sourceSystem": source,
        "resourceType": rtype,
        "resourceId": res["id"],
        "patientKey": owner,
        "fhirStatus": _fhir_status(res),
        "ingestionStatus": "READY",
        "ingestionErrorCode": None,
        "s3Key": s3_key(res, is_synthetic),
        "sourceBundleS3Key": bundle_s3_key,
        "sourceEntryFullUrl": index.full_url.get(self_ref),
        "sourceSha256": sha256_hex(serialized),
        "sourceVersionId": (res.get("meta") or {}).get("versionId"),
        "isSynthetic": is_synthetic,
        "title": _title(res, info),
        **dates,
        "dateEvidencePath": date_path if dates["sourceDatePrecision"] != "UNKNOWN" else None,
        **_specialty(info),
        **_organization(res, index, info, source),
        "linkedResourceKeys": sorted(linked)[:MAX_LINKS],
        "sharedReferences": sorted(shared.values(), key=lambda s: s["resourceKey"])[:MAX_SHARED],
        "unresolvedReferences": unresolved[:MAX_LINKS],
        "linkageStatus": "PARTIAL" if unresolved else "RESOLVED",
        "linkEvidence": evidence[:MAX_EVIDENCE],
    }
    return record


# --------------------------------------------------------------------------- FollowUps

_INACTIVE_APPOINTMENT = frozenset({"cancelled", "noshow", "entered-in-error"})

def _build_followups(
    owned: list[tuple[dict, str]], records_by_ref: dict[str, dict], index: _Index, source: str, is_synthetic: bool
) -> list[dict]:
    """Appointment → 예약 일정, 일정 없는 활성 CarePlan → 날짜 미정 계획.

    - CarePlan.activity가 **같은 환자의 유효한(취소·노쇼·오입력이 아닌)** Appointment를 가리키면 그 CarePlan은
      예약 행의 relatedSourceResourceKeys에 넣고 별도 행을 만들지 않는다(같은 일정이 두 번 보이지 않게).
      다른 환자의 예약이나 취소된 예약을 가리키면 묶지 않고, 계획은 자기 환자의 CARE_PLAN 행으로 남긴다.
    - 취소된 예약도 남기고 sourceStatus=cancelled 원문 그대로 둔다. '다음 진료'에서 거르는 것은 조회 쪽 규칙.
    - CarePlan.period를 예약일로 쓰지 않는다. 완료·취소된 CarePlan은 후속 항목이 아니다.
    - 진료과는 원본(또는 관련) 레코드에 같은 코드의 근거가 있을 때만 넣는다(DB 쓰기 규칙).
    """
    owner_by_ref = {resource_ref(res): owner for res, owner in owned}
    plans_for_appointment: dict[str, list[str]] = {}
    linked_plans: set[str] = set()
    for res, owner in owned:
        if res["resourceType"] != "CarePlan":
            continue
        for ref, _path in _iter_references(res.get("activity") or [], "CarePlan.activity"):
            target = index.resolve(ref)
            if target is None or target["resourceType"] != "Appointment":
                continue
            target_ref = resource_ref(target)
            if owner_by_ref.get(target_ref) != owner:
                continue  # 다른 환자의 예약에는 묶지 않는다. 계획은 아래에서 자기 환자의 CARE_PLAN 행으로 남는다
            if target.get("status") in _INACTIVE_APPOINTMENT:
                continue  # 취소된 예약에 묶으면 '다음 진료'에서 계획까지 사라지므로 계획 행을 따로 남긴다
            plans_for_appointment.setdefault(target_ref, []).append(resource_ref(res))
            linked_plans.add(resource_ref(res))

    def specialty_and_clinic(refs: list[str], owner: str) -> tuple[str, str | None]:
        for ref in refs:
            rec = records_by_ref.get(ref)
            if rec and rec["patientKey"] == owner and rec["specialtyCode"] != UNKNOWN and isinstance(rec.get("specialtyEvidence"), dict):
                clinic = rec.get("clinicKey")
                if clinic is None:  # CarePlan 등은 자신이 속한 Encounter의 병원 키를 쓴다
                    enc_key = next((k for k in rec["linkedResourceKeys"] if "|Encounter/" in k), None)
                    enc = records_by_ref.get(enc_key.split("|", 1)[1]) if enc_key else None
                    clinic = enc.get("clinicKey") if enc and enc["patientKey"] == owner else None
                return rec["specialtyCode"], clinic
        return UNKNOWN, None

    rows: list[dict] = []
    for res, owner in owned:
        ref = resource_ref(res)
        rec = records_by_ref.get(ref)
        if rec is None:  # 레코드 변환에 실패한 리소스는 일정도 만들지 않는다
            continue
        if res["resourceType"] == "Appointment":
            related = sorted(plans_for_appointment.get(ref, []))
            specialty, clinic = specialty_and_clinic([ref] + related, owner)
            precision = rec["datePrecision"]
            title = rec["title"] or next(
                (records_by_ref[p]["title"] for p in related
                 if p in records_by_ref and records_by_ref[p]["patientKey"] == owner and records_by_ref[p]["title"]), None
            ) or "예약"
            rows.append({
                "schemaVersion": SCHEMA_VERSION,
                "patientKey": owner,
                "followupKey": followup_key(rec["resourceKey"], "$"),
                "kind": "APPOINTMENT",
                "title": title,
                "sourceResourceKey": rec["resourceKey"],
                "sourceElementKey": "$",
                "relatedSourceResourceKeys": [resource_key(source, p) for p in related
                                              if p in records_by_ref and records_by_ref[p]["patientKey"] == owner],
                "sourceStatus": rec["fhirStatus"],
                "activityStatus": None,
                # 예약 일정은 원본 Appointment 레코드의 날짜 모델과 정확히 같아야 한다(DB 쓰기 규칙)
                "scheduledAt": rec["occurredAt"] if precision == "DATETIME" else None,
                "scheduledDate": rec["occurredAt"] if precision == "DATE" else None,
                "datePrecision": precision,
                "sourceDatePrecision": rec["sourceDatePrecision"],
                "timingEvidence": (
                    {"path": "Appointment.start", "originalValue": rec["sourceDateValue"]}
                    if rec["sourceDateValue"] is not None else None
                ),
                "specialtyCode": specialty,
                "clinicKey": clinic,
                "isSynthetic": is_synthetic,
            })
        elif res["resourceType"] == "CarePlan" and ref not in linked_plans:
            if res.get("status") not in ("active", "draft", "on-hold"):
                continue
            specialty, clinic = specialty_and_clinic([ref], owner)
            activity_status = next(
                ((a.get("detail") or {}).get("status") for a in res.get("activity") or []
                 if (a.get("detail") or {}).get("status")), None
            )
            rows.append({
                "schemaVersion": SCHEMA_VERSION,
                "patientKey": owner,
                "followupKey": followup_key(rec["resourceKey"], "$"),
                "kind": "CARE_PLAN",
                "title": rec["title"] or "후속 계획",
                "sourceResourceKey": rec["resourceKey"],
                "sourceElementKey": "$",
                "relatedSourceResourceKeys": [],
                "sourceStatus": rec["fhirStatus"],
                "activityStatus": activity_status,
                "scheduledAt": None,
                "scheduledDate": None,
                "datePrecision": "UNKNOWN",
                "sourceDatePrecision": "UNKNOWN",
                "timingEvidence": None,
                "specialtyCode": specialty,
                "clinicKey": clinic,
                "isSynthetic": is_synthetic,
            })
    rows.sort(key=lambda r: (r["patientKey"], r["scheduledAt"] or r["scheduledDate"] or "~", r["followupKey"]))
    return rows


# --------------------------------------------------------------------------- 진입점

def parse_bundle(
    bundle: dict,
    source: str = DEFAULT_SOURCE,
    *,
    encounter_info: dict[str, dict] | None = None,
    is_synthetic: bool = True,
    bundle_s3_key: str | None = None,
) -> ParseResult:
    """FHIR Bundle 1개를 Records / FollowUps / S3 원문 목록으로 변환한다.

    encounter_info: specialty_map.load_encounter_info() 결과. 없으면 진료과는 UNKNOWN.
    bundle_s3_key : 원본 Bundle을 S3에 함께 저장했다면 그 key (예: carecompass/v1/data/synthetic/fhir/bundles/A1.bundle.json)
    """
    index = _Index(bundle.get("entry", []))
    info_by_encounter = encounter_info or {}
    result = ParseResult()

    for res in index.resources.values():
        result.resources[s3_key(res, is_synthetic)] = res

    result.errors.extend(index.issues)

    owners: dict[str, str] = {}
    for ref, res in index.resources.items():
        if res["resourceType"] not in PATIENT_OWNED_TYPES:
            continue
        candidates, unresolved_owner = index.owner_resolution(res)
        if len(candidates) == 1 and not unresolved_owner:
            owners[ref] = next(iter(candidates))
        elif len(candidates) > 1:  # 여러 환자가 걸린 리소스는 어느 한 환자에게 임의로 붙이지 않는다
            result.errors.append({"resource": ref, "code": "AMBIGUOUS_OWNER",
                                  "error": f"환자가 2명 이상이라 변환하지 않음: {sorted(candidates)}"})
        else:  # 번들에서 환자를 찾을 수 없으면 조용히 버리지 않고 기록한다
            result.errors.append({"resource": ref, "code": "OWNER_UNRESOLVED",
                                  "error": "환자 참조를 모두 확인할 수 없어 소유자를 확정하지 않고 변환하지 않음"})

    children: dict[str, list[tuple[str, str, str]]] = {}  # Encounter → (이 Encounter를 참조하는 같은 환자 리소스, 참조, 경로)
    for ref, res in index.resources.items():
        if ref not in owners:
            continue
        enc_ref = (res.get("encounter") or {}).get("reference")
        enc = index.resolve(enc_ref)
        if enc is not None and enc["resourceType"] == "Encounter" and owners.get(resource_ref(enc)) == owners[ref]:
            children.setdefault(resource_ref(enc), []).append((ref, enc_ref, f"{res['resourceType']}.encounter"))

    owned: list[tuple[dict, str]] = []
    records_by_ref: dict[str, dict] = {}
    for ref, res in index.resources.items():
        if ref not in owners:
            continue
        problem = _db_precheck(res, owners[ref], source)
        if problem:
            result.errors.append({"resource": ref, "code": "INVALID_FOR_DB", "error": problem})
            continue
        owned.append((res, owners[ref]))
        try:
            record = _build_record(res, owners[ref], owners, children, index, info_by_encounter, source,
                                   is_synthetic, bundle_s3_key)
        except Exception as exc:  # 한 건의 실패가 전체 적재를 막지 않게 한다
            result.errors.append({"resource": ref, "code": "PARSE_ERROR", "error": repr(exc)})
            continue
        result.records.append(record)
        records_by_ref[ref] = record

    try:
        result.followups = _build_followups(owned, records_by_ref, index, source, is_synthetic)
    except Exception as exc:
        result.errors.append({"resource": "FollowUps", "code": "PARSE_ERROR", "error": repr(exc)})
    return result


def parse_file(path: str | Path, **kwargs: Any) -> ParseResult:
    with open(path, encoding="utf-8-sig") as fh:
        return parse_bundle(json.load(fh), **kwargs)


def index_by_key(items: Iterable[dict]) -> dict[str, dict]:
    """여러 번 적재해도 resourceKey당 1건만 남긴다 (같은 데이터 재적재 시 중복 방지)."""
    return {item["resourceKey"]: item for item in items}


def upsert_batches(records: list[dict], followups: list[dict], patient_key: str, size: int = 50) -> list[dict]:
    """한 환자의 행을 upsert_records 1회 요청(최대 50행) 단위로 나눈다.

    DB 쓰기 규칙에 맞춘 순서: Patient 행이 첫 요청에 들어가고, 일정 행은 그 원본 레코드가 들어간 뒤에 온다.
    linkedResourceKeys 대상이 아직 적재 전이면 그 행은 DB에서 PARTIAL로 저장될 수 있다
    (조회 Lambda의 get_record_detail은 읽을 때 연결 상태를 다시 계산한다).
    """
    order = {"Patient": 0, "Encounter": 1, "Condition": 2, "Observation": 3, "DiagnosticReport": 4,
             "MedicationRequest": 5, "CarePlan": 6, "Appointment": 7}
    mine = sorted((r for r in records if r["patientKey"] == patient_key),
                  key=lambda r: (order.get(r["resourceType"], 9), r["resourceKey"]))
    items: list[tuple[str, dict]] = [("record", r) for r in mine]
    items += [("followup", f) for f in followups if f["patientKey"] == patient_key]
    batches = []
    for start in range(0, len(items), size):
        chunk = items[start:start + size]
        batches.append({"records": [x for kind, x in chunk if kind == "record"],
                        "followups": [x for kind, x in chunk if kind == "followup"]})
    return batches
