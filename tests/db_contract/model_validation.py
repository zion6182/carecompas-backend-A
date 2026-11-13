"""Strict validation and key derivation for the DB writer's materialized models.

This module deliberately accepts CareCompass query models rather than FHIR
resources or DynamoDB items. It performs shape and contract checks only; the
Backend remains responsible for FHIR parsing and independently verifying S3
objects before it attests to the request.
"""

from __future__ import annotations

import base64
import hashlib
import re
from datetime import date, datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from common import ApiError, canonicalhash, time_token, reverse_token


MAX_RECORDS = 50
MAX_FOLLOWUPS = 50
FHIR_VERSION = "4.0.1"
SOURCE_PREFIXES = (
    "carecompass/v1/data/synthetic/fhir/",
    "carecompass/v1/data/fhir/",
)
RESOURCE_TYPES = frozenset(
    {
        "Patient",
        "Encounter",
        "Observation",
        "DiagnosticReport",
        "Condition",
        "MedicationRequest",
        "Appointment",
        "CarePlan",
    }
)
SHARED_RESOURCE_TYPES = frozenset({"Organization", "Location", "CodeSystem"})

RECORD_FIELDS = frozenset(
    {
        "schemaVersion",
        "itemType",
        "resourceKey",
        "sourceSystem",
        "resourceType",
        "resourceId",
        "patientKey",
        "fhirStatus",
        "ingestionStatus",
        "ingestionErrorCode",
        "s3Key",
        "sourceBundleS3Key",
        "sourceEntryFullUrl",
        "sourceSha256",
        "sourceVersionId",
        "ingestedAt",
        "lastAttemptAt",
        "isSynthetic",
        "title",
        "occurredAt",
        "datePrecision",
        "sourceDateValue",
        "sourceDatePrecision",
        "dateEvidencePath",
        "specialtyCode",
        "specialtySource",
        "specialtyEvidence",
        "clinicKey",
        "organizationResourceKey",
        "organizationName",
        "organizationNameSource",
        "linkedResourceKeys",
        "sharedReferences",
        "unresolvedReferences",
        "linkageStatus",
        "linkEvidence",
    }
)
FOLLOWUP_FIELDS = frozenset(
    {
        "schemaVersion",
        "patientKey",
        "followupKey",
        "kind",
        "title",
        "sourceResourceKey",
        "sourceElementKey",
        "relatedSourceResourceKeys",
        "sourceStatus",
        "activityStatus",
        "scheduledAt",
        "scheduledDate",
        "datePrecision",
        "sourceDatePrecision",
        "timingEvidence",
        "specialtyCode",
        "clinicKey",
        "isSynthetic",
        "ingestedAt",
    }
)
DERIVED_KEY_FIELDS = frozenset(
    {"PK", "SK", "timelinePK", "timelineSK", "specialtyTimelinePK", "specialtyTimelineSK", "schedulePK", "scheduleSK"}
)
RESOURCE_KEY_RE = re.compile(
    r"^(?P<source>[^|\s/]{1,128})\|(?P<type>[A-Z][A-Za-z0-9]{0,63})/(?P<id>[A-Za-z0-9.-]{1,64})$"
)
FHIR_ID_RE = re.compile(r"^[A-Za-z0-9.-]{1,64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SPECIALTY_RE = re.compile(r"^[0-9]{2}$")

FHIR_STATUS = {
    "Encounter": frozenset(
        {"planned", "arrived", "triaged", "in-progress", "onleave", "finished", "cancelled", "entered-in-error", "unknown"}
    ),
    "Observation": frozenset({"registered", "preliminary", "final", "amended", "corrected", "cancelled", "entered-in-error", "unknown"}),
    "DiagnosticReport": frozenset(
        {"registered", "partial", "preliminary", "final", "amended", "corrected", "appended", "cancelled", "entered-in-error", "unknown"}
    ),
    "Condition": frozenset({"active", "recurrence", "relapse", "inactive", "remission", "resolved"}),
    "MedicationRequest": frozenset(
        {"active", "on-hold", "cancelled", "completed", "entered-in-error", "stopped", "draft", "unknown"}
    ),
    "Appointment": frozenset(
        {"proposed", "pending", "booked", "arrived", "fulfilled", "cancelled", "noshow", "entered-in-error", "checked-in", "waitlist"}
    ),
    "CarePlan": frozenset({"draft", "active", "on-hold", "revoked", "completed", "entered-in-error", "unknown"}),
}
DATE_PRECISIONS = frozenset({"DATETIME", "DATE", "YEAR", "MONTH", "UNKNOWN"})
ACTIVITY_STATUSES = frozenset({"not-started", "scheduled", "in-progress", "on-hold", "completed", "cancelled", "unknown"})


def _invalid(message: str) -> None:
    raise ApiError("VALIDATION_ERROR", message)


def _is_plain_dict(value: Any) -> bool:
    return isinstance(value, dict)


def _keys(value: Mapping[str, Any], allowed: frozenset[str], label: str) -> None:
    derived = sorted(set(value) & DERIVED_KEY_FIELDS)
    if derived:
        _invalid(f"{label} must not include DynamoDB primary or index keys.")
    unknown = sorted(set(value) - allowed)
    if unknown:
        _invalid(f"{label} contains unsupported query-model fields.")


def _required(value: Mapping[str, Any], fields: tuple[str, ...], label: str) -> None:
    missing = [field for field in fields if field not in value]
    if missing:
        _invalid(f"{label} is missing required fields: {', '.join(missing)}.")


def _string(value: Any, label: str, *, maximum: int = 2048, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > maximum or (not allow_empty and not value.strip()):
        _invalid(f"{label} must be a string of at most {maximum} characters.")
    return value


def _nullable_string(value: Any, label: str, *, maximum: int = 2048) -> str | None:
    if value is None:
        return None
    return _string(value, label, maximum=maximum)


def parse_resource_key(resource_key: Any, label: str = "resourceKey") -> tuple[str, str, str]:
    text = _string(resource_key, label, maximum=300)
    match = RESOURCE_KEY_RE.fullmatch(text)
    if not match:
        _invalid(f"{label} must be sourceSystem|ResourceType/resourceId.")
    return match.group("source"), match.group("type"), match.group("id")


def _source_systems(token_claims: Mapping[str, Any]) -> set[str]:
    value = token_claims.get("sourceSystems")
    if isinstance(value, str):
        systems = {value}
    elif isinstance(value, (list, tuple, set, frozenset)):
        systems = {item for item in value if isinstance(item, str) and item}
    else:
        systems = set()
    if not systems:
        _invalid("The verified token does not authorize a source system.")
    return systems


def _verify_source(source: Any, token_claims: Mapping[str, Any], label: str = "sourceSystem") -> str:
    value = _string(source, label, maximum=128)
    if "|" in value or "/" in value or any(ch.isspace() for ch in value):
        _invalid(f"{label} is invalid.")
    if value not in _source_systems(token_claims):
        raise ApiError("FORBIDDEN", f"{label} is not authorized by the patient token.")
    return value


def _validate_pointer(value: Any, label: str, source_bucket: str) -> str:
    key = _string(value, label, maximum=1024)
    if "\\" in key or "?" in key or "#" in key:
        _invalid(f"{label} is not an allowed S3 object key.")
    segments = key.split("/")
    if any(part in {"", ".", ".."} for part in segments):
        _invalid(f"{label} is not an allowed S3 object key.")
    if not any(key.startswith(prefix) for prefix in SOURCE_PREFIXES):
        _invalid(f"{label} must use a verified CareCompass FHIR object prefix.")
    if any("incoming" in part.lower() for part in segments):
        _invalid(f"{label} must not point to an incoming object.")
    if not source_bucket or not source_bucket.strip():
        raise ApiError("CONFIGURATION_ERROR", "SOURCE_BUCKET is not configured.")
    return key


def _parse_datetime(value: str, label: str) -> datetime:
    _canonical_utc(value, label)
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        _invalid(f"{label} must be an ISO 8601 date-time.")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _invalid(f"{label} must include a timezone offset.")
    return parsed


_OFFSET_TIMESTAMP_RE = re.compile(
    r"^(?P<base>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?P<fraction>\.[0-9]+)?(?P<zone>Z|(?:\+|-)(?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))$"
)


def _canonical_utc(value: str, label: str) -> str:
    """Normalize the whole-second instant and keep every original fraction digit."""
    match = _OFFSET_TIMESTAMP_RE.fullmatch(value)
    if not match:
        _invalid(f"{label} must be a full ISO 8601 timestamp with seconds and a timezone.")
    zone_text = match.group("zone")
    parse_zone = "+00:00" if zone_text == "Z" else zone_text
    try:
        base = datetime.fromisoformat(match.group("base") + parse_zone)
    except (ValueError, OverflowError):
        _invalid(f"{label} must be a valid ISO 8601 timestamp.")
    if base.utcoffset() is None or abs(base.utcoffset().total_seconds()) > 14 * 60 * 60:
        _invalid(f"{label} timezone offset exceeds the FHIR date-time range.")
    try:
        utc_base = base.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    except OverflowError:
        _invalid(f"{label} is outside the supported timestamp range.")
    return utc_base + (match.group("fraction") or "") + "Z"


def _date_value(value: Any, label: str) -> str:
    text = _string(value, label, maximum=10)
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", text) or text[:4] == "0000":
        _invalid(f"{label} must be a valid YYYY-MM-DD date.")
    try:
        date.fromisoformat(text)
    except ValueError:
        _invalid(f"{label} must be YYYY-MM-DD.")
    if len(text) != 10:
        _invalid(f"{label} must be YYYY-MM-DD.")
    return text


def _validate_date_fields(
    *,
    precision: Any,
    source_precision: Any,
    occurred_at: Any,
    source_value: Any,
    label: str,
) -> tuple[str, str, str | None, str | None]:
    if (
        not isinstance(precision, str)
        or precision not in DATE_PRECISIONS
        or not isinstance(source_precision, str)
        or source_precision not in DATE_PRECISIONS
    ):
        _invalid(f"{label} has an unsupported date precision.")
    if precision == "DATETIME":
        value = _string(occurred_at, f"{label}.occurredAt", maximum=64)
        normalized = _canonical_utc(value, f"{label}.occurredAt")
        original = _string(source_value, f"{label}.sourceDateValue", maximum=128)
        _parse_datetime(original, f"{label}.sourceDateValue")
        if source_precision != "DATETIME":
            _invalid(f"{label} sourceDatePrecision must be DATETIME for a date-time.")
        if time_token(normalized) != time_token(original):
            _invalid(f"{label}.occurredAt must represent the same instant as sourceDateValue.")
        return precision, source_precision, normalized, original
    if precision == "DATE":
        value = _date_value(occurred_at, f"{label}.occurredAt")
        original = _date_value(source_value, f"{label}.sourceDateValue")
        if source_precision != "DATE":
            _invalid(f"{label} sourceDatePrecision must be DATE for a date.")
        if value != original:
            _invalid(f"{label}.occurredAt must match sourceDateValue for a DATE value.")
        return precision, source_precision, value, original
    if occurred_at is not None:
        _invalid(f"{label}.occurredAt must be null for a partial or unknown date.")
    if precision != "UNKNOWN":
        _invalid(f"{label}.datePrecision must be UNKNOWN for a partial or unknown date.")
    if source_precision == "YEAR":
        original = _string(source_value, f"{label}.sourceDateValue", maximum=4)
        if not re.fullmatch(r"[0-9]{4}", original) or original == "0000":
            _invalid(f"{label}.sourceDateValue must contain a four-digit year.")
        return precision, source_precision, None, original
    if source_precision == "MONTH":
        original = _string(source_value, f"{label}.sourceDateValue", maximum=7)
        if not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", original) or original[:4] == "0000":
            _invalid(f"{label}.sourceDateValue must contain a year and month.")
        try:
            datetime.strptime(original, "%Y-%m")
        except ValueError:
            _invalid(f"{label}.sourceDateValue has an invalid month.")
        return precision, source_precision, None, original
    if source_precision != "UNKNOWN" or source_value is not None:
        _invalid(f"{label} unknown dates must keep a null sourceDateValue and UNKNOWN sourceDatePrecision.")
    return "UNKNOWN", "UNKNOWN", None, None


def _specialty(value: Mapping[str, Any], label: str) -> tuple[str, str, Any]:
    code = _string(value.get("specialtyCode"), f"{label}.specialtyCode", maximum=16)
    source = _string(value.get("specialtySource"), f"{label}.specialtySource", maximum=64)
    evidence = value.get("specialtyEvidence")
    specialty_sources = {"SOURCE_MAPPING", "DEMO_MAPPING", "UNKNOWN"}
    if source not in specialty_sources:
        _invalid(f"{label}.specialtySource must be SOURCE_MAPPING, DEMO_MAPPING, or UNKNOWN.")
    if code == "UNKNOWN":
        if source != "UNKNOWN" or evidence is not None:
            _invalid(f"{label} UNKNOWN specialty must not contain specialty evidence.")
        return code, source, None
    if not SPECIALTY_RE.fullmatch(code):
        _invalid(f"{label}.specialtyCode must be a two-digit code or UNKNOWN.")
    if source == "UNKNOWN" or not _is_plain_dict(evidence):
        _invalid(f"{label} requires evidence for a non-UNKNOWN specialty.")
    if set(evidence) != {"path", "sourceValue", "mappingVersion", "description"}:
        _invalid(f"{label}.specialtyEvidence has an unsupported shape.")
    path = _string(evidence["path"], f"{label}.specialtyEvidence.path", maximum=512)
    if re.search(r"(?:^|[./])class$", path.strip(), flags=re.IGNORECASE):
        _invalid(f"{label} cannot derive specialty only from Encounter.class.")
    source_value = evidence["sourceValue"]
    description = evidence["description"]
    if source_value is not None:
        source_value = _string(source_value, f"{label}.specialtyEvidence.sourceValue", maximum=512)
    if description is not None:
        description = _string(description, f"{label}.specialtyEvidence.description", maximum=512)
    sanitized = {
        "path": path,
        "sourceValue": source_value,
        "mappingVersion": _string(evidence["mappingVersion"], f"{label}.specialtyEvidence.mappingVersion", maximum=512),
        "description": description,
    }
    return code, source, sanitized


def _list_of_strings(value: Any, label: str, *, maximum_items: int = 200, maximum_length: int = 300) -> list[str]:
    if not isinstance(value, list) or len(value) > maximum_items:
        _invalid(f"{label} must be a list with at most {maximum_items} entries.")
    items = [_string(item, label, maximum=maximum_length) for item in value]
    if len(items) != len(set(items)):
        _invalid(f"{label} must not contain duplicates.")
    return items


def _validate_reference_keys(values: list[str], token_claims: Mapping[str, Any], label: str) -> list[str]:
    authorized = _source_systems(token_claims)
    for key in values:
        source, resource_type, _resource_id = parse_resource_key(key, label)
        if source not in authorized:
            _invalid(f"{label} contains a reference from an unauthorized source system.")
        if resource_type not in RESOURCE_TYPES:
            _invalid(f"{label} may contain only patient-owned FHIR resource references.")
        if resource_type == "Patient" and key != token_claims.get("patientResourceKey"):
            raise ApiError("CROSS_PATIENT_REFERENCE", f"{label} contains a Patient reference outside the authorized patient.")
    return values


def validate_record_model(
    raw: Any,
    token_claims: Mapping[str, Any],
    source_bucket: str,
) -> dict[str, Any]:
    label = "record"
    if not _is_plain_dict(raw):
        _invalid(f"{label} must be a query-model object.")
    _keys(raw, RECORD_FIELDS, label)
    _required(
        raw,
        (
            "schemaVersion",
            "itemType",
            "resourceKey",
            "sourceSystem",
            "resourceType",
            "resourceId",
            "patientKey",
            "fhirStatus",
            "ingestionStatus",
            "s3Key",
            "sourceSha256",
            "isSynthetic",
            "occurredAt",
            "datePrecision",
            "sourceDateValue",
            "sourceDatePrecision",
            "specialtyCode",
            "specialtySource",
            "specialtyEvidence",
            "organizationNameSource",
            "linkedResourceKeys",
            "sharedReferences",
            "unresolvedReferences",
            "linkageStatus",
            "linkEvidence",
        ),
        label,
    )
    if type(raw["schemaVersion"]) is not int or raw["schemaVersion"] != 2:
        _invalid(f"{label}.schemaVersion must be 2.")
    if raw["itemType"] != "RESOURCE":
        _invalid(f"{label}.itemType must be RESOURCE.")
    if raw["ingestionStatus"] != "READY" or raw.get("ingestionErrorCode") not in (None, ""):
        _invalid("Only READY records without an ingestion error can be upserted.")
    patient_key = token_claims.get("patientKey")
    if not isinstance(patient_key, str) or not patient_key:
        _invalid("The verified patient token is missing patientKey.")
    if raw["patientKey"] != patient_key:
        raise ApiError("CROSS_PATIENT_REFERENCE", "The record patientKey does not match the verified patient token.")
    source_system = _verify_source(raw["sourceSystem"], token_claims)
    resource_type = _string(raw["resourceType"], f"{label}.resourceType", maximum=64)
    if resource_type not in RESOURCE_TYPES:
        _invalid(f"{label}.resourceType is not an accepted patient-owned FHIR type.")
    resource_id = _string(raw["resourceId"], f"{label}.resourceId", maximum=64)
    if not FHIR_ID_RE.fullmatch(resource_id):
        _invalid(f"{label}.resourceId is not a valid FHIR id.")
    resource_key = _string(raw["resourceKey"], f"{label}.resourceKey", maximum=300)
    if resource_key != f"{source_system}|{resource_type}/{resource_id}":
        _invalid(f"{label}.resourceKey must match sourceSystem, resourceType, and resourceId.")
    if resource_type == "Patient" and resource_key != token_claims.get("patientResourceKey"):
        raise ApiError("CROSS_PATIENT_REFERENCE", "A Patient row must match the patientResourceKey in the verified token.")

    source_hash = _string(raw["sourceSha256"], f"{label}.sourceSha256", maximum=64)
    if not SHA256_RE.fullmatch(source_hash):
        _invalid(f"{label}.sourceSha256 must be a lowercase SHA-256 hex digest.")
    pointer = _validate_pointer(raw["s3Key"], f"{label}.s3Key", source_bucket)
    bundle_pointer = raw.get("sourceBundleS3Key")
    if bundle_pointer is not None:
        bundle_pointer = _validate_pointer(bundle_pointer, f"{label}.sourceBundleS3Key", source_bucket)
    entry_url = _nullable_string(raw.get("sourceEntryFullUrl"), f"{label}.sourceEntryFullUrl", maximum=2048)
    version_id = _nullable_string(raw.get("sourceVersionId"), f"{label}.sourceVersionId", maximum=512)

    is_synthetic = token_claims.get("isSynthetic")
    if type(raw["isSynthetic"]) is not bool or raw["isSynthetic"] is not is_synthetic:
        _invalid(f"{label}.isSynthetic must match the verified patient token.")

    precision, source_precision, occurred_at, source_date_value = _validate_date_fields(
        precision=raw["datePrecision"],
        source_precision=raw["sourceDatePrecision"],
        occurred_at=raw["occurredAt"],
        source_value=raw["sourceDateValue"],
        label=label,
    )
    fhir_status = raw["fhirStatus"]
    if resource_type == "Patient":
        if fhir_status is not None:
            _invalid("Patient records must use a null fhirStatus.")
    elif resource_type == "Condition" and fhir_status is None:
        pass  # The storage contract permits a null clinicalStatus when it is absent or ambiguous.
    elif not isinstance(fhir_status, str) or fhir_status not in FHIR_STATUS[resource_type]:
        _invalid(f"{label}.fhirStatus is not valid for {resource_type} in FHIR R4.")

    specialty_code, specialty_source, specialty_evidence = _specialty(raw, label)
    link_keys = _validate_reference_keys(
        _list_of_strings(raw["linkedResourceKeys"], f"{label}.linkedResourceKeys"), token_claims, f"{label}.linkedResourceKeys"
    )
    shared_raw = raw["sharedReferences"]
    if not isinstance(shared_raw, list) or len(shared_raw) > 100:
        _invalid(f"{label}.sharedReferences must be a list with at most 100 entries.")
    shared_refs: list[dict[str, str]] = []
    seen_shared: set[str] = set()
    for index, ref in enumerate(shared_raw):
        ref_label = f"{label}.sharedReferences[{index}]"
        if not _is_plain_dict(ref) or set(ref) != {"resourceKey", "resourceType", "s3Key"}:
            _invalid(f"{ref_label} has an unsupported shape.")
        key = _string(ref["resourceKey"], f"{ref_label}.resourceKey", maximum=300)
        source, ref_type, _ref_id = parse_resource_key(key, f"{ref_label}.resourceKey")
        ref_type_field = _string(ref["resourceType"], f"{ref_label}.resourceType", maximum=64)
        if ref_type not in SHARED_RESOURCE_TYPES or ref_type_field != ref_type:
            _invalid(f"{ref_label} must refer only to an Organization, Location, or CodeSystem.")
        if source not in _source_systems(token_claims):
            _invalid(f"{ref_label} uses an unauthorized source system.")
        if key in seen_shared:
            _invalid(f"{label}.sharedReferences must not contain duplicate resource keys.")
        seen_shared.add(key)
        shared_refs.append(
            {
                "resourceKey": key,
                "resourceType": ref_type,
                "s3Key": _validate_pointer(ref["s3Key"], f"{ref_label}.s3Key", source_bucket),
            }
        )

    unresolved = raw["unresolvedReferences"]
    if not isinstance(unresolved, list) or len(unresolved) > 200:
        _invalid(f"{label}.unresolvedReferences must be a list with at most 200 entries.")
    unresolved_clean = [_string(item, f"{label}.unresolvedReferences", maximum=2048) for item in unresolved]
    if not isinstance(raw["linkageStatus"], str) or raw["linkageStatus"] not in {"RESOLVED", "PARTIAL"}:
        _invalid(f"{label}.linkageStatus must be RESOLVED or PARTIAL.")
    link_evidence_raw = raw["linkEvidence"]
    if not isinstance(link_evidence_raw, list) or len(link_evidence_raw) > 300:
        _invalid(f"{label}.linkEvidence must be a list with at most 300 entries.")
    link_evidence: list[dict[str, str]] = []
    for index, evidence in enumerate(link_evidence_raw):
        evidence_label = f"{label}.linkEvidence[{index}]"
        if not _is_plain_dict(evidence) or set(evidence) != {"targetResourceKey", "reference", "path"}:
            _invalid(f"{evidence_label} has an unsupported shape.")
        target_key = _string(evidence["targetResourceKey"], f"{evidence_label}.targetResourceKey", maximum=300)
        target_source, target_type, _target_id = parse_resource_key(target_key, f"{evidence_label}.targetResourceKey")
        if target_source not in _source_systems(token_claims):
            _invalid(f"{evidence_label} uses an unauthorized source system.")
        if target_type == "Patient" and target_key != token_claims.get("patientResourceKey"):
            raise ApiError("CROSS_PATIENT_REFERENCE", "Record link evidence contains a Patient reference outside the authorized patient.")
        link_evidence.append(
            {
                "targetResourceKey": target_key,
                "reference": _string(evidence["reference"], f"{evidence_label}.reference", maximum=2048),
                "path": _string(evidence["path"], f"{evidence_label}.path", maximum=512),
            }
        )

    def optional_text(field: str, maximum: int = 512) -> str | None:
        return _nullable_string(raw.get(field), f"{label}.{field}", maximum=maximum)

    clinic_key = optional_text("clinicKey", 256)
    org_key = optional_text("organizationResourceKey", 300)
    if org_key is not None:
        org_source, org_type, _org_id = parse_resource_key(org_key, f"{label}.organizationResourceKey")
        if org_type != "Organization" or org_source not in _source_systems(token_claims):
            _invalid(f"{label}.organizationResourceKey must reference an authorized Organization.")
    organization_name_source = _string(raw.get("organizationNameSource"), f"{label}.organizationNameSource", maximum=64)
    allowed_name_sources = {"UNKNOWN", "FHIR", "CLINIC_MAPPING"}
    if organization_name_source not in allowed_name_sources:
        _invalid(f"{label}.organizationNameSource is unsupported.")
    title = optional_text("title")
    if resource_type == "Encounter" and title is None:
        _invalid("READY Encounter records require a backend-generated title.")

    sanitized: dict[str, Any] = {
        "schemaVersion": 2,
        "itemType": "RESOURCE",
        "resourceKey": resource_key,
        "sourceSystem": source_system,
        "resourceType": resource_type,
        "resourceId": resource_id,
        "patientKey": patient_key,
        "fhirStatus": fhir_status,
        "ingestionStatus": "READY",
        "ingestionErrorCode": None,
        "s3Key": pointer,
        "sourceBundleS3Key": bundle_pointer,
        "sourceEntryFullUrl": entry_url,
        "sourceSha256": source_hash,
        "sourceVersionId": version_id,
        "isSynthetic": is_synthetic,
        "title": title,
        "occurredAt": occurred_at,
        "datePrecision": precision,
        "sourceDateValue": source_date_value,
        "sourceDatePrecision": source_precision,
        "dateEvidencePath": optional_text("dateEvidencePath", 512),
        "specialtyCode": specialty_code,
        "specialtySource": specialty_source,
        "specialtyEvidence": specialty_evidence,
        "clinicKey": clinic_key,
        "organizationResourceKey": org_key,
        "organizationName": optional_text("organizationName"),
        "organizationNameSource": organization_name_source,
        "linkedResourceKeys": link_keys,
        "sharedReferences": shared_refs,
        "unresolvedReferences": unresolved_clean,
        "linkageStatus": raw["linkageStatus"],
        "linkEvidence": link_evidence,
    }
    return sanitized


def _followup_date_fields(raw: Mapping[str, Any], label: str) -> tuple[str, str, str | None, str | None]:
    precision = raw.get("datePrecision")
    source_precision = raw.get("sourceDatePrecision")
    scheduled_at = raw.get("scheduledAt")
    scheduled_date = raw.get("scheduledDate")
    source_value = None
    timing_evidence = raw.get("timingEvidence")
    if isinstance(timing_evidence, dict):
        source_value = timing_evidence.get("originalValue")

    if precision == "DATETIME":
        if scheduled_date is not None:
            _invalid(f"{label}.scheduledDate must be null for a date-time.")
        parsed_at = _string(scheduled_at, f"{label}.scheduledAt", maximum=64)
        parsed_at = _canonical_utc(parsed_at, f"{label}.scheduledAt")
        if source_precision != "DATETIME":
            _invalid(f"{label}.sourceDatePrecision must be DATETIME for a date-time.")
        source_value = _string(source_value, f"{label}.timingEvidence.originalValue", maximum=128)
        _parse_datetime(source_value, f"{label}.timingEvidence.originalValue")
        if time_token(parsed_at) != time_token(source_value):
            _invalid(f"{label}.scheduledAt must represent the same instant as timingEvidence.originalValue.")
        return precision, source_precision, parsed_at, None
    if precision == "DATE":
        if scheduled_at is not None:
            _invalid(f"{label}.scheduledAt must be null for a date-only value.")
        parsed_date = _date_value(scheduled_date, f"{label}.scheduledDate")
        if source_precision != "DATE":
            _invalid(f"{label}.sourceDatePrecision must be DATE for a date.")
        source_value = _date_value(source_value, f"{label}.timingEvidence.originalValue")
        if parsed_date != source_value:
            _invalid(f"{label}.scheduledDate must match timingEvidence.originalValue.")
        return precision, source_precision, None, parsed_date
    if precision == "UNKNOWN":
        if scheduled_at is not None or scheduled_date is not None:
            _invalid(f"{label} unknown dates must have null scheduledAt and scheduledDate.")
        if not isinstance(source_precision, str) or source_precision not in {"UNKNOWN", "YEAR", "MONTH"}:
            _invalid(f"{label}.sourceDatePrecision is unsupported for an unknown date.")
        if source_precision == "YEAR" and not (isinstance(source_value, str) and re.fullmatch(r"[0-9]{4}", source_value) and source_value != "0000"):
            _invalid(f"{label} must preserve a four-digit year in timingEvidence.originalValue.")
        if source_precision == "MONTH" and not (isinstance(source_value, str) and re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", source_value) and source_value[:4] != "0000"):
            _invalid(f"{label} must preserve a year-month in timingEvidence.originalValue.")
        if source_precision == "UNKNOWN" and source_value is not None:
            _invalid(f"{label} unknown dates must not contain an originalValue.")
        return "UNKNOWN", source_precision, None, None
    _invalid(f"{label}.datePrecision is unsupported.")


def validate_followup_model(raw: Any, token_claims: Mapping[str, Any]) -> dict[str, Any]:
    label = "followup"
    if not _is_plain_dict(raw):
        _invalid(f"{label} must be a query-model object.")
    _keys(raw, FOLLOWUP_FIELDS, label)
    _required(
        raw,
        (
            "schemaVersion",
            "patientKey",
            "followupKey",
            "kind",
            "title",
            "sourceResourceKey",
            "sourceElementKey",
            "relatedSourceResourceKeys",
            "sourceStatus",
            "activityStatus",
            "scheduledAt",
            "scheduledDate",
            "datePrecision",
            "sourceDatePrecision",
            "timingEvidence",
            "specialtyCode",
            "clinicKey",
            "isSynthetic",
        ),
        label,
    )
    if type(raw["schemaVersion"]) is not int or raw["schemaVersion"] != 2:
        _invalid(f"{label}.schemaVersion must be 2.")
    patient_key = token_claims.get("patientKey")
    if raw["patientKey"] != patient_key:
        raise ApiError("CROSS_PATIENT_REFERENCE", "The followup patientKey does not match the verified patient token.")
    source_key = _string(raw["sourceResourceKey"], f"{label}.sourceResourceKey", maximum=300)
    source_system, source_type, _source_id = parse_resource_key(source_key, f"{label}.sourceResourceKey")
    if source_system not in _source_systems(token_claims):
        _invalid(f"{label}.sourceResourceKey uses an unauthorized source system.")
    kind = _string(raw["kind"], f"{label}.kind", maximum=32)
    expected_type = {"APPOINTMENT": "Appointment", "CARE_PLAN": "CarePlan"}.get(kind)
    if expected_type is None or source_type != expected_type:
        _invalid(f"{label}.kind must match its Appointment or CarePlan source resource.")
    source_element_key = _string(raw["sourceElementKey"], f"{label}.sourceElementKey", maximum=256)
    if "\n" in source_element_key or "\r" in source_element_key:
        _invalid(f"{label}.sourceElementKey is invalid.")
    expected_followup_key = hashlib.sha256(f"{source_key}\n{source_element_key}".encode("utf-8")).hexdigest()
    followup_key = _string(raw["followupKey"], f"{label}.followupKey", maximum=64)
    if followup_key != expected_followup_key:
        _invalid(f"{label}.followupKey must be SHA-256(sourceResourceKey + newline + sourceElementKey).")

    is_synthetic = token_claims.get("isSynthetic")
    if type(raw["isSynthetic"]) is not bool or raw["isSynthetic"] is not is_synthetic:
        _invalid(f"{label}.isSynthetic must match the verified patient token.")
    status = _string(raw["sourceStatus"], f"{label}.sourceStatus", maximum=64)
    status_enum = FHIR_STATUS[expected_type]
    if status not in status_enum:
        _invalid(f"{label}.sourceStatus is not valid for {expected_type} in FHIR R4.")
    activity_status = raw["activityStatus"]
    if activity_status is not None and (not isinstance(activity_status, str) or activity_status not in ACTIVITY_STATUSES):
        _invalid(f"{label}.activityStatus is not a valid CarePlan activity status.")
    if kind == "APPOINTMENT" and activity_status is not None:
        _invalid(f"{label}.activityStatus must be null for an Appointment.")

    precision, source_precision, scheduled_at, scheduled_date = _followup_date_fields(raw, label)
    timing_raw = raw["timingEvidence"]
    timing_evidence: dict[str, str] | None
    if timing_raw is None:
        timing_evidence = None
    elif isinstance(timing_raw, dict) and set(timing_raw) == {"path", "originalValue"}:
        timing_evidence = {
            "path": _string(timing_raw["path"], f"{label}.timingEvidence.path", maximum=512),
            "originalValue": _string(timing_raw["originalValue"], f"{label}.timingEvidence.originalValue", maximum=128),
        }
    else:
        _invalid(f"{label}.timingEvidence has an unsupported shape.")
    if source_precision in {"YEAR", "MONTH"} and timing_evidence is None:
        _invalid(f"{label} partial source dates require timingEvidence.")
    if precision in {"DATE", "DATETIME"} and timing_evidence is None:
        _invalid(f"{label} dated schedule values require timingEvidence.")

    related_keys = _validate_reference_keys(
        _list_of_strings(raw["relatedSourceResourceKeys"], f"{label}.relatedSourceResourceKeys"),
        token_claims,
        f"{label}.relatedSourceResourceKeys",
    )
    specialty_code = _string(raw["specialtyCode"], f"{label}.specialtyCode", maximum=16)
    if specialty_code != "UNKNOWN" and not SPECIALTY_RE.fullmatch(specialty_code):
        _invalid(f"{label}.specialtyCode must be a two-digit code or UNKNOWN.")
    clinic_key = _nullable_string(raw["clinicKey"], f"{label}.clinicKey", maximum=256)
    title = _nullable_string(raw["title"], f"{label}.title", maximum=512)
    if title is None:
        _invalid(f"{label}.title must be a string.")

    return {
        "schemaVersion": 2,
        "patientKey": patient_key,
        "followupKey": followup_key,
        "kind": kind,
        "title": title,
        "sourceResourceKey": source_key,
        "sourceElementKey": source_element_key,
        "relatedSourceResourceKeys": related_keys,
        "sourceStatus": status,
        "activityStatus": activity_status,
        "scheduledAt": scheduled_at,
        "scheduledDate": scheduled_date,
        "datePrecision": precision,
        "sourceDatePrecision": source_precision,
        "timingEvidence": timing_evidence,
        "specialtyCode": specialty_code,
        "clinicKey": clinic_key,
        "isSynthetic": is_synthetic,
    }


def validate_source_verification(value: Any) -> dict[str, Any]:
    if not _is_plain_dict(value) or set(value) != {"fhirVersion", "validated", "objectsVerified"}:
        _invalid("sourceVerification must contain exactly fhirVersion, validated, and objectsVerified.")
    if value.get("fhirVersion") != FHIR_VERSION:
        _invalid("sourceVerification.fhirVersion must be 4.0.1.")
    if value.get("validated") is not True or value.get("objectsVerified") is not True:
        _invalid("The Backend must attest to FHIR R4 validation and S3 object verification before upsert.")
    return {"fhirVersion": FHIR_VERSION, "validated": True, "objectsVerified": True}


def record_keys(item: Mapping[str, Any], patient_pk: str) -> dict[str, str]:
    resource_key = item["resourceKey"]
    pk = patient_pk
    sk = f"RESOURCE#{resource_key}"
    derived = {"PK": pk, "SK": sk}
    if item["resourceType"] == "Encounter":
        if item["datePrecision"] in {"DATE", "DATETIME"}:
            if item["datePrecision"] == "DATE":
                local_midnight = datetime.combine(date.fromisoformat(item["occurredAt"]), datetime.min.time(), ZoneInfo("Asia/Seoul"))
                sort_time = local_midnight.isoformat()
            else:
                sort_time = item["occurredAt"]
            token = reverse_token(time_token(sort_time))
            record_id = base64.urlsafe_b64encode(resource_key.encode("utf-8")).decode("ascii").rstrip("=")
            timeline_sk = f"0#{token}#{record_id}"
        else:
            record_id = base64.urlsafe_b64encode(resource_key.encode("utf-8")).decode("ascii").rstrip("=")
            timeline_sk = f"1#{record_id}"
        derived["timelinePK"] = f"{patient_pk}#ENCOUNTERS"
        derived["timelineSK"] = timeline_sk
        if item["specialtyCode"] != "UNKNOWN":
            derived["specialtyTimelinePK"] = f"{patient_pk}#SPECIALTY#{item['specialtyCode']}"
            derived["specialtyTimelineSK"] = timeline_sk
    return derived


def followup_keys(item: Mapping[str, Any], patient_pk: str) -> dict[str, str]:
    followup_key = item["followupKey"]
    derived = {"PK": patient_pk, "SK": f"FOLLOWUP#{followup_key}", "schedulePK": f"{patient_pk}#FOLLOWUPS"}
    if item["datePrecision"] == "DATETIME":
        schedule_time = item["scheduledAt"]
        schedule_token = time_token(schedule_time)
        derived["scheduleSK"] = f"0#{schedule_token}#{followup_key}"
    elif item["datePrecision"] == "DATE":
        local_midnight = datetime.combine(date.fromisoformat(item["scheduledDate"]), datetime.min.time(), ZoneInfo("Asia/Seoul"))
        derived["scheduleSK"] = f"0#{time_token(local_midnight.isoformat())}#{followup_key}"
    else:
        derived["scheduleSK"] = f"1#{followup_key}"
    return derived


def business_hash(item: Mapping[str, Any], *, followup: bool = False) -> str:
    ignored = {
        "PK",
        "SK",
        "timelinePK",
        "timelineSK",
        "specialtyTimelinePK",
        "specialtyTimelineSK",
        "schedulePK",
        "scheduleSK",
        "ingestedAt",
        "lastAttemptAt",
        "sourceBucket",
        "contentHash",
        "revision",
        "writeVersion",
    }
    if followup:
        # FollowUps have no timeline attributes, but sharing the same exclusion
        # set makes the canonical comparison safe for both row types.
        pass
    projection = {key: value for key, value in item.items() if key not in ignored}
    return canonicalhash(projection)

