"""FHIR 파서 테스트 (schemaVersion 2).

1) DB 계약: 모든 출력이 DB 팀 쓰기 Lambda의 검증 코드(tests/db_contract)를 통과하는가
2) 골든: DB 팀 패키지의 Records.jsonl / FollowUps.jsonl(v1)과 핵심 값이 같은가
3) 단위: 날짜, 끊긴 참조, 다른 환자 참조, 중복 적재 등 작은 번들로 규칙 확인
"""
import json
import sys
from pathlib import Path

import pytest

from fhir_parser import (
    DEFAULT_SOURCE,
    date_model,
    decode_id,
    encode_id,
    followup_key,
    index_by_key,
    parse_bundle,
    parse_file,
    serialize_resource,
    sha256_hex,
    upsert_batches,
)
from specialty_map import load_encounter_info, specialty_label

sys.path.insert(0, str(Path(__file__).resolve().parent / "db_contract"))
import model_validation as db_contract  # noqa: E402  DB 팀 DB Lambda 검증 코드 (수정 없이 복사)

DATA = Path(__file__).resolve().parents[1] / "sample_data" / "carecompass_6patients"
ALL = DATA / "fhir" / "all-patients.bundle.json"
BUNDLES = sorted((DATA / "fhir" / "bundles").glob("*.bundle.json"))
TIMELINE = DATA / "query_models" / "encounter_timeline.json"


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def claims(patient, source=DEFAULT_SOURCE):
    """쓰기 Lambda가 patientToken에서 꺼내는 값 (테스트용)"""
    return {"patientKey": patient, "patientResourceKey": f"{source}|Patient/{patient}",
            "sourceSystems": [source], "isSynthetic": True, "datasetId": source}


@pytest.fixture(scope="module")
def info():
    return load_encounter_info(TIMELINE)


@pytest.fixture(scope="module")
def result(info):
    return parse_file(ALL, encounter_info=info)


@pytest.fixture(scope="module")
def expected_records():
    return {r["resourceKey"]: r for r in read_jsonl(DATA / "query_models" / "Records.jsonl")}


# ===================================================================== DB 계약 (schemaVersion 2)

def test_every_record_passes_db_writer_validation(result):
    for rec in result.records:
        db_contract.validate_record_model(rec, claims(rec["patientKey"]), "carecompass-data-test")


def test_every_followup_passes_db_writer_validation(result):
    for fu in result.followups:
        db_contract.validate_followup_model(fu, claims(fu["patientKey"]))


def test_followup_rules_checked_by_db_writer_transaction(result):
    """쓰기 Lambda가 트랜잭션 전에 확인하는 규칙 (write_handler.py 650~700행)."""
    by_key = index_by_key(result.records)
    for fu in result.followups:
        src = by_key[fu["sourceResourceKey"]]
        assert fu["sourceStatus"] == src["fhirStatus"]
        if fu["kind"] == "APPOINTMENT":
            assert (fu["datePrecision"], fu["sourceDatePrecision"]) == (src["datePrecision"], src["sourceDatePrecision"])
            assert (fu["scheduledAt"] or fu["scheduledDate"]) == src["occurredAt"]
            assert (fu["timingEvidence"] or {}).get("originalValue") == src["sourceDateValue"]
        if fu["specialtyCode"] != "UNKNOWN":
            sources = [src] + [by_key[k] for k in fu["relatedSourceResourceKeys"]]
            assert any(s["specialtyCode"] == fu["specialtyCode"] and isinstance(s["specialtyEvidence"], dict)
                       for s in sources)


def test_upsert_batches_order(result):
    for patient in result.patients():
        batches = upsert_batches(result.records, result.followups, patient, size=10)
        assert all(len(b["records"]) + len(b["followups"]) <= 10 for b in batches)
        assert batches[0]["records"][0]["resourceType"] == "Patient"  # Patient 행이 첫 요청에
        seen = set()
        for b in batches:
            seen |= {r["resourceKey"] for r in b["records"]}
            assert all(f["sourceResourceKey"] in seen for f in b["followups"])  # 원본 레코드가 먼저
        total = sum(len(b["records"]) + len(b["followups"]) for b in batches)
        assert total == sum(r["patientKey"] == patient for r in result.records + result.followups)


def test_sha256_matches_teammates_manifest(result):
    """DB 팀 manifest.json의 원문 파일 해시와 sourceSha256이 같다 (= 이미 만든 원문 파일과 같은 바이트)."""
    manifest = json.loads((DATA / "manifest.json").read_text(encoding="utf-8"))["files"]
    objects = result.object_bytes()
    assert len(objects) == 122
    for s3key, data in objects.items():
        name = "fhir/resources/" + s3key.rsplit("/", 1)[1]
        assert sha256_hex(data) == manifest[name]["sha256"], name
        assert len(data) == manifest[name]["bytes"], name
    for rec in result.records:
        assert rec["sourceSha256"] == manifest["fhir/resources/" + rec["s3Key"].rsplit("/", 1)[1]]["sha256"]


def test_sha256_matches_s3_bytes(result):
    objects = result.object_bytes()
    for rec in result.records:
        assert sha256_hex(objects[rec["s3Key"]]) == rec["sourceSha256"]
        assert rec["s3Key"].startswith("carecompass/v1/data/synthetic/fhir/resources/")


# ===================================================================== 골든: DB 팀 v1 조회 모델과 핵심 값 동일

def test_records_match_teammates_model(result, expected_records):
    mine = index_by_key(result.records)
    assert set(mine) == set(expected_records), "resourceKey 집합이 다름"
    for key, exp in expected_records.items():
        got = mine[key]
        for f in ("patientKey", "resourceType", "fhirStatus", "ingestionStatus", "isSynthetic",
                  "specialtyCode", "specialtySource", "linkedResourceKeys"):
            assert got[f] == exp[f], (key, f, got[f], exp[f])
        assert got["linkageStatus"] == exp["linkStatus"]
        if exp["occurredAt"] is not None:  # v1에 있던 시각은 그대로. (v1은 Appointment.start를 비워 뒀음)
            assert got["occurredAt"] == exp["occurredAt"], key
        if "clinicKey" in exp:
            assert got["clinicKey"] == exp["clinicKey"]
        assert got["s3Key"].endswith(exp["s3Key"])


def test_record_counts(result):
    assert len(result.records) == 105
    kinds = {}
    for r in result.records:
        kinds[r["resourceType"]] = kinds.get(r["resourceType"], 0) + 1
    assert kinds == {"Patient": 6, "Encounter": 36, "Condition": 12, "Observation": 36,
                     "MedicationRequest": 1, "DiagnosticReport": 8, "Appointment": 3, "CarePlan": 3}
    assert not result.errors


def test_followups_match_teammates_model(result):
    """v1 FollowUps 4행과 같은 일정. kind PLAN→CARE_PLAN, status는 FHIR 원문 상태로 바뀜."""
    v1 = read_jsonl(DATA / "query_models" / "FollowUps.jsonl")
    to_v2 = {"BOOKED": "booked", "CANCELLED": "cancelled", "DATE_UNDETERMINED": "active"}
    expected = sorted((e["patientKey"], "CARE_PLAN" if e["kind"] == "PLAN" else e["kind"], to_v2[e["status"]],
                       e["scheduledAt"], e["sourceResourceKeys"][-1]) for e in v1)
    got = sorted((f["patientKey"], f["kind"], f["sourceStatus"], f["scheduledAt"], f["sourceResourceKey"])
                 for f in result.followups)
    assert got == expected


def test_followups_cancelled_and_undetermined_are_distinguished(result):
    by_status = {f["sourceStatus"]: f for f in result.followups}
    assert {"booked", "cancelled", "active"} == set(by_status)
    plan = by_status["active"]
    assert plan["kind"] == "CARE_PLAN" and plan["scheduledAt"] is None and plan["scheduledDate"] is None  # 날짜를 지어내지 않음
    assert plan["datePrecision"] == "UNKNOWN" and plan["timingEvidence"] is None


def test_careplan_linked_to_appointment_is_one_row(result):
    a1 = [f for f in result.followups if f["patientKey"] == "A1"]
    assert len(a1) == 1 and a1[0]["relatedSourceResourceKeys"] == [f"{DEFAULT_SOURCE}|CarePlan/A1-careplan-01"]
    assert a1[0]["specialtyCode"] == "05"  # CarePlan이 속한 정형외과 진료에서 근거를 가져옴


def test_per_patient_bundles_equal_all_patients_bundle(result, info):
    combined = {}
    for path in BUNDLES:
        part = parse_file(path, encounter_info=info)
        for r in part.records:  # 번들마다 fullUrl은 같고 sha도 같은 원문이므로 같은 행
            combined[r["resourceKey"]] = r
    assert combined == index_by_key(result.records)


def test_all_fhir_resources_are_kept_for_s3(result):
    assert len(result.resources) == 122  # Patient 6, Encounter 36, ... + Organization 8, Location 8, CodeSystem 1
    assert "carecompass/v1/data/synthetic/fhir/resources/Organization-hira-243ee9c36e9a37f4.json" in result.resources


def test_encounter_has_title_and_organization(result):
    for r in result.records:
        if r["resourceType"] == "Encounter":
            assert r["title"] and r["organizationName"] and r["organizationNameSource"] == "FHIR"
            assert r["organizationResourceKey"].startswith(f"{DEFAULT_SOURCE}|Organization/")
            assert {s["resourceType"] for s in r["sharedReferences"]} <= {"Organization", "Location", "CodeSystem"}


# ===================================================================== 안전성

def test_links_never_cross_patients(result):
    owner_of = {r["resourceKey"]: r["patientKey"] for r in result.records}
    for r in result.records:
        assert all(owner_of[k] == r["patientKey"] for k in r["linkedResourceKeys"])
        assert all(owner_of[e["targetResourceKey"]] == r["patientKey"] for e in r["linkEvidence"])


def test_every_link_has_evidence(result):
    for r in result.records:
        assert set(r["linkedResourceKeys"]) == {e["targetResourceKey"] for e in r["linkEvidence"]}


def test_clinic_key_is_read_from_fhir_organization(result, info):
    """진료과와 달리 병원 키는 FHIR 원본(Organization.identifier)에서 직접 읽는다."""
    for r in result.records:
        if r["resourceType"] == "Encounter":
            ref = r["resourceKey"].split("|", 1)[1]
            assert r["clinicKey"] == info[ref]["clinicKey"]


def test_without_encounter_info_specialty_is_unknown(result):
    plain = parse_file(ALL)  # 진료과 정보 없이
    assert {r["specialtyCode"] for r in plain.records} == {"UNKNOWN"}
    assert {r["specialtySource"] for r in plain.records} == {"UNKNOWN"}
    assert all(r["specialtyEvidence"] is None for r in plain.records)
    assert {f["specialtyCode"] for f in plain.followups} == {"UNKNOWN"}
    for r in plain.records:
        if r["resourceType"] == "Encounter":
            assert r["title"]  # 매핑 없어도 Encounter 제목은 있어야 한다 (DB 필수)
        db_contract.validate_record_model(r, claims(r["patientKey"]), "carecompass-data-test")


def test_specialty_labels():
    assert (specialty_label("05"), specialty_label("01"), specialty_label("14")) == ("정형외과", "내과", "피부과")
    assert specialty_label("99") == specialty_label(None) == "확인 불가"


# ===================================================================== 날짜·식별자

def test_resource_key_roundtrip():
    key = f"{DEFAULT_SOURCE}|Encounter/A1-01"
    token = encode_id(key)
    assert "=" not in token and "|" not in token and "/" not in token
    assert decode_id(token) == key


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-09-01T10:00:00+09:00", ("2026-09-01T01:00:00Z", "DATETIME", "2026-09-01T10:00:00+09:00", "DATETIME")),
        ("2025-01-17T02:13:46.120+09:00", ("2025-01-16T17:13:46.120Z", "DATETIME", "2025-01-17T02:13:46.120+09:00", "DATETIME")),
        ("2026-07-01T00:00:00Z", ("2026-07-01T00:00:00Z", "DATETIME", "2026-07-01T00:00:00Z", "DATETIME")),
        ("2026-07-01T09:00:00", ("2026-07-01", "DATE", "2026-07-01", "DATE")),  # 시간대 없음 → 날짜로 낮춤
        ("2026-08-15", ("2026-08-15", "DATE", "2026-08-15", "DATE")),
        ("2026-08", (None, "UNKNOWN", "2026-08", "MONTH")),
        ("2026", (None, "UNKNOWN", "2026", "YEAR")),
        ("2026-02-30", (None, "UNKNOWN", None, "UNKNOWN")),
        ("2026-12-31T23:59:60+09:00", (None, "UNKNOWN", None, "UNKNOWN")),  # 윤초는 보정하지 않음
        ("", (None, "UNKNOWN", None, "UNKNOWN")),
        (None, (None, "UNKNOWN", None, "UNKNOWN")),
        ("not-a-date", (None, "UNKNOWN", None, "UNKNOWN")),
    ],
)
def test_date_model(value, expected):
    got = date_model(value)
    assert (got["occurredAt"], got["datePrecision"], got["sourceDateValue"], got["sourceDatePrecision"]) == expected


# ===================================================================== 단위: 작은 번들

def bundle(*resources):
    return {"resourceType": "Bundle", "type": "collection",
            "entry": [{"fullUrl": f"https://x.example/{r['resourceType']}/{r['id']}", "resource": r} for r in resources]}


PATIENT = {"resourceType": "Patient", "id": "P1"}
OTHER = {"resourceType": "Patient", "id": "P2"}


def enc(id_, start="2026-01-01T09:00:00+09:00", patient="P1", **extra):
    res = {"resourceType": "Encounter", "id": id_, "status": "finished", "subject": {"reference": f"Patient/{patient}"}}
    if start:
        res["period"] = {"start": start}
    res.update(extra)
    return res


def get(result, ref):
    return next(r for r in result.records if r["resourceKey"].endswith("|" + ref))


def valid(result):
    for r in result.records:
        db_contract.validate_record_model(r, claims(r["patientKey"]), "carecompass-data-test")
    for f in result.followups:
        db_contract.validate_followup_model(f, claims(f["patientKey"]))
    return result


def test_undated_encounter_has_no_invented_date():
    r = get(valid(parse_bundle(bundle(PATIENT, enc("E1", start=None)))), "Encounter/E1")
    assert (r["occurredAt"], r["datePrecision"], r["dateEvidencePath"]) == (None, "UNKNOWN", None)
    assert r["title"] == "진료 방문"


def test_date_only_value_keeps_date_precision():
    obs = {"resourceType": "Observation", "id": "O1", "status": "final", "subject": {"reference": "Patient/P1"},
           "effectiveDateTime": "2026-02-01"}
    r = get(valid(parse_bundle(bundle(PATIENT, obs))), "Observation/O1")
    assert (r["occurredAt"], r["datePrecision"], r["dateEvidencePath"]) == ("2026-02-01", "DATE", "Observation.effectiveDateTime")


def test_dangling_reference_makes_link_status_partial():
    e = enc("E1", diagnosis=[{"condition": {"reference": "Condition/NOPE"}}])
    r = get(valid(parse_bundle(bundle(PATIENT, e))), "Encounter/E1")
    assert r["linkageStatus"] == "PARTIAL"
    assert r["unresolvedReferences"] == ["Condition/NOPE"]
    assert r["linkedResourceKeys"] == [f"{DEFAULT_SOURCE}|Patient/P1"]


def test_conditional_reference_is_ignored():
    e = enc("E1", serviceProvider={"reference": "Organization?identifier=x|1"})
    r = get(valid(parse_bundle(bundle(PATIENT, e))), "Encounter/E1")
    assert r["linkageStatus"] == "RESOLVED" and r["unresolvedReferences"] == []
    assert r["organizationResourceKey"] is None and r["organizationNameSource"] == "UNKNOWN"


def test_encounter_links_both_directions():
    cond = {"resourceType": "Condition", "id": "C1", "subject": {"reference": "Patient/P1"}}
    obs = {"resourceType": "Observation", "id": "O1", "status": "final", "subject": {"reference": "Patient/P1"},
           "encounter": {"reference": "Encounter/E1"}}
    e = enc("E1", diagnosis=[{"condition": {"reference": "Condition/C1"}}])  # Encounter → Condition (Condition에는 encounter 없음)
    r = get(valid(parse_bundle(bundle(PATIENT, cond, obs, e))), "Encounter/E1")
    assert [k.split("|")[1] for k in r["linkedResourceKeys"]] == ["Condition/C1", "Observation/O1", "Patient/P1"]
    assert {"path": "Observation.encounter (reverse)", "reference": "Encounter/E1",
            "targetResourceKey": f"{DEFAULT_SOURCE}|Observation/O1"} in r["linkEvidence"]


def test_other_patients_resource_is_not_linked():
    other_cond = {"resourceType": "Condition", "id": "CX", "subject": {"reference": "Patient/P2"}}
    e = enc("E1", diagnosis=[{"condition": {"reference": "Condition/CX"}}])
    r = get(valid(parse_bundle(bundle(PATIENT, OTHER, other_cond, e))), "Encounter/E1")
    assert all("Condition/CX" not in k for k in r["linkedResourceKeys"])
    assert r["linkageStatus"] == "RESOLVED"  # 다른 환자 리소스는 끊긴 참조가 아니라 '연결하지 않는 것'


def test_resources_without_patient_are_not_records():
    org = {"resourceType": "Organization", "id": "O1", "name": "병원"}
    res = parse_bundle(bundle(PATIENT, org))
    assert [r["resourceType"] for r in res.records] == ["Patient"]
    assert "carecompass/v1/data/synthetic/fhir/resources/Organization-O1.json" in res.resources


def test_organization_name_and_shared_reference():
    org = {"resourceType": "Organization", "id": "O1", "name": "한빛병원"}
    r = get(valid(parse_bundle(bundle(PATIENT, org, enc("E1", serviceProvider={"reference": "Organization/O1"})))),
            "Encounter/E1")
    assert (r["organizationName"], r["organizationNameSource"]) == ("한빛병원", "FHIR")
    assert r["sharedReferences"] == [{"resourceKey": f"{DEFAULT_SOURCE}|Organization/O1", "resourceType": "Organization",
                                      "s3Key": "carecompass/v1/data/synthetic/fhir/resources/Organization-O1.json"}]


def test_real_data_uses_real_prefix():
    res = parse_bundle(bundle(PATIENT, enc("E1")), is_synthetic=False)
    assert all(r["s3Key"].startswith("carecompass/v1/data/fhir/") and r["isSynthetic"] is False for r in res.records)


def test_one_bad_resource_does_not_stop_the_rest(monkeypatch):
    import fhir_parser

    real = fhir_parser._occurred
    monkeypatch.setattr(fhir_parser, "_occurred", lambda r: (_ for _ in ()).throw(ValueError("boom")) if r["id"] == "E1" else real(r))
    res = parse_bundle(bundle(PATIENT, enc("E1"), enc("E2")))
    assert {r["resourceKey"].split("/")[-1] for r in res.records} == {"P1", "E2"}
    assert len(res.errors) == 1 and res.errors[0]["resource"] == "Encounter/E1"


def test_completed_careplan_is_not_a_followup():
    plan = {"resourceType": "CarePlan", "id": "CP1", "status": "completed", "subject": {"reference": "Patient/P1"}}
    assert parse_bundle(bundle(PATIENT, plan)).followups == []


def test_appointment_without_start_stays_undated():
    appt = {"resourceType": "Appointment", "id": "A1", "status": "proposed",
            "participant": [{"actor": {"reference": "Patient/P1"}, "status": "needs-action"}]}
    (row,) = valid(parse_bundle(bundle(PATIENT, appt))).followups
    assert (row["sourceStatus"], row["scheduledAt"], row["scheduledDate"], row["datePrecision"], row["timingEvidence"]) == (
        "proposed", None, None, "UNKNOWN", None)
    assert row["followupKey"] == followup_key(f"{DEFAULT_SOURCE}|Appointment/A1", "$")


def test_date_only_appointment_uses_scheduled_date():
    appt = {"resourceType": "Appointment", "id": "A1", "status": "booked", "start": "2026-12-01",
            "participant": [{"actor": {"reference": "Patient/P1"}, "status": "accepted"}]}
    (row,) = valid(parse_bundle(bundle(PATIENT, appt))).followups
    assert (row["scheduledAt"], row["scheduledDate"], row["datePrecision"]) == (None, "2026-12-01", "DATE")


def test_parsing_twice_is_identical_and_deduplicates(info):
    raw = json.loads(ALL.read_text(encoding="utf-8-sig"))
    first, second = parse_bundle(raw, encounter_info=info), parse_bundle(raw, encounter_info=info)
    assert first.records == second.records and first.followups == second.followups
    assert len(index_by_key(first.records + second.records)) == 105


def test_serialization_is_stable():
    res = {"resourceType": "Patient", "id": "P1", "name": [{"text": "김OO"}]}
    assert serialize_resource(res) == serialize_resource(json.loads(json.dumps(res)))


# ===================================================================== 환자 간 혼입·누락 방지 (11/6 추가 검증)

PB = {"resourceType": "Patient", "id": "PB"}


def appt(id_, patient, status="booked", start="2026-12-01T10:00:00+09:00", **extra):
    res = {"resourceType": "Appointment", "id": id_, "status": status, "start": start,
           "participant": [{"actor": {"reference": f"Patient/{patient}"}, "status": "accepted"}]}
    res.update(extra)
    return res


def careplan(id_, patient, appointment_ref=None, title="계획", status="active"):
    res = {"resourceType": "CarePlan", "id": id_, "status": status, "intent": "plan", "title": title,
           "subject": {"reference": f"Patient/{patient}"}}
    if appointment_ref:
        res["activity"] = [{"reference": {"reference": appointment_ref}}]
    return res


def rows_of(result, patient):
    return [f for f in result.followups if f["patientKey"] == patient]


def test_careplan_pointing_to_other_patients_appointment_is_not_linked():
    """API 팀 재현 사례: P1의 CarePlan이 PB의 Appointment를 참조."""
    res = valid(parse_bundle(bundle(PATIENT, PB, careplan("CPA", "P1", "Appointment/APB", title="P1 계획"),
                                    appt("APB", "PB"))))
    (b_row,) = rows_of(res, "PB")
    assert b_row["kind"] == "APPOINTMENT"
    assert b_row["relatedSourceResourceKeys"] == []          # 다른 환자 계획이 붙지 않는다
    assert b_row["title"] != "P1 계획"                        # 다른 환자 계획 제목이 새지 않는다
    assert b_row["specialtyCode"] == "UNKNOWN"
    (a_row,) = rows_of(res, "P1")                           # 원래 환자의 계획은 누락되지 않는다
    assert (a_row["kind"], a_row["sourceResourceKey"], a_row["title"]) == (
        "CARE_PLAN", f"{DEFAULT_SOURCE}|CarePlan/CPA", "P1 계획")


def test_other_patients_careplan_specialty_is_not_borrowed(info):
    """다른 환자 계획이 가진 진료과 근거를 예약이 가져다 쓰지 않는다."""
    enc_a = enc("EA")
    plan = careplan("CPA", "P1", "Appointment/APB")
    plan["encounter"] = {"reference": "Encounter/EA"}
    encounter_info = {"Encounter/EA": {"specialtyCode": "05", "specialtySource": "DEMO_MAPPING",
                                       "specialtyName": "정형외과", "summary": "P1 진료"}}
    res = valid(parse_bundle(bundle(PATIENT, PB, enc_a, plan, appt("APB", "PB")), encounter_info=encounter_info))
    (b_row,) = rows_of(res, "PB")
    assert (b_row["specialtyCode"], b_row["clinicKey"]) == ("UNKNOWN", None)
    (a_row,) = rows_of(res, "P1")
    assert a_row["specialtyCode"] == "05"


def test_same_patient_careplan_and_appointment_still_merge():
    res = valid(parse_bundle(bundle(PATIENT, careplan("CP1", "P1", "Appointment/AP1", title="재진 계획"),
                                    appt("AP1", "P1"))))
    (row,) = res.followups
    assert row["kind"] == "APPOINTMENT" and row["relatedSourceResourceKeys"] == [f"{DEFAULT_SOURCE}|CarePlan/CP1"]
    assert row["title"] == "재진 계획"


@pytest.mark.parametrize("status", ["cancelled", "noshow", "entered-in-error"])
def test_careplan_linked_to_inactive_appointment_keeps_its_own_row(status):
    """취소된 예약에 묶으면 '다음 진료'에서 계획까지 사라지므로 계획 행을 따로 남긴다."""
    res = valid(parse_bundle(bundle(PATIENT, careplan("CP1", "P1", "Appointment/AP1", title="재방문 계획"),
                                    appt("AP1", "P1", status=status))))
    kinds = sorted((f["kind"], f["sourceStatus"]) for f in res.followups)
    assert kinds == [("APPOINTMENT", status), ("CARE_PLAN", "active")]
    assert all(f["relatedSourceResourceKeys"] == [] for f in res.followups)


def test_appointment_with_two_patients_is_not_assigned_to_either():
    shared = {"resourceType": "Appointment", "id": "AP2", "status": "booked", "start": "2026-12-01T10:00:00+09:00",
              "participant": [{"actor": {"reference": "Patient/P1"}}, {"actor": {"reference": "Patient/PB"}}]}
    res = parse_bundle(bundle(PATIENT, PB, shared))
    assert all(r["resourceType"] == "Patient" for r in res.records) and res.followups == []
    assert [(e["resource"], e["code"]) for e in res.errors] == [("Appointment/AP2", "AMBIGUOUS_OWNER")]


def test_resource_whose_patient_is_missing_is_reported():
    obs = {"resourceType": "Observation", "id": "O1", "status": "final", "subject": {"reference": "Patient/NOPE"}}
    res = parse_bundle(bundle(PATIENT, obs))
    assert [r["resourceType"] for r in res.records] == ["Patient"]
    assert [(e["resource"], e["code"]) for e in res.errors] == [("Observation/O1", "OWNER_UNRESOLVED")]


def test_duplicate_id_with_different_content_is_not_used():
    e1, e2 = enc("E1", start="2026-01-01"), enc("E1", patient="PB", start="2026-02-01")
    res = parse_bundle(bundle(PATIENT, PB, e1, e2))
    assert all(r["resourceType"] == "Patient" for r in res.records)   # 어느 쪽도 임의로 고르지 않는다
    assert ("Encounter/E1", "DUPLICATE_RESOURCE_ID") in [(e["resource"], e["code"]) for e in res.errors]
    assert not any(k.endswith("Encounter-E1.json") for k in res.resources)


def test_identical_duplicate_is_fine():
    e1 = enc("E1")
    res = valid(parse_bundle(bundle(PATIENT, e1, json.loads(json.dumps(e1)))))
    assert sum(r["resourceType"] == "Encounter" for r in res.records) == 1 and not res.errors


def test_conflicting_full_url_is_not_resolved():
    obs = {"resourceType": "Observation", "id": "O9", "status": "final", "subject": {"reference": "urn:uuid:p"}}
    raw = {"resourceType": "Bundle", "type": "collection", "entry": [
        {"fullUrl": "urn:uuid:p", "resource": PATIENT}, {"fullUrl": "urn:uuid:p", "resource": PB},
        {"fullUrl": "urn:uuid:o", "resource": obs}]}
    res = parse_bundle(raw)
    assert not any(r["resourceType"] == "Observation" for r in res.records)   # 엉뚱한 환자에게 붙지 않는다
    codes = {(e["resource"], e["code"]) for e in res.errors}
    assert ("urn:uuid:p", "DUPLICATE_FULL_URL") in codes and ("Observation/O9", "OWNER_UNRESOLVED") in codes


@pytest.mark.parametrize("bad, expected", [
    ({"resourceType": "Observation", "id": "O_bad!", "status": "final"}, "FHIR id"),
    ({"resourceType": "Observation", "id": "O2", "status": "done"}, "상태값"),
    ({"resourceType": "Encounter", "id": "E9", "status": None}, "상태값"),
])
def test_rows_the_db_would_reject_are_reported_not_sent(bad, expected):
    """DB는 한 행만 틀려도 요청 전체(최대 50행)를 거부하므로, 그 행만 빼고 errors에 남긴다."""
    bad = {**bad, "subject": {"reference": "Patient/P1"}}
    good = {"resourceType": "Observation", "id": "OK1", "status": "final", "subject": {"reference": "Patient/P1"}}
    res = valid(parse_bundle(bundle(PATIENT, bad, good)))
    assert sorted(r["resourceId"] for r in res.records) == ["OK1", "P1"]
    (err,) = res.errors
    assert err["code"] == "INVALID_FOR_DB" and expected in err["error"]


def test_no_followup_or_record_ever_crosses_patients(result):
    """샘플 전체에서 어떤 행도 다른 환자의 키를 담지 않는다."""
    owner = {r["resourceKey"]: r["patientKey"] for r in result.records}
    for f in result.followups:
        assert owner[f["sourceResourceKey"]] == f["patientKey"]
        assert all(owner[k] == f["patientKey"] for k in f["relatedSourceResourceKeys"])


# ===================================================================== 환자 판별 엄격화 (API 팀 파서와 통합, 11/6)

@pytest.mark.parametrize("name, res, expected_owner", [
    ("확인 안 되는 다른 환자가 함께 참여한 예약",
     {"resourceType": "Appointment", "id": "AP1", "status": "booked", "start": "2026-12-01",
      "participant": [{"actor": {"reference": "Patient/P1"}}, {"actor": {"reference": "Patient/NOPE"}}]}, None),
    ("subject의 종류 표시(type)가 실제와 다름",
     {"resourceType": "Observation", "id": "O1", "status": "final",
      "subject": {"reference": "Patient/P1", "type": "Group"}}, None),
    ("subject 칸이 있지만 참조가 비어 있음",
     {"resourceType": "Observation", "id": "O3", "status": "final", "subject": "Patient/P1"}, None),
    ("번들에 없는 의사가 함께 참여한 예약은 환자 것으로 본다",
     {"resourceType": "Appointment", "id": "AP2", "status": "booked", "start": "2026-12-01",
      "participant": [{"actor": {"reference": "Patient/P1"}}, {"actor": {"reference": "Practitioner/DR9"}}]}, "P1"),
    ("subject가 환자가 아닌 리소스를 가리킴",
     {"resourceType": "Observation", "id": "O2", "status": "final",
      "subject": {"reference": "Practitioner/DR9"}}, None),
])
def test_ownership_is_decided_only_when_every_patient_reference_is_confirmed(name, res, expected_owner):
    """환자 참조가 하나라도 확인되지 않으면 어느 환자에게도 붙이지 않는다. 환자가 아닌 게 분명한 참조(의사 등)는 무시."""
    result = parse_bundle(bundle(PATIENT, res))
    rows = [r for r in result.records if r["resourceId"] == res["id"]]
    if expected_owner is None:
        assert rows == [], name
        assert ("%s/%s" % (res["resourceType"], res["id"]), "OWNER_UNRESOLVED") in [
            (e["resource"], e.get("code")) for e in result.errors], name
    else:
        assert [r["patientKey"] for r in rows] == [expected_owner], name
        valid(result)
