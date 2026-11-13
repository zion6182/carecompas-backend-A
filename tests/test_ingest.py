"""적재(S3 저장 → DB 쓰기) 테스트. AWS 호출 없이 가짜 S3·가짜 쓰기 Lambda로 확인한다."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from db_writer import (DbWriteError, DbWriter, JsonFileRequestIdStore, TransportError, access_token_auth,
                       source_verification)
from fake_aws import FakeS3, FakeWriteLambda, claims
from fhir_validate import NotConfiguredValidator, PrecomputedResult, ValidatorNotConfigured, parse_operation_outcome
from ingest import IngestError, ingest_patient
from s3_store import S3ObjectStore, S3StoreError
from specialty_map import load_encounter_info

DATA = Path(__file__).resolve().parents[1] / "sample_data" / "carecompass_6patients"
A1_BUNDLE = (DATA / "fhir" / "bundles" / "A1.bundle.json").read_bytes()
BUCKET = "carecompass-data-test"


class OkValidator:
    def __init__(self):
        self.calls = 0

    def validate(self, bundle_bytes, name):
        from fhir_validate import ValidationReport

        self.calls += 1
        return ValidationReport(True, "test")


@pytest.fixture(scope="module")
def info():
    return load_encounter_info(DATA / "query_models" / "encounter_timeline.json")


@pytest.fixture
def aws():
    s3 = FakeS3()
    db = FakeWriteLambda({"tok-A1": claims("A1", "A1"), "tok-B1": claims("B1", "B1"),
                          "tok-person": claims("person-001", "A1")})
    return s3, db


def run(aws, info, token="tok-A1", patient_key="A1", sleep=lambda s: None, **kwargs):
    s3, db = aws
    writer = DbWriter(db, access_token_auth(lambda: token), sleep=sleep)
    return ingest_patient(A1_BUNDLE, fhir_patient_id="A1", patient_key=patient_key,
                          validator=kwargs.pop("validator", OkValidator()), store=S3ObjectStore(s3, BUCKET),
                          writer=writer, encounter_info=info, **kwargs)


# ===================================================================== 정상 흐름

def test_full_ingest_of_patient_a1(aws, info):
    s3, db = aws
    report = run(aws, info)
    assert (report.records, report.followups) == (20, 1)              # A1: Patient+진료8+진단1+검사8+예약+계획 / 예약 1
    assert report.state_version_before == 0 and report.state_version_after == len(report.batches) == 1
    assert len(report.s3.uploaded) == report.s3_objects == len(s3.puts)  # 이 환자 원문 + 병원 원문
    assert len([k for k in db.records if k[0] == "A1"]) == 20 and len(db.followups) == 1
    for (_, key), data in s3.objects.items():                         # S3 바이트 = DB 행의 sourceSha256
        rec = next((r for r in db.records.values() if r["s3Key"] == key), None)
        if rec:
            assert hashlib.sha256(data).hexdigest() == rec["sourceSha256"]
    assert db.calls[0]["apiVersion"] == "2" and db.calls[0]["auth"] == {"accessToken": "tok-A1"}


def test_second_run_changes_nothing(aws, info):
    s3, db = aws
    run(aws, info)
    puts = len(s3.puts)
    report = run(aws, info)
    assert len(s3.puts) == puts and len(report.s3.unchanged) == report.s3_objects  # S3 재업로드 없음
    (batch,) = report.batches
    assert batch["records"]["created"] == 0 and batch["records"]["unchanged"] == 20


def test_small_batches_chain_state_versions(aws, info):
    report = run(aws, info, batch_size=6)
    assert len(report.batches) == 4
    assert [b["stateVersion"] for b in report.batches] == [1, 2, 3, 4]


def test_patient_key_is_replaced_with_linked_key(aws, info):
    _, db = aws
    report = run(aws, info, token="tok-person", patient_key="person-001")
    assert report.state_version_after == 1
    assert {k[0] for k in db.records} == {"person-001"}
    assert all(r["patientKey"] == "person-001" for r in db.records.values())


def test_dry_run_sends_nothing(aws, info):
    s3, db = aws
    report = run(aws, info, dry_run=True)
    assert s3.puts == [] and db.calls == []
    assert report.batches == [{"records": 20, "followups": 1}]
    assert set(report.s3_states.values()) == {"missing"}


# ===================================================================== 재시도·충돌

def lose_first_upsert_response(db):
    """upsert는 DB에 저장됐는데 응답만 사라지는 상황을 한 번 만든다. 사라진 요청의 requestId를 돌려준다."""
    real = db._handle
    lost: list[str] = []

    def handle(event):
        response = real(event)
        if event["operation"] == "upsert_records" and not lost:
            lost.append(event["requestId"])
            raise TransportError("response lost")
        return response

    db._handle = handle
    return lost


def test_lost_response_is_retried_with_same_request_id(aws, info):
    _, db = aws
    lost = lose_first_upsert_response(db)
    report = run(aws, info)
    upserts = [c for c in db.calls if c["operation"] == "upsert_records"]
    assert len(upserts) == 2 and upserts[0]["requestId"] == upserts[1]["requestId"] == lost[0]
    assert report.batches[0]["idempotentReplay"] is True
    assert db.version["A1"] == 1                                       # 두 번 저장되지 않음


def test_transport_failure_before_processing_is_retried(aws, info):
    _, db = aws
    db.fail_before = 1
    assert run(aws, info).state_version_after == 1


def test_stale_state_conflict_is_resolved_with_new_request_id(aws, info):
    _, db = aws
    real = db._handle
    bumped = []

    def bump_once(event):
        if event["operation"] == "upsert_records" and not bumped:
            bumped.append(1)
            db.bump("A1")  # 다른 적재가 먼저 끝남
        return real(event)

    db._handle = bump_once
    report = run(aws, info)
    upserts = [c for c in db.calls if c["operation"] == "upsert_records"]
    assert len(upserts) == 2 and upserts[0]["requestId"] != upserts[1]["requestId"]
    assert upserts[1]["params"]["expectedStateVersion"] == 1
    assert report.batches[0]["conflictsResolved"] == 1 and report.state_version_after == 2


def test_conflict_without_state_change_stops(aws, info):
    _, db = aws
    real = db._handle
    db._handle = lambda e: (db._err(e, "CONFLICT", "requestId already used") if e["operation"] == "upsert_records"
                            else real(e))
    with pytest.raises(DbWriteError) as exc:
        run(aws, info)
    assert exc.value.code == "CONFLICT"


def test_request_id_survives_restart(tmp_path, aws, info):
    """응답을 받기 전에 프로그램이 죽어도, 다시 실행하면 저장된 requestId로 같은 요청을 보낸다(중복 저장 없음)."""
    s3, db = aws
    store_path = tmp_path / "request_ids.json"

    def writer(retries):
        return DbWriter(db, access_token_auth(lambda: "tok-A1"), request_ids=JsonFileRequestIdStore(store_path),
                        max_retries=retries, sleep=lambda s: None)

    lost = lose_first_upsert_response(db)
    with pytest.raises(TransportError):                                 # 1차 실행: 재시도 없이 죽음
        ingest_patient(A1_BUNDLE, fhir_patient_id="A1", patient_key="A1", validator=OkValidator(),
                       store=S3ObjectStore(s3, BUCKET), writer=writer(0), encounter_info=info)
    assert db.version["A1"] == 1                                       # DB에는 이미 저장됨
    report = ingest_patient(A1_BUNDLE, fhir_patient_id="A1", patient_key="A1", validator=OkValidator(),
                            store=S3ObjectStore(s3, BUCKET), writer=writer(3), encounter_info=info)  # 재실행
    assert report.batches[0]["requestId"] == lost[0] and report.batches[0]["idempotentReplay"] is True
    assert db.version["A1"] == 1                                       # 두 번 저장되지 않음


# ===================================================================== 멈춰야 하는 경우

def test_existing_different_s3_object_is_not_overwritten(aws, info):
    s3, db = aws
    key = "carecompass/v1/data/synthetic/fhir/resources/Patient-A1.json"
    s3.objects[(BUCKET, key)] = b'{"resourceType":"Patient","id":"A1","note":"already here"}'
    with pytest.raises(S3StoreError) as exc:
        run(aws, info)
    assert exc.value.code == "OBJECT_CONTENT_DIFFERS"
    assert s3.puts == [] and db.calls == []                            # S3도 DB도 건드리지 않음
    assert s3.objects[(BUCKET, key)].endswith(b'"already here"}')


def test_overwrite_only_when_explicitly_allowed(aws, info):
    s3, _ = aws
    key = "carecompass/v1/data/synthetic/fhir/resources/Patient-A1.json"
    s3.objects[(BUCKET, key)] = b"{}"
    report = run(aws, info, allow_overwrite=True)
    assert report.s3.overwritten == [key]


def test_validator_must_be_configured(aws, info):
    s3, db = aws
    with pytest.raises(ValidatorNotConfigured):
        run(aws, info, validator=NotConfiguredValidator())
    assert s3.puts == [] and db.calls == []


def test_failed_validation_stops_before_s3(aws, info):
    s3, db = aws
    with pytest.raises(IngestError) as exc:
        run(aws, info, validator=PrecomputedResult(ok=False, validated_sha256=hashlib.sha256(A1_BUNDLE).hexdigest()))
    assert exc.value.step == "validate" and s3.puts == [] and db.calls == []


def test_precomputed_result_must_match_the_same_file(aws, info):
    with pytest.raises(IngestError):
        run(aws, info, validator=PrecomputedResult(ok=True, validated_sha256="0" * 64))
    ok = PrecomputedResult(ok=True, validated_sha256=hashlib.sha256(A1_BUNDLE).hexdigest())
    assert run(aws, info, validator=ok).state_version_after == 1


def test_token_linked_to_another_patient_is_rejected(aws, info):
    """B1에 연결된 토큰으로 A1 기록을 쓰려 하면 DB가 거부한다(행의 patientKey가 토큰과 다름)."""
    with pytest.raises(DbWriteError) as exc:
        run(aws, info, token="tok-B1", patient_key="A1")
    assert exc.value.code == "CROSS_PATIENT_REFERENCE"


def test_unknown_token_is_rejected_and_token_not_in_error(aws, info):
    with pytest.raises(DbWriteError) as exc:
        run(aws, info, token="secret-token-value")
    assert exc.value.code == "UNAUTHORIZED" and "secret-token-value" not in str(exc.value)


def test_missing_patient_in_bundle_stops(aws, info):
    s3, db = aws
    writer = DbWriter(db, access_token_auth(lambda: "tok-A1"), sleep=lambda s: None)
    with pytest.raises(IngestError) as exc:
        ingest_patient(A1_BUNDLE, fhir_patient_id="ZZ", patient_key="A1", validator=OkValidator(),
                       store=S3ObjectStore(s3, BUCKET), writer=writer, encounter_info=info)
    assert exc.value.step == "parse" and db.calls == []


# ===================================================================== 부품 단위

@pytest.mark.parametrize("key", [
    "fhir/resources/Patient-A1.json",                                      # prefix 밖
    "carecompass/v1/data/synthetic/fhir/../secret.json",
    "carecompass/v1/data/synthetic/fhir/incoming/x.json",
    "carecompass/v1/data/synthetic/fhir/a?b.json",
])
def test_s3_keys_outside_allowed_prefix_are_refused(key):
    with pytest.raises(S3StoreError) as exc:
        S3ObjectStore(FakeS3(), BUCKET).put_verified({key: b"{}"})
    assert exc.value.code == "KEY_NOT_ALLOWED"


def test_readback_mismatch_is_detected():
    class Corrupting(FakeS3):
        def put_object(self, *, Bucket, Key, Body, **kwargs):
            return super().put_object(Bucket=Bucket, Key=Key, Body=Body + b" ")

    key = "carecompass/v1/data/synthetic/fhir/resources/Patient-X.json"
    with pytest.raises(S3StoreError) as exc:
        S3ObjectStore(Corrupting(), BUCKET).put_verified({key: b"{}"})
    assert exc.value.code == "READBACK_MISMATCH"


def test_source_verification_requires_both_checks():
    with pytest.raises(ValueError):
        source_verification(validated=True, objects_verified=False)
    assert source_verification(validated=True, objects_verified=True) == {
        "fhirVersion": "4.0.1", "validated": True, "objectsVerified": True}


def test_token_is_fetched_for_every_request(aws, info):
    tokens = iter(["tok-A1"] * 10)
    calls = []

    def get_token():
        calls.append(1)
        return next(tokens)

    s3, db = aws
    ingest_patient(A1_BUNDLE, fhir_patient_id="A1", patient_key="A1", validator=OkValidator(),
                   store=S3ObjectStore(s3, BUCKET), writer=DbWriter(db, access_token_auth(get_token)),
                   encounter_info=info)
    assert len(calls) == len(db.calls)  # 만료 전 토큰을 매번 다시 받아 쓸 수 있게


def test_parse_operation_outcome_counts_errors():
    outcome = {"resourceType": "OperationOutcome", "issue": [
        {"severity": "error", "code": "structure", "expression": ["Patient.birthDate"]},
        {"severity": "warning", "code": "informational"},
        {"severity": "information", "code": "informational"}]}
    report = parse_operation_outcome(outcome)
    assert (report.ok, report.errors, report.warnings) == (False, 1, 1)
    assert parse_operation_outcome({"resourceType": "OperationOutcome", "issue": []}).ok is True


@pytest.mark.parametrize("header", ["Bearer tok-A1", "bearer tok-A1", "  Bearer   tok-A1  ", "tok-A1"])
def test_bearer_prefix_is_removed_before_sending(aws, info, header):
    """DB v2 명세: auth.accessToken에는 'Bearer ' 없이 JWT 원문만 넣는다."""
    _, db = aws
    run(aws, info, token=header)
    assert {c["auth"]["accessToken"] for c in db.calls} == {"tok-A1"}


@pytest.mark.parametrize("bad", ["", "Bearer ", "Bearer a b"])
def test_malformed_access_token_is_refused(aws, info, bad):
    _, db = aws
    with pytest.raises(ValueError):
        run(aws, info, token=bad)
    assert db.calls == []
