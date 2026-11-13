"""적재 테스트용 가짜 S3 / 가짜 DB 쓰기 Lambda (AWS 호출 없음).

FakeWriteLambda는 DB 팀 쓰기 Lambda의 핵심 규칙만 흉내 낸다:
- apiVersion "2" + auth.accessToken 으로 환자 범위를 정한다(토큰 → 연결된 환자).
- 행 검증은 DB 팀 model_validation.py(tests/db_contract, 수정 없는 사본)를 그대로 쓴다.
- expectedStateVersion 불일치 → CONFLICT, 같은 requestId + 같은 내용 → 이전 결과 replay,
  같은 requestId + 다른 내용 → CONFLICT, 성공하면 stateVersion +1.
- Patient 행이 이미 있거나 같은 요청에 있어야 하고, 일정의 원본 레코드가 있어야 한다.
실제 Lambda와 다른 부분(트랜잭션 크기·조건부 쓰기 세부)은 2단계 실연결에서 확인한다.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "db_contract"))
import model_validation as db_contract  # noqa: E402

from db_writer import TransportError  # noqa: E402


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts: list[str] = []

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise KeyError(Key)
        return {"Body": _Body(self.objects[(Bucket, Key)])}

    def put_object(self, *, Bucket, Key, Body, **kwargs):
        self.objects[(Bucket, Key)] = bytes(Body)
        self.puts.append(Key)
        return {}


def claims(patient_key: str, fhir_patient_id: str, source: str = "carecompass-demo-v1") -> dict:
    return {"patientKey": patient_key, "patientResourceKey": f"{source}|Patient/{fhir_patient_id}",
            "sourceSystems": [source], "isSynthetic": True, "datasetId": source}


def _hash(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class FakeWriteLambda:
    def __init__(self, tokens: dict[str, dict]):
        self.tokens = tokens                    # accessToken → 연결된 환자 claims
        self.version: dict[str, int] = {}       # patientKey → stateVersion
        self.records: dict[tuple[str, str], dict] = {}
        self.followups: dict[tuple[str, str], dict] = {}
        self.receipts: dict[str, dict] = {}
        self.calls: list[dict] = []
        self.fail_before = 0                    # 처리 전에 통신 오류
        self.lose_response = 0                  # 처리는 됐는데 응답이 사라짐

    def bump(self, patient_key: str) -> None:
        """다른 적재 요청이 먼저 끝난 상황."""
        self.version[patient_key] = self.version.get(patient_key, 0) + 1

    @staticmethod
    def _err(event, code, message, retryable=False):
        return {"apiVersion": "2", "requestId": event.get("requestId"), "ok": False,
                "error": {"code": code, "message": message, "retryable": retryable}}

    def __call__(self, event: dict) -> dict:
        self.calls.append(event)
        if self.fail_before:
            self.fail_before -= 1
            raise TransportError("timeout")
        response = self._handle(event)
        if self.lose_response:
            self.lose_response -= 1
            raise TransportError("response lost")
        return response

    def _handle(self, event: dict) -> dict:
        if set(event) - {"apiVersion", "operation", "requestId", "auth", "params"}:
            return self._err(event, "INVALID_REQUEST", "Unknown request fields")
        if event.get("apiVersion") != "2":
            return self._err(event, "UNSUPPORTED_API_VERSION", "apiVersion must be 2")
        token = (event.get("auth") or {}).get("accessToken")
        claim = self.tokens.get(token)
        if claim is None:
            return self._err(event, "UNAUTHORIZED", "invalid access token")
        pk = claim["patientKey"]
        if event["operation"] == "get_write_state":
            return {"apiVersion": "2", "requestId": event["requestId"], "ok": True,
                    "data": {"patientKey": pk, "stateVersion": self.version.get(pk, 0)}}
        if event["operation"] != "upsert_records":
            return self._err(event, "UNKNOWN_OPERATION", event["operation"])
        p = event["params"]
        try:
            db_contract.validate_source_verification(p["sourceVerification"])
            recs = [db_contract.validate_record_model(r, claim, "test-bucket") for r in p["records"]]
            fus = [db_contract.validate_followup_model(f, claim) for f in p["followups"]]
        except db_contract.ApiError as exc:
            return self._err(event, exc.code, str(exc))
        payload = _hash({"records": p["records"], "followups": p["followups"], "v": p["sourceVerification"]})
        receipt = self.receipts.get(event["requestId"])
        if receipt is not None:
            if receipt["payload"] != payload or receipt["expected"] != p["expectedStateVersion"]:
                return self._err(event, "CONFLICT", "requestId was already used with a different payload")
            return {"apiVersion": "2", "requestId": event["requestId"], "ok": True,
                    "data": {**receipt["result"], "idempotentReplay": True}}
        if p["expectedStateVersion"] != self.version.get(pk, 0):
            return self._err(event, "CONFLICT", "expectedStateVersion is stale")
        known = {k[1] for k in self.records if k[0] == pk} | {r["resourceKey"] for r in recs}
        if claim["patientResourceKey"] not in known:
            return self._err(event, "VALIDATION_ERROR", "Patient row must exist or be in the batch")
        if any(f["sourceResourceKey"] not in known for f in fus):
            return self._err(event, "VALIDATION_ERROR", "followup source record missing")
        counts = {"records": {"accepted": len(recs), "created": 0, "updated": 0, "unchanged": 0},
                  "followups": {"accepted": len(fus), "created": 0, "updated": 0, "unchanged": 0}}
        for table, rows, key, name in ((self.records, recs, "resourceKey", "records"),
                                       (self.followups, fus, "followupKey", "followups")):
            for row in rows:
                old = table.get((pk, row[key]))
                counts[name]["created" if old is None else ("unchanged" if old == row else "updated")] += 1
                table[(pk, row[key])] = row
        self.version[pk] = self.version.get(pk, 0) + 1
        result = {"requestId": event["requestId"], "stateVersion": self.version[pk], **counts, "idempotentReplay": False}
        self.receipts[event["requestId"]] = {"payload": payload, "expected": p["expectedStateVersion"], "result": result}
        return {"apiVersion": "2", "requestId": event["requestId"], "ok": True, "data": result}
