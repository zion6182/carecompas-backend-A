"""DB 쓰기 Lambda(carecompass-db-ingest:live) 호출 (적재 1단계: 호출 방법·인증을 주입받는다).

요청 봉투 (DB 팀 2026-11-06 공지 기준 apiVersion "2"):
  {"apiVersion": "2", "operation": ..., "requestId": <UUID>, "auth": {"accessToken": "<Cognito access token>"}, "params": {...}}
인증 방식이 아직 정해지지 않은 부분(환자 토큰이 없는 의료진 입력·자동 적재 등)이 있어
`auth` 부분은 AuthProvider로 바꿔 끼울 수 있게 했다.

재시도 규칙 (DB 명세 README 4절)
- 통신 오류·retryable 오류: **같은 requestId + 같은 payload**로 다시 보낸다(서버가 이전 결과를 replay).
- CONFLICT: 상태를 다시 읽어 stateVersion이 바뀌었으면, 새 expectedStateVersion + **새 requestId**로 다시 만든다.
  상태가 그대로인데 CONFLICT면(같은 requestId를 다른 내용에 쓴 경우 등) 멈춘다.
- Invoke가 성공해도 응답의 ok / error를 반드시 확인한다.
토큰 값은 예외 메시지·로그에 넣지 않는다.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

Transport = Callable[[dict], dict]       # 요청 이벤트 → 응답 JSON (Lambda Invoke 결과)
AuthProvider = Callable[[], dict]        # 요청마다 호출. 예: lambda: {"auth": {"accessToken": token()}}

FHIR_VERSION = "4.0.1"


class DbWriteError(Exception):
    def __init__(self, code: str, message: str, retryable: bool = False, request_id: str | None = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.retryable = retryable
        self.request_id = request_id


class TransportError(Exception):
    """응답을 받지 못한 경우(타임아웃·연결 오류). 같은 requestId로 재시도해도 안전하다."""


def raw_access_token(value: str) -> str:
    """HTTP Authorization 헤더 값('Bearer eyJ...')이 들어와도 JWT 원문만 남긴다.
    DB v2 명세: auth.accessToken에는 Bearer 접두사 없이 JWT 원문을 넣는다."""
    if not isinstance(value, str):
        raise ValueError("access token must be a string")
    parts = value.split()
    if parts and parts[0].lower() == "bearer":
        parts = parts[1:]
    token = parts[0] if len(parts) == 1 else ""
    if not token:
        raise ValueError("access token is empty or malformed")
    return token


def access_token_auth(get_token: Callable[[], str]) -> AuthProvider:
    """apiVersion 2: Cognito access token. get_token은 요청마다 불려서 만료 전 토큰을 돌려줘야 한다."""
    return lambda: {"auth": {"accessToken": raw_access_token(get_token())}}


def source_verification(*, validated: bool, objects_verified: bool) -> dict:
    """실제로 검증을 마친 경우에만 True가 들어간 attestation을 만든다."""
    if not (validated and objects_verified):
        raise ValueError("FHIR 검증과 S3 원문 확인이 모두 끝나야 sourceVerification을 만들 수 있습니다")
    return {"fhirVersion": FHIR_VERSION, "validated": True, "objectsVerified": True}


# --------------------------------------------------------------------------- requestId 보관

class RequestIdStore(Protocol):
    """요청 내용(fingerprint)별로 {"requestId", "expected", "done"}을 보관한다.

    보내기 전에 저장하고 성공하면 done=True로 바꾼다. 프로그램이 응답 전에 죽으면 다음 실행에서
    done=False인 요청을 **같은 requestId + 같은 expectedStateVersion**으로 먼저 다시 보낸다(서버가 replay).
    """
    def get(self, fingerprint: str) -> dict | None: ...
    def put(self, fingerprint: str, entry: dict) -> None: ...


class MemoryRequestIdStore:
    def __init__(self) -> None:
        self._data: dict[str, dict] = {}

    def get(self, fingerprint):
        return self._data.get(fingerprint)

    def put(self, fingerprint, entry):
        self._data[fingerprint] = dict(entry)


class JsonFileRequestIdStore:
    """스크립트 실행용 파일 보관소. 쓸 때마다 원자적으로 저장한다."""

    def __init__(self, path: str | Path):
        self._path = Path(path)

    def _load(self) -> dict:
        return json.loads(self._path.read_text()) if self._path.exists() else {}

    def get(self, fingerprint):
        return self._load().get(fingerprint)

    def put(self, fingerprint, entry):
        data = self._load()
        data[fingerprint] = dict(entry)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
        os.replace(tmp, self._path)


def batch_fingerprint(patient_key: str, records: list[dict], followups: list[dict], verification: dict) -> str:
    """같은 내용의 요청인지 판단하는 값(행 순서와 무관)."""
    body = {
        "patientKey": patient_key,
        "records": sorted(records, key=lambda r: r["resourceKey"]),
        "followups": sorted(followups, key=lambda f: f["followupKey"]),
        "sourceVerification": verification,
    }
    raw = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- 호출

@dataclass
class UpsertResult:
    request_id: str
    state_version: int
    data: dict
    attempts: int
    conflicts_resolved: int


class DbWriter:
    def __init__(
        self,
        transport: Transport,
        auth: AuthProvider,
        *,
        request_ids: RequestIdStore | None = None,
        api_version: str = "2",
        max_retries: int = 3,
        max_conflict_retries: int = 3,
        backoff_seconds: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._transport = transport
        self._auth = auth
        self._ids = request_ids or MemoryRequestIdStore()
        self._api_version = api_version
        self._max_retries = max_retries
        self._max_conflicts = max_conflict_retries
        self._backoff = backoff_seconds
        self._sleep = sleep

    def _event(self, operation: str, params: dict, request_id: str) -> dict:
        auth = self._auth()
        if not isinstance(auth, dict) or set(auth) - {"auth", "patientToken"}:
            raise ValueError("AuthProvider는 {'auth': {...}} (또는 구버전 {'patientToken': ...})만 돌려줘야 합니다")
        return {"apiVersion": self._api_version, "operation": operation, "requestId": request_id, **auth,
                "params": params}

    def _call(self, operation: str, params: dict, request_id: str) -> tuple[dict, int]:
        """같은 requestId로 재시도. (data, 시도 횟수)"""
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._transport(self._event(operation, params, request_id))
            except TransportError:
                if attempt > self._max_retries:
                    raise
                self._sleep(self._backoff * attempt)
                continue
            if not isinstance(response, dict):
                raise DbWriteError("INVALID_RESPONSE", "응답이 JSON 객체가 아닙니다", request_id=request_id)
            if response.get("ok") is True:
                return response.get("data") or {}, attempt
            error = response.get("error") or {}
            err = DbWriteError(str(error.get("code", "UNKNOWN")), str(error.get("message", "")),
                               bool(error.get("retryable")), request_id)
            if err.retryable and attempt <= self._max_retries:
                self._sleep(self._backoff * attempt)
                continue
            raise err

    def get_write_state(self) -> dict:
        data, _ = self._call("get_write_state", {}, str(uuid.uuid4()))
        return data

    def upsert(self, patient_key: str, records: list[dict], followups: list[dict], verification: dict,
               expected_state_version: int) -> UpsertResult:
        """한 요청(최대 50행)을 보낸다. 성공하면 서버의 새 stateVersion을 돌려준다."""
        if len(records) + len(followups) > 50 or not (records or followups):
            raise ValueError("한 요청은 1~50행이어야 합니다")
        fingerprint = batch_fingerprint(patient_key, records, followups, verification)
        pending = self._ids.get(fingerprint)
        if pending and not pending.get("done"):
            # 지난번에 응답을 못 받은 같은 요청 → 같은 requestId·같은 expected로 먼저 다시 보낸다
            request_id, expected = pending["requestId"], int(pending["expected"])
        else:
            request_id, expected = str(uuid.uuid4()), expected_state_version
        conflicts = 0
        total_attempts = 0
        while True:
            # 보내기 전에 저장해야 중간에 죽어도 같은 ID로 재시도된다
            self._ids.put(fingerprint, {"requestId": request_id, "expected": expected, "done": False})
            params = {"records": records, "followups": followups, "expectedStateVersion": expected,
                      "sourceVerification": verification}
            try:
                data, attempts = self._call("upsert_records", params, request_id)
            except DbWriteError as exc:
                if exc.code != "CONFLICT" or conflicts >= self._max_conflicts:
                    raise
                current = int(self.get_write_state().get("stateVersion", -1))
                if current == expected:
                    raise  # 상태는 그대로인데 충돌 → 같은 requestId를 다른 내용에 쓴 경우 등. 사람이 확인해야 함
                conflicts += 1
                request_id, expected = str(uuid.uuid4()), current  # 최신 상태 기준 새 요청
                continue
            total_attempts += attempts
            self._ids.put(fingerprint, {"requestId": request_id, "expected": expected, "done": True})
            return UpsertResult(request_id, int(data["stateVersion"]), data, total_attempts, conflicts)


def lambda_transport(function_arn: str | None = None, region: str = "us-east-1") -> Transport:
    """실제 AWS용(2단계에서 사용). boto3는 이 함수를 부를 때만 import 한다."""
    import boto3  # noqa: PLC0415
    from botocore.config import Config  # noqa: PLC0415
    from botocore.exceptions import BotoCoreError, ClientError  # noqa: PLC0415

    arn = function_arn or os.environ.get("DB_WRITE_FUNCTION_ARN")
    if not arn:
        raise ValueError("DB_WRITE_FUNCTION_ARN 환경 변수에 DB 쓰기 Lambda ARN을 지정하세요")
    client = boto3.client("lambda", region_name=region,
                          config=Config(connect_timeout=5, read_timeout=40, retries={"max_attempts": 1}))

    def call(event: dict) -> dict:
        try:
            result = client.invoke(FunctionName=arn, InvocationType="RequestResponse",
                                   Payload=json.dumps(event, ensure_ascii=False).encode("utf-8"))
        except (BotoCoreError, ClientError) as exc:
            raise TransportError(type(exc).__name__) from None
        payload: Any = json.loads(result["Payload"].read())
        if result.get("FunctionError"):
            raise DbWriteError("DB_FUNCTION_ERROR", "DB 함수 실행 오류", request_id=event.get("requestId"))
        return payload

    return call
