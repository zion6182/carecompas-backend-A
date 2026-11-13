"""S3 원문 저장 + 검증 (적재 1단계: AWS 없이 테스트 가능하게 클라이언트를 주입받는다).

규칙
- 허용된 원본 prefix(carecompass/v1/data/synthetic/fhir/, carecompass/v1/data/fhir/) 밖에는 쓰지 않는다.
- 같은 key에 **같은 바이트**가 이미 있으면 다시 올리지 않는다(재적재해도 안전).
- 같은 key에 **다른 바이트**가 있으면 기본적으로 덮어쓰지 않고 멈춘다. 이미 적재된 데이터(예: DB 팀 시연 데이터)를
  실수로 바꾸지 않기 위해서다. 정말 바꿔야 하면 allow_overwrite=True를 명시해야 한다.
- 올린 뒤에는 다시 읽어서 SHA-256이 기대값과 같은지 확인한다. 같아야만 "objectsVerified"로 본다.

S3 클라이언트는 boto3 client('s3')와 같은 메서드 이름(get_object / put_object)만 쓴다. 테스트에서는 가짜를 넣는다.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Protocol

ALLOWED_PREFIXES = ("carecompass/v1/data/synthetic/fhir/", "carecompass/v1/data/fhir/")


class S3Client(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> dict: ...
    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **kwargs: Any) -> dict: ...


class S3StoreError(Exception):
    def __init__(self, code: str, key: str, message: str):
        super().__init__(f"{code} {key}: {message}")
        self.code = code
        self.key = key


@dataclass
class UploadReport:
    uploaded: list[str] = field(default_factory=list)   # 새로 올림 (+ 다시 읽어 해시 확인)
    unchanged: list[str] = field(default_factory=list)  # 같은 바이트가 이미 있어 건너뜀
    overwritten: list[str] = field(default_factory=list)  # allow_overwrite=True로 바꾼 것

    @property
    def verified(self) -> list[str]:
        return self.uploaded + self.unchanged + self.overwritten


def _is_missing(exc: Exception) -> bool:
    """boto3의 NoSuchKey / 404 와 테스트용 KeyError 를 '없음'으로 본다."""
    if isinstance(exc, KeyError):
        return True
    response = getattr(exc, "response", None) or {}
    code = str((response.get("Error") or {}).get("Code", ""))
    return code in ("NoSuchKey", "404", "NotFound")


def _validate_key(key: str) -> None:
    parts = key.split("/")
    if (not key.startswith(ALLOWED_PREFIXES) or any(p in ("", ".", "..") for p in parts)
            or any(ch in key for ch in "\\?#") or any("incoming" in p.lower() for p in parts) or len(key) > 1024):
        raise S3StoreError("KEY_NOT_ALLOWED", key, "허용된 FHIR 원본 경로가 아닙니다")


class S3ObjectStore:
    def __init__(self, client: S3Client, bucket: str):
        if not bucket:
            raise ValueError("bucket is required")
        self._client = client
        self._bucket = bucket

    def _read(self, key: str) -> bytes | None:
        try:
            body = self._client.get_object(Bucket=self._bucket, Key=key)["Body"]
        except Exception as exc:  # noqa: BLE001
            if _is_missing(exc):
                return None
            raise
        return body.read() if hasattr(body, "read") else bytes(body)

    def check(self, objects: dict[str, bytes]) -> dict[str, str]:
        """올리지 않고 상태만 본다: {key: 'missing' | 'same' | 'different'}"""
        states = {}
        for key, data in sorted(objects.items()):
            _validate_key(key)
            existing = self._read(key)
            states[key] = "missing" if existing is None else ("same" if existing == data else "different")
        return states

    def put_verified(self, objects: dict[str, bytes], *, allow_overwrite: bool = False) -> UploadReport:
        """objects = {s3Key: 바이트}. 하나라도 문제가 있으면 예외로 멈춘다(DB 적재로 넘어가지 않게)."""
        for key in objects:
            _validate_key(key)
        states = self.check(objects)
        conflicts = [k for k, state in states.items() if state == "different"]
        if conflicts and not allow_overwrite:
            raise S3StoreError("OBJECT_CONTENT_DIFFERS", conflicts[0],
                               f"이미 다른 내용의 파일이 있어 덮어쓰지 않습니다 (총 {len(conflicts)}건)")
        report = UploadReport()
        for key, data in sorted(objects.items()):
            if states[key] == "same":
                report.unchanged.append(key)
                continue
            digest = hashlib.sha256(data).hexdigest()
            self._client.put_object(Bucket=self._bucket, Key=key, Body=data,
                                    ContentType="application/fhir+json", Metadata={"sha256": digest})
            readback = self._read(key)
            if readback is None or hashlib.sha256(readback).hexdigest() != digest:
                raise S3StoreError("READBACK_MISMATCH", key, "올린 뒤 다시 읽은 내용의 해시가 다릅니다")
            (report.overwritten if states[key] == "different" else report.uploaded).append(key)
        return report
